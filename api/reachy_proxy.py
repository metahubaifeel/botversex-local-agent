"""Reachy Mini daemon proxy.

BotverseX owns the platform-facing API. For the USB-only Reachy Mini variant,
the Pollen/Allan daemon runs locally on this machine and talks to the robot over
serial (for example /dev/ttyACM0).
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from .robot_config import get_robot_manifest, get_service_config, get_transport_config


router = APIRouter(prefix="/api/reachy", tags=["reachy-mini"])
ws_router = APIRouter(tags=["reachy-mini"])

REACHY_MINI_JOINT_NAMES = [
    "body_rotation",
    "stewart_1",
    "stewart_2",
    "stewart_3",
    "stewart_4",
    "stewart_5",
    "stewart_6",
    "right_antenna",
    "left_antenna",
]

REALTIME_ROOT = Path(__file__).parent.parent.resolve()
LOCAL_MOVES_DIR = REALTIME_ROOT / "data" / "reachy_mini_moves"
CUSTOM_MOVE_CATEGORY = "custom"
REACHY_RUNTIME_ID = "reachy_mini"
DEFAULT_RECORD_HZ = 10.0
IDENTITY_HEAD_MATRIX = [
    [1.0, 0.0, 0.0, 0.0],
    [0.0, 1.0, 0.0, 0.0],
    [0.0, 0.0, 1.0, 0.0],
    [0.0, 0.0, 0.0, 1.0],
]

_local_daemon_process: subprocess.Popen[str] | None = None

# In-memory Reachy record / virtual / local playback state (single-process realtime).
_record_session: dict[str, Any] = {
    "active": False,
    "name": "",
    "description": "",
    "sample_hz": DEFAULT_RECORD_HZ,
    "started_at": 0.0,
    "frames": [],
    "task": None,
}
_virtual: dict[str, Any] = {
    "active": False,
    "state": {},
    "motor_mode": "disabled",
}
_playback_tasks: dict[str, asyncio.Task[None]] = {}
_playback_cancel: dict[str, asyncio.Event] = {}

# Reachy Mini body_rotation motor (Dynamixel ID 10) — used for USB port autodetect.
_REACHY_PROBE_MOTOR_ID = 10
_SERIAL_DETECT_CACHE_TTL_S = 30.0
_serial_detect_cache: dict[str, Any] = {
    "port": None,
    "expires_at": 0.0,
    "source": None,
}
# Serial port used by the last successfully started managed daemon (port may be busy while running).
_active_daemon_serial: str | None = None


def _reachy_host() -> str:
    service = get_service_config(REACHY_RUNTIME_ID)
    return os.environ.get("REACHY_MINI_HOST", str(service.get("host") or "127.0.0.1"))


def _reachy_port() -> int:
    service = get_service_config(REACHY_RUNTIME_ID)
    raw = os.environ.get("REACHY_MINI_PORT", str(service.get("port") or 8010))
    try:
        return int(raw)
    except ValueError:
        return int(service.get("port") or 8010)


def _configured_serial_port() -> str:
    transport = get_transport_config(REACHY_RUNTIME_ID)
    ports = transport.get("ports") or {}
    return str(ports.get("serial") or "auto")


def _invalidate_serial_detect_cache() -> None:
    _serial_detect_cache["port"] = None
    _serial_detect_cache["expires_at"] = 0.0
    _serial_detect_cache["source"] = None


def _daemon_tcp_open(timeout_s: float = 0.4) -> bool:
    try:
        with socket.create_connection((_reachy_host(), _reachy_port()), timeout=timeout_s):
            return True
    except OSError:
        return False


def _daemon_log_tail_text(max_lines: int = 80) -> str:
    lines = _daemon_log_tail(max_lines=max_lines)
    return "\n".join(lines)


def _daemon_log_shows_motor_failure() -> bool:
    """True when the latest daemon boot failed to find motors on the configured port."""
    text = _daemon_log_tail_text()
    if not text:
        return False
    return (
        "No motor found on port" in text
        or "Application startup failed" in text
        or "Motor communication error" in text
    )


def _serial_port_from_daemon_log(*, trust_only_if_healthy: bool = True) -> str | None:
    """Read the last --serialport from the daemon log (works when the port is busy)."""
    if trust_only_if_healthy and _daemon_log_shows_motor_failure():
        return None
    log_path = _reachy_daemon_log()
    if not log_path.is_file():
        return None
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    matches = re.findall(r"--serialport\s+(/dev/\S+)", text)
    return matches[-1] if matches else None


def _ping_reachy_motor_on_port(port: str) -> bool:
    """Return True if Reachy Mini motor ID 10 responds on this serial port (via SDK rustypot)."""
    sdk_path = _reachy_sdk_path()
    if not sdk_path.exists():
        return False

    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(sdk_path / 'src')!r})\n"
        "from reachy_mini.tools.setup_motor import lookup_for_motor\n"
        f"ok = lookup_for_motor({port!r}, {_REACHY_PROBE_MOTOR_ID}, 1_000_000, silent=True)\n"
        "raise SystemExit(0 if ok else 1)\n"
    )
    try:
        proc = subprocess.run(
            [_reachy_python(), "-c", code],
            cwd=str(sdk_path),
            capture_output=True,
            timeout=2.5,
            env=_reachy_daemon_env(),
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _auto_detect_reachy_port(*, force: bool = False) -> str | None:
    """Scan /dev/ttyACM* for a Reachy Mini motor response."""
    now = time.monotonic()
    cached = _serial_detect_cache.get("port")
    if (
        not force
        and isinstance(cached, str)
        and now < float(_serial_detect_cache.get("expires_at") or 0.0)
        and Path(cached).exists()
    ):
        return cached

    try:
        import serial  # noqa: F401
    except ImportError:
        _serial_detect_cache["port"] = None
        _serial_detect_cache["expires_at"] = now + _SERIAL_DETECT_CACHE_TTL_S
        _serial_detect_cache["source"] = "auto"
        return None

    for port in sorted(glob.glob("/dev/ttyACM*")):
        if _ping_reachy_motor_on_port(port):
            _serial_detect_cache["port"] = port
            _serial_detect_cache["expires_at"] = now + _SERIAL_DETECT_CACHE_TTL_S
            _serial_detect_cache["source"] = "auto"
            return port

    _serial_detect_cache["port"] = None
    _serial_detect_cache["expires_at"] = now + _SERIAL_DETECT_CACHE_TTL_S
    _serial_detect_cache["source"] = "auto"
    return None


def _resolve_serial_port_meta(*, force_detect: bool = False) -> dict[str, Any]:
    """Resolve serial port with source: env | auto | config | fallback."""
    configured = _configured_serial_port()
    env = os.environ.get("REACHY_MINI_SERIAL_PORT", "").strip()
    if env:
        return {"port": env, "source": "env", "configured": configured}

    if _local_daemon_running() and _active_daemon_serial:
        return {
            "port": _active_daemon_serial,
            "source": "daemon",
            "configured": configured,
        }

    if _daemon_tcp_open():
        from_log = _serial_port_from_daemon_log(trust_only_if_healthy=True)
        if from_log and _ping_reachy_motor_on_port(from_log):
            return {
                "port": from_log,
                "source": "daemon",
                "configured": configured,
            }

    use_auto = configured.lower() == "auto"
    configured_missing = (
        not use_auto
        and configured
        and not Path(configured).exists()
    )
    if use_auto or configured_missing:
        detected = _auto_detect_reachy_port(force=force_detect)
        if detected:
            return {"port": detected, "source": "auto", "configured": configured}

    if use_auto:
        acm_ports = sorted(glob.glob("/dev/ttyACM*"))
        fallback = acm_ports[0] if acm_ports else "/dev/ttyACM0"
        return {"port": fallback, "source": "fallback", "configured": configured}

    return {"port": configured, "source": "config", "configured": configured}


def _reachy_serial_port(*, force_detect: bool = False) -> str:
    return _resolve_serial_port_meta(force_detect=force_detect)["port"]


def _resolve_serial_port_for_start() -> dict[str, Any]:
    """Pick a serial port with a live Reachy motor before starting the daemon."""
    configured = _configured_serial_port()
    for candidate in sorted(glob.glob("/dev/ttyACM*")):
        if _ping_reachy_motor_on_port(candidate):
            return {"port": candidate, "source": "auto", "configured": configured}

    meta = _resolve_serial_port_meta(force_detect=True)
    port = str(meta["port"])
    if Path(port).exists():
        return meta

    acm_ports = sorted(glob.glob("/dev/ttyACM*"))
    if acm_ports:
        return {"port": acm_ports[0], "source": "fallback", "configured": configured}
    return meta


async def _stop_broken_daemon_listener() -> None:
    """Stop a reachy daemon that is listening on HTTP but failed motor init (503)."""
    global _local_daemon_process, _active_daemon_serial

    proc = _local_daemon_process
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            await asyncio.get_event_loop().run_in_executor(None, proc.wait, 5)
        except Exception:
            proc.kill()
            await asyncio.get_event_loop().run_in_executor(None, proc.wait)
        _local_daemon_process = None

    try:
        subprocess.run(
            ["pkill", "-f", "reachy_mini.daemon.app.main"],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass

    _active_daemon_serial = None
    _invalidate_serial_detect_cache()
    await asyncio.sleep(0.5)


def _reachy_sdk_path() -> Path:
    service = get_service_config(REACHY_RUNTIME_ID)
    return Path(os.environ.get("REACHY_MINI_SDK_PATH", str(service.get("sdk_path") or ""))).expanduser()


def _reachy_daemon_log() -> Path:
    service = get_service_config(REACHY_RUNTIME_ID)
    return Path(os.environ.get("REACHY_MINI_LOG_FILE", str(service.get("log_file") or "/tmp/botversex_reachy_mini_daemon.log"))).expanduser()


def _reachy_python() -> str:
    configured = os.environ.get("REACHY_MINI_DAEMON_PYTHON")
    if configured:
        return configured
    sdk_path = _reachy_sdk_path()
    venv_python = sdk_path / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _reachy_daemon_env() -> dict[str, str]:
    env = dict(os.environ)
    sdk_src = _reachy_sdk_path() / "src"
    existing = env.get("PYTHONPATH")
    parts = [str(sdk_src)]
    if existing:
        parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def _http_base_url() -> str:
    return f"http://{_reachy_host()}:{_reachy_port()}"


def _ws_base_url() -> str:
    return f"ws://{_reachy_host()}:{_reachy_port()}"


def _daemon_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_http_base_url()}{path}"


def _local_categories() -> list[str]:
    if not LOCAL_MOVES_DIR.exists():
        return []
    return sorted(
        p.name for p in LOCAL_MOVES_DIR.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )


def _local_moves(category: str) -> list[str]:
    category_dir = LOCAL_MOVES_DIR / category
    if not category_dir.is_dir():
        return []
    return sorted(p.stem for p in category_dir.glob("*.json"))


def _local_daemon_running() -> bool:
    return _local_daemon_process is not None and _local_daemon_process.poll() is None


def _daemon_log_tail(max_lines: int = 10) -> list[str]:
    log_path = _reachy_daemon_log()
    if not log_path.is_file():
        return []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-max_lines:]
    except OSError:
        return []


def _reachy_ping_hints(
    *,
    serial: dict[str, Any],
    http_probe: dict[str, Any],
    log_tail: list[str],
    serial_meta: dict[str, Any],
) -> list[str]:
    port = str(serial.get("port") or serial_meta.get("port") or "")
    source = str(serial_meta.get("source") or "")
    configured = str(serial_meta.get("configured") or "")
    hints: list[str] = []

    if source == "auto" and port:
        hints.append(f"已自动检测到 Reachy 串口：{port}（换 USB 口后点「启动守护进程」会重新探测）。")
    elif configured.lower() == "auto" and not port:
        acm_ports = sorted(glob.glob("/dev/ttyACM*"))
        if acm_ports:
            hints.append(
                f"未在任何 /dev/ttyACM* 上检测到 Reachy 电机（已扫描：{', '.join(acm_ports)}）。"
                "请确认机器人已通电。"
            )
        else:
            hints.append("未找到 /dev/ttyACM* 设备。请插入 Reachy USB 并确认已通电。")

    log_text = "\n".join(log_tail)
    if "No motor found on port" in log_text:
        hints.append(
            f"守护进程在 {port} 未检测到 Reachy 电机：请确认机器人已通电（不只是 USB 插入），"
            "并确认数据线支持数据传输。"
        )
        other_ports = sorted(p for p in glob.glob("/dev/ttyACM*") if p != port)
        if other_ports:
            hints.append(
                f"系统上还有其它串口 {', '.join(other_ports)}。"
                "串口设为 auto 时会自动探测；也可设置 REACHY_MINI_SERIAL_PORT 指定端口。"
            )
    elif not http_probe.get("ok"):
        if serial.get("exists"):
            hints.append(
                f"串口 {port} 已找到，但守护进程 HTTP ({http_probe.get('base')}) 不可达。"
                "请点击「启动守护进程」或查看日志。"
            )
        else:
            hints.append(
                f"未找到串口 {port}。请插入 Reachy USB；配置为 auto 时会自动扫描 /dev/ttyACM*。"
            )

    if source == "env":
        hints.append(f"当前使用环境变量 REACHY_MINI_SERIAL_PORT={port}。")

    hints.extend([
        "Plug Reachy Mini into USB and confirm the robot is powered on.",
        "Click Start Local Daemon if daemon_http.ok=false.",
        "If permission denied, add the user running apps/realtime to dialout or fix udev permissions.",
        "Override with REACHY_MINI_SERIAL_PORT to pin a specific /dev/ttyACM* path.",
    ])
    return hints


def _local_daemon_status_payload(http_probe: dict[str, Any] | None = None) -> dict[str, Any]:
    proc_running = _local_daemon_running()
    return {
        "transport": "usb_serial",
        "service": "local_daemon",
        "serial_port": _reachy_serial_port(),
        "sdk_path": str(_reachy_sdk_path()),
        "python": _reachy_python(),
        "pythonpath": _reachy_daemon_env().get("PYTHONPATH"),
        "log_file": str(_reachy_daemon_log()),
        "manifest": get_robot_manifest(REACHY_RUNTIME_ID),
        "daemon_http": http_probe or {
            "base": _http_base_url(),
            "ok": None,
            "error": None,
        },
        "managed_process": {
            "running": proc_running,
            "pid": _local_daemon_process.pid if proc_running and _local_daemon_process else None,
            "returncode": _local_daemon_process.poll() if _local_daemon_process else None,
        },
    }


async def _probe_daemon_http(timeout_s: float = 2.0) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=timeout_s, trust_env=False) as client:
            r = await client.get(
                _daemon_url("/api/state/full"),
                params={"with_body_yaw": "true"},
            )
        return {
            "base": _http_base_url(),
            "ok": r.status_code == 200,
            "status_code": r.status_code,
            "error": None if r.status_code == 200 else f"HTTP {r.status_code}",
        }
    except Exception as exc:
        return {
            "base": _http_base_url(),
            "ok": False,
            "status_code": None,
            "error": f"{type(exc).__name__}: {exc!s}",
        }


def _serial_status(check_open: bool = False, port: str | None = None) -> dict[str, Any]:
    port = port or _reachy_serial_port()
    path = Path(port)
    status: dict[str, Any] = {
        "port": port,
        "exists": path.exists(),
        "readable": os.access(port, os.R_OK) if path.exists() else False,
        "writable": os.access(port, os.W_OK) if path.exists() else False,
        "openable": None,
        "error": None,
    }
    if not path.exists() or not check_open:
        return status

    try:
        import serial  # type: ignore
    except ImportError as exc:
        status["error"] = f"pyserial_not_installed: {exc!s}"
        return status

    try:
        ser = serial.Serial(port, baudrate=1_000_000, timeout=0.1, write_timeout=0.1)
        ser.close()
        status["openable"] = True
    except Exception as exc:
        status["openable"] = False
        status["error"] = f"{type(exc).__name__}: {exc!s}"
    return status


async def _forward_json(
    request: Request,
    method: str,
    path: str,
    *,
    json_body: Any | None = None,
) -> Response:
    """Forward a request to the Reachy daemon and preserve useful errors."""
    params = dict(request.query_params)
    url = _daemon_url(path)

    try:
        # Ignore ALL_PROXY / HTTP_PROXY / HTTPS_PROXY. Reachy daemon is a LAN
        # endpoint and proxying it breaks (SOCKS requires extra deps).
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            response = await client.request(method, url, params=params, json=json_body)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "reachy_daemon_unreachable",
                "message": f"Cannot reach Reachy Mini daemon at {_http_base_url()}",
                "hint": "Use /api/reachy/local-daemon/start for USB Reachy Mini, or set REACHY_MINI_HOST/REACHY_MINI_PORT for an external daemon.",
                "error": str(exc),
            },
        ) from exc

    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return JSONResponse(
                status_code=response.status_code,
                content=response.json(),
            )
        except ValueError:
            pass

    return Response(
        status_code=response.status_code,
        content=response.content,
        media_type=content_type or None,
    )


def _to_float_list(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    out: list[float] = []
    for item in value:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            continue
    return out


def _extract_joint_values(state: dict[str, Any]) -> tuple[dict[str, float], dict[str, float]]:
    """Convert Reachy state payloads to BotverseX numeric + named joint maps."""
    head = _to_float_list(state.get("head_joints"))
    target_head = _to_float_list(state.get("target_head_joints"))
    antennas = _to_float_list(
        state.get("antennas_position") or state.get("antenna_positions")
    )

    if not head and target_head:
        head = target_head

    values = [0.0] * len(REACHY_MINI_JOINT_NAMES)
    if head:
        for idx, value in enumerate(head[:7]):
            values[idx] = value
    else:
        try:
            values[0] = float(state.get("body_yaw", 0.0) or 0.0)
        except (TypeError, ValueError):
            values[0] = 0.0

    if antennas:
        if len(antennas) >= 1:
            # The Pollen daemon returns (left, right); BotverseX runtime names are
            # right_antenna, left_antenna, so keep both explicit below.
            values[8] = antennas[0]
        if len(antennas) >= 2:
            values[7] = antennas[1]

    numeric = {str(i + 1): value for i, value in enumerate(values)}
    named = {name: values[i] for i, name in enumerate(REACHY_MINI_JOINT_NAMES)}
    return numeric, named


def _state_envelope(state: dict[str, Any]) -> dict[str, Any]:
    numeric, named = _extract_joint_values(state)
    return {
        "type": "reachy_state",
        "robot_id": "reachy_mini",
        "timestamp": time.time(),
        "state": state,
        "joints": numeric,
        "named_joints": named,
        "virtual": _virtual_active(),
    }


def _virtual_active() -> bool:
    return bool(_virtual.get("active"))


def _default_virtual_state() -> dict[str, Any]:
    return {
        "control_mode": "disabled",
        "head_pose": [row[:] for row in IDENTITY_HEAD_MATRIX],
        "head_joints": [0.0] * 7,
        "target_head_joints": [0.0] * 7,
        "body_yaw": 0.0,
        "target_body_yaw": 0.0,
        "antennas_position": [0.0, 0.0],
        "target_antennas_position": [0.0, 0.0],
    }


def _virtual_state_dict() -> dict[str, Any]:
    state = _virtual.get("state")
    if not isinstance(state, dict) or not state:
        state = _default_virtual_state()
        _virtual["state"] = state
    state["control_mode"] = _virtual.get("motor_mode", "disabled")
    return state


def _is_head_matrix(value: Any) -> bool:
    if not isinstance(value, list) or len(value) != 4:
        return False
    return all(isinstance(row, list) and len(row) == 4 for row in value)


def _state_to_move_frame(state: dict[str, Any]) -> dict[str, Any]:
    head = state.get("head_pose") or state.get("target_head_pose")
    if not _is_head_matrix(head):
        head = [row[:] for row in IDENTITY_HEAD_MATRIX]
    antennas = _to_float_list(
        state.get("antennas_position") or state.get("antenna_positions")
    )
    if len(antennas) < 2:
        antennas = [0.0, 0.0]
    try:
        body_yaw = float(state.get("body_yaw", state.get("target_body_yaw", 0.0)) or 0.0)
    except (TypeError, ValueError):
        body_yaw = 0.0
    return {
        "head": head,
        "antennas": antennas[:2],
        "body_yaw": body_yaw,
    }


def _apply_frame_to_virtual(frame: dict[str, Any]) -> None:
    state = _virtual_state_dict()
    head = frame.get("head")
    state["head_pose"] = head if _is_head_matrix(head) else [row[:] for row in IDENTITY_HEAD_MATRIX]
    antennas = _to_float_list(frame.get("antennas"))
    if len(antennas) < 2:
        antennas = [0.0, 0.0]
    state["antennas_position"] = antennas[:2]
    state["target_antennas_position"] = antennas[:2]
    try:
        body_yaw = float(frame.get("body_yaw", 0.0) or 0.0)
    except (TypeError, ValueError):
        body_yaw = 0.0
    state["body_yaw"] = body_yaw
    state["target_body_yaw"] = body_yaw
    # _extract_joint_values reads body_rotation from head_joints[0] when
    # head_joints is present, so keep it in sync with body_yaw.
    hj = state.get("head_joints")
    if isinstance(hj, list) and len(hj) >= 1:
        hj[0] = body_yaw
    thj = state.get("target_head_joints")
    if isinstance(thj, list) and len(thj) >= 1:
        thj[0] = body_yaw


def _slugify_move_name(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip().lower()).strip("_")
    return slug or f"move_{int(time.time())}"


def _find_local_move_path(move_name: str) -> Path | None:
    if not LOCAL_MOVES_DIR.exists():
        return None
    stem = Path(move_name).stem
    for category_dir in LOCAL_MOVES_DIR.iterdir():
        if not category_dir.is_dir():
            continue
        candidate = category_dir / f"{stem}.json"
        if candidate.is_file():
            return candidate
    return None


def _load_local_move(move_name: str) -> dict[str, Any] | None:
    path = _find_local_move_path(move_name)
    if path is None:
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    times = data.get("time")
    frames = data.get("set_target_data")
    if not isinstance(times, list) or not isinstance(frames, list) or not times or not frames:
        return None
    if len(times) != len(frames):
        return None
    return data


async def _daemon_request(
    method: str,
    path: str,
    *,
    params: dict[str, str] | None = None,
    json_body: Any | None = None,
    timeout_s: float = 10.0,
) -> httpx.Response:
    url = _daemon_url(path)
    async with httpx.AsyncClient(timeout=timeout_s, trust_env=False) as client:
        return await client.request(method, url, params=params, json=json_body)


async def _fetch_daemon_state() -> dict[str, Any]:
    response = await _daemon_request(
        "GET",
        "/api/state/full",
        params={
            "with_head_joints": "true",
            "with_body_yaw": "true",
            "with_antenna_positions": "true",
            "with_control_mode": "true",
        },
    )
    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "reachy_daemon_state_failed",
                "message": f"Daemon state/full returned HTTP {response.status_code}",
            },
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Invalid daemon state payload")
    return payload


async def _daemon_goto_frame(frame: dict[str, Any], duration: float) -> None:
    payload: dict[str, Any] = {
        "head_pose": frame.get("head"),
        "antennas": frame.get("antennas"),
        "body_yaw": frame.get("body_yaw", 0.0),
        "duration": max(float(duration), 0.05),
        "interpolation": "minjerk",
    }
    response = await _daemon_request("POST", "/api/move/goto", json_body=payload)
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "reachy_daemon_goto_failed",
                "message": f"Daemon move/goto returned HTTP {response.status_code}",
            },
        )


async def _cancel_all_playbacks() -> None:
    for play_id, event in list(_playback_cancel.items()):
        event.set()
    for task in list(_playback_tasks.values()):
        if not task.done():
            task.cancel()
    _playback_cancel.clear()
    _playback_tasks.clear()


async def _play_local_move_task(play_id: str, move_name: str, move_data: dict[str, Any]) -> None:
    cancel_event = _playback_cancel[play_id]
    times = [float(t) for t in move_data.get("time", [])]
    frames = move_data.get("set_target_data", [])
    if not times or not frames:
        return

    for index, frame in enumerate(frames):
        if cancel_event.is_set():
            break
        if not isinstance(frame, dict):
            continue
        if _virtual_active():
            _apply_frame_to_virtual(frame)
        else:
            if index + 1 < len(times):
                duration = max(times[index + 1] - times[index], 0.05)
            else:
                duration = 0.1
            try:
                await _daemon_goto_frame(frame, duration)
            except HTTPException:
                break
        if index + 1 < len(times):
            sleep_s = max(times[index + 1] - times[index], 0.02)
        else:
            sleep_s = 0.05
        try:
            await asyncio.wait_for(cancel_event.wait(), timeout=sleep_s)
            break
        except asyncio.TimeoutError:
            continue

    _playback_cancel.pop(play_id, None)
    _playback_tasks.pop(play_id, None)


async def _start_local_playback(move_name: str, move_data: dict[str, Any]) -> dict[str, Any]:
    await _cancel_all_playbacks()
    play_id = str(uuid.uuid4())
    cancel_event = asyncio.Event()
    _playback_cancel[play_id] = cancel_event
    task = asyncio.create_task(_play_local_move_task(play_id, move_name, move_data))
    _playback_tasks[play_id] = task
    return {"uuid": play_id, "status": "playing", "move_name": move_name, "source": "local"}


async def _merged_local_categories(request: Request) -> list[str]:
    local = set(_local_categories())
    try:
        response = await _forward_json(request, "GET", "/api/move/local-categories")
        if isinstance(response, JSONResponse):
            body = response.body
            daemon_cats = json.loads(body.decode()) if body else []
            if isinstance(daemon_cats, list):
                local.update(str(c) for c in daemon_cats)
    except HTTPException as exc:
        if exc.status_code != 502:
            raise
    return sorted(local)


async def _merged_local_moves(category: str, request: Request) -> list[str]:
    local = set(_local_moves(category))
    try:
        response = await _forward_json(
            request,
            "GET",
            f"/api/move/local-moves/list/{category}",
        )
        if isinstance(response, JSONResponse):
            body = response.body
            daemon_moves = json.loads(body.decode()) if body else []
            if isinstance(daemon_moves, list):
                local.update(str(m) for m in daemon_moves)
    except HTTPException as exc:
        if exc.status_code != 502:
            raise
    return sorted(local)


async def _record_poll_loop() -> None:
    sample_hz = float(_record_session.get("sample_hz") or DEFAULT_RECORD_HZ)
    interval = 1.0 / max(sample_hz, 1.0)
    started_at = float(_record_session.get("started_at") or time.time())
    while _record_session.get("active"):
        try:
            state = await _fetch_daemon_state()
            frame = _state_to_move_frame(state)
            elapsed = time.time() - started_at
            frames = _record_session.setdefault("frames", [])
            if frames:
                last_t = float(frames[-1].get("t", 0.0))
                if elapsed <= last_t:
                    elapsed = last_t + interval
            frames.append({"t": elapsed, "data": frame})
        except HTTPException:
            pass
        except Exception:
            pass
        await asyncio.sleep(interval)


def _frames_to_move_json(
    frames: list[dict[str, Any]],
    *,
    description: str,
) -> dict[str, Any]:
    if not frames:
        raise HTTPException(status_code=400, detail="No frames recorded")
    times = [float(f.get("t", 0.0)) for f in frames]
    t0 = times[0]
    normalized_times = [max(t - t0, 0.0) for t in times]
    set_target_data = [f["data"] for f in frames if isinstance(f.get("data"), dict)]
    if not set_target_data:
        raise HTTPException(status_code=400, detail="No valid frames recorded")
    if len(normalized_times) != len(set_target_data):
        normalized_times = normalized_times[: len(set_target_data)]
    return {
        "description": description,
        "time": normalized_times,
        "set_target_data": set_target_data,
    }


class ReachyRecordStartBody(BaseModel):
    name: str | None = None
    description: str | None = None
    sample_hz: float = Field(default=DEFAULT_RECORD_HZ, ge=1.0, le=30.0)


@router.get("/state/full")
async def get_full_state(request: Request) -> Response:
    if _virtual_active():
        return JSONResponse(content=_virtual_state_dict())
    return await _forward_json(request, "GET", "/api/state/full")


@router.get("/motors/status")
async def get_motor_status(request: Request) -> Response:
    if _virtual_active():
        return JSONResponse(content={"mode": _virtual.get("motor_mode", "disabled")})
    return await _forward_json(request, "GET", "/api/motors/status")


@router.post("/motors/set_mode/{mode}")
async def set_motor_mode(mode: str, request: Request) -> Response:
    if _virtual_active():
        _virtual["motor_mode"] = mode
        state = _virtual_state_dict()
        state["control_mode"] = mode
        return JSONResponse(content={"status": "ok", "mode": mode, "virtual": True})
    return await _forward_json(request, "POST", f"/api/motors/set_mode/{mode}")


@router.post("/move/goto")
async def move_goto(request: Request) -> Response:
    body = await request.json()
    if _virtual_active():
        frame = {
            "head": body.get("head_pose"),
            "antennas": body.get("antennas"),
            "body_yaw": body.get("body_yaw", 0.0),
        }
        if not _is_head_matrix(frame["head"]):
            frame["head"] = [row[:] for row in IDENTITY_HEAD_MATRIX]
        _apply_frame_to_virtual(frame)
        return JSONResponse(content={"status": "ok", "virtual": True})
    return await _forward_json(
        request,
        "POST",
        "/api/move/goto",
        json_body=body,
    )


@router.post("/move/stop")
async def stop_move(request: Request) -> Response:
    had_playback = bool(_playback_tasks)
    await _cancel_all_playbacks()
    if _virtual_active() or had_playback:
        return JSONResponse(
            content={"status": "stopped", "virtual": _virtual_active(), "local": had_playback},
        )
    body = await request.json()
    return await _forward_json(request, "POST", "/api/move/stop", json_body=body)


@router.get("/move/local-categories")
async def get_local_categories(request: Request) -> Response:
    if _virtual_active():
        return JSONResponse(content=_local_categories())
    merged = await _merged_local_categories(request)
    return JSONResponse(content=merged)


@router.get("/move/local-moves/list/{category}")
async def get_local_moves(category: str, request: Request) -> Response:
    if _virtual_active():
        return JSONResponse(content=_local_moves(category))
    merged = await _merged_local_moves(category, request)
    return JSONResponse(content=merged)


@router.post("/move/play/local-move/{move_name}")
async def play_local_move(move_name: str, request: Request) -> Response:
    move_data = _load_local_move(move_name)
    if move_data is not None:
        result = await _start_local_playback(move_name, move_data)
        return JSONResponse(content=result)
    if _virtual_active():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "reachy_move_not_found",
                "message": f"Local move not found: {move_name}",
            },
        )
    return await _forward_json(request, "POST", f"/api/move/play/local-move/{move_name}")


@router.post("/move/play/wake_up")
async def wake_up(request: Request) -> Response:
    if _virtual_active():
        move_data = _load_local_move("wake_up")
        if move_data:
            return JSONResponse(content=await _start_local_playback("wake_up", move_data))
        return JSONResponse(content={"status": "ok", "virtual": True})
    return await _forward_json(request, "POST", "/api/move/play/wake_up")


@router.post("/move/play/goto_sleep")
async def goto_sleep(request: Request) -> Response:
    if _virtual_active():
        move_data = _load_local_move("goto_sleep")
        if move_data:
            return JSONResponse(content=await _start_local_playback("goto_sleep", move_data))
        return JSONResponse(content={"status": "ok", "virtual": True})
    return await _forward_json(request, "POST", "/api/move/play/goto_sleep")


@router.get("/daemon/status")
async def get_daemon_status(request: Request) -> Response:
    return await _forward_json(request, "GET", "/api/daemon/status")


@router.get("/local-daemon/status")
async def get_local_daemon_status() -> Response:
    http_probe = await _probe_daemon_http()
    return JSONResponse(content=_local_daemon_status_payload(http_probe=http_probe))


@router.post("/local-daemon/start")
async def start_local_daemon() -> Response:
    global _local_daemon_process, _active_daemon_serial

    _invalidate_serial_detect_cache()

    http_probe = await _probe_daemon_http(timeout_s=1.0)
    if http_probe["ok"]:
        inferred = _serial_port_from_daemon_log(trust_only_if_healthy=True)
        if inferred:
            _active_daemon_serial = inferred
        elif not _active_daemon_serial:
            _active_daemon_serial = _serial_port_from_daemon_log(trust_only_if_healthy=False)
        return JSONResponse(
            content={
                "ok": True,
                "message": "Reachy Mini local daemon is already reachable.",
                "serial_detection": _resolve_serial_port_meta(force_detect=False),
                **_local_daemon_status_payload(http_probe=http_probe),
            }
        )

    if _daemon_tcp_open() and not http_probe.get("ok"):
        await _stop_broken_daemon_listener()
        http_probe = await _probe_daemon_http(timeout_s=1.0)

    if _local_daemon_running():
        return JSONResponse(
            content={
                "ok": True,
                "message": "Reachy Mini local daemon is already managed by BotverseX.",
                **_local_daemon_status_payload(http_probe=http_probe),
            }
        )

    sdk_path = _reachy_sdk_path()
    if not sdk_path.exists():
        raise HTTPException(
            status_code=500,
            detail={
                "code": "reachy_sdk_path_missing",
                "message": f"Reachy Mini SDK path does not exist: {sdk_path}",
                "hint": "Set REACHY_MINI_SDK_PATH to Allan's reachy_mini_Allan directory.",
            },
        )

    serial_meta = _resolve_serial_port_for_start()
    serial_port = str(serial_meta["port"])
    serial = _serial_status(check_open=False, port=serial_port)
    if not serial["exists"]:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "reachy_serial_missing",
                "message": f"Reachy Mini serial port not found: {serial_port}",
                "hint": "Plug Reachy Mini into USB (power on) or set REACHY_MINI_SERIAL_PORT.",
                "serial": serial,
                "serial_detection": serial_meta,
            },
        )
    if not _ping_reachy_motor_on_port(serial_port):
        scanned = {
            p: _ping_reachy_motor_on_port(p) for p in sorted(glob.glob("/dev/ttyACM*"))
        }
        raise HTTPException(
            status_code=400,
            detail={
                "code": "reachy_motor_not_found",
                "message": f"No Reachy motor on {serial_port}. Power on the robot (not only USB).",
                "hint": "Confirm the USB cable supports data. Try another port or set REACHY_MINI_SERIAL_PORT.",
                "serial": serial,
                "serial_detection": serial_meta,
                "scan": scanned,
            },
        )

    service = get_service_config(REACHY_RUNTIME_ID)
    template = service.get("command") or [
        "python",
        "-m",
        "reachy_mini.daemon.app.main",
        "--serialport",
        "{serial}",
        "--fastapi-host",
        "{host}",
        "--fastapi-port",
        "{port}",
        "--no-preload-datasets",
        "--autostart",
        "--deactivate-audio",
    ]
    cmd = [
        _reachy_python() if part == "python" else str(part).format(
            serial=serial_port,
            host=_reachy_host(),
            port=_reachy_port(),
            sdk_path=str(_reachy_sdk_path()),
        )
        for part in template
    ]

    try:
        log_path = _reachy_daemon_log()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_fh = log_path.open("a", encoding="utf-8")
        log_fh.write("\n\n=== BotverseX Reachy Mini local daemon start ===\n")
        log_fh.write(f"cwd={sdk_path}\n")
        log_fh.write(f"cmd={' '.join(cmd)}\n")
        log_fh.flush()
        _local_daemon_process = subprocess.Popen(
            cmd,
            cwd=str(sdk_path),
            env=_reachy_daemon_env(),
            stdin=subprocess.DEVNULL,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            text=True,
        )
        _active_daemon_serial = serial_port
        _serial_detect_cache["port"] = serial_port
        _serial_detect_cache["expires_at"] = time.monotonic() + _SERIAL_DETECT_CACHE_TTL_S
        _serial_detect_cache["source"] = "auto"
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "reachy_local_daemon_start_failed",
                "message": f"Failed to start Reachy Mini local daemon: {exc!s}",
                "command": cmd,
            },
        ) from exc

    # Wait for daemon bind (startup can take ~15–25s on first motor scan).
    http_probe = {"ok": False, "base": _http_base_url(), "status_code": None, "error": "starting"}
    for _ in range(30):
        await asyncio.sleep(1.0)
        if _local_daemon_process is not None and _local_daemon_process.poll() is not None:
            break
        http_probe = await _probe_daemon_http(timeout_s=1.0)
        if http_probe.get("ok"):
            break
    if not http_probe.get("ok"):
        _invalidate_serial_detect_cache()
        if _local_daemon_process is not None and _local_daemon_process.poll() is not None:
            _local_daemon_process = None
            _active_daemon_serial = None
    return JSONResponse(
        content={
            "ok": _local_daemon_running() and bool(http_probe.get("ok")),
            "message": "Reachy Mini local daemon start requested.",
            "command": cmd,
            "serial_detection": serial_meta,
            **_local_daemon_status_payload(http_probe=http_probe),
        }
    )


@router.post("/local-daemon/stop")
async def stop_local_daemon() -> Response:
    global _local_daemon_process, _active_daemon_serial

    proc = _local_daemon_process
    if proc is None:
        http_probe = await _probe_daemon_http(timeout_s=1.0)
        return JSONResponse(
            content={
                "ok": True,
                "message": "No BotverseX-managed Reachy Mini local daemon process is running.",
                **_local_daemon_status_payload(http_probe=http_probe),
            }
        )

    if proc.poll() is None:
        proc.terminate()
        try:
            await asyncio.get_event_loop().run_in_executor(None, proc.wait, 5)
        except Exception:
            proc.kill()
            await asyncio.get_event_loop().run_in_executor(None, proc.wait)

    _local_daemon_process = None
    _active_daemon_serial = None
    _invalidate_serial_detect_cache()
    http_probe = await _probe_daemon_http(timeout_s=1.0)
    return JSONResponse(
        content={
            "ok": True,
            "message": "BotverseX-managed Reachy Mini local daemon stopped.",
            **_local_daemon_status_payload(http_probe=http_probe),
        }
    )


@router.get("/record/status")
async def get_record_status() -> Response:
    frames = _record_session.get("frames") or []
    started_at = float(_record_session.get("started_at") or 0.0)
    elapsed_s = time.time() - started_at if _record_session.get("active") and started_at else 0.0
    return JSONResponse(
        content={
            "active": bool(_record_session.get("active")),
            "name": _record_session.get("name") or "",
            "sample_hz": float(_record_session.get("sample_hz") or DEFAULT_RECORD_HZ),
            "sample_count": len(frames),
            "elapsed_s": round(elapsed_s, 3),
            "virtual": _virtual_active(),
        },
    )


@router.post("/record/start")
async def start_record(body: ReachyRecordStartBody) -> Response:
    if _virtual_active():
        raise HTTPException(
            status_code=409,
            detail={
                "code": "reachy_record_virtual_active",
                "message": "Cannot record while virtual mode is active.",
            },
        )
    if _record_session.get("active"):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "reachy_record_already_active",
                "message": "A recording session is already running.",
            },
        )

    http_probe = await _probe_daemon_http()
    if not http_probe.get("ok"):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "reachy_daemon_unavailable",
                "message": "Reachy daemon must be online to record.",
                "daemon_http": http_probe,
            },
        )

    move_name = _slugify_move_name(body.name or f"custom_{int(time.time())}")
    _record_session.update({
        "active": True,
        "name": move_name,
        "description": body.description or f"Custom move {move_name}",
        "sample_hz": body.sample_hz,
        "started_at": time.time(),
        "frames": [],
    })
    task = asyncio.create_task(_record_poll_loop())
    _record_session["task"] = task

    return JSONResponse(
        content={
            "ok": True,
            "active": True,
            "name": move_name,
            "sample_hz": body.sample_hz,
            "category": CUSTOM_MOVE_CATEGORY,
        },
    )


@router.post("/record/stop")
async def stop_record() -> Response:
    if not _record_session.get("active"):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "reachy_record_not_active",
                "message": "No active recording session.",
            },
        )

    _record_session["active"] = False
    task = _record_session.get("task")
    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _record_session["task"] = None

    frames = list(_record_session.get("frames") or [])
    move_name = str(_record_session.get("name") or f"custom_{int(time.time())}")
    description = str(_record_session.get("description") or f"Custom move {move_name}")
    move_json = _frames_to_move_json(frames, description=description)

    category_dir = LOCAL_MOVES_DIR / CUSTOM_MOVE_CATEGORY
    category_dir.mkdir(parents=True, exist_ok=True)
    out_path = category_dir / f"{move_name}.json"
    tmp_path = out_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(move_json, indent=2), encoding="utf-8")
    tmp_path.replace(out_path)

    duration_s = float(move_json["time"][-1]) if move_json["time"] else 0.0
    sample_count = len(move_json["set_target_data"])

    _record_session["frames"] = []

    return JSONResponse(
        content={
            "ok": True,
            "move_name": move_name,
            "category": CUSTOM_MOVE_CATEGORY,
            "path": str(out_path.relative_to(REALTIME_ROOT)),
            "duration_s": duration_s,
            "sample_count": sample_count,
        },
    )


@router.get("/virtual/status")
async def get_virtual_status() -> Response:
    return JSONResponse(
        content={
            "active": _virtual_active(),
            "motor_mode": _virtual.get("motor_mode", "disabled"),
            "playback_count": len(_playback_tasks),
            "recording": bool(_record_session.get("active")),
        },
    )


@router.post("/virtual/start")
async def start_virtual_mode() -> Response:
    if _record_session.get("active"):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "reachy_record_active",
                "message": "Stop recording before entering virtual mode.",
            },
        )

    await _cancel_all_playbacks()
    _virtual["active"] = True
    _virtual["motor_mode"] = "disabled"
    _virtual["state"] = _default_virtual_state()
    return JSONResponse(
        content={
            "ok": True,
            "active": True,
            "message": "Reachy virtual mode started (no hardware).",
        },
    )


@router.post("/virtual/stop")
async def stop_virtual_mode() -> Response:
    await _cancel_all_playbacks()
    _virtual["active"] = False
    _virtual["state"] = {}
    return JSONResponse(
        content={
            "ok": True,
            "active": False,
            "message": "Reachy virtual mode stopped.",
        },
    )


@router.get("/ping")
async def reachy_ping() -> Response:
    """USB-aware debug endpoint for local Reachy Mini daemon readiness."""
    host = _reachy_host()
    port = _reachy_port()

    resolved: dict[str, Any] = {"host": host, "port": port, "ips": []}
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        ips: list[str] = []
        for info in infos:
            addr = info[4][0]
            if addr not in ips:
                ips.append(addr)
        resolved["ips"] = ips
    except Exception as exc:
        resolved["resolve_error"] = f"{type(exc).__name__}: {exc!s}"

    http_probe = await _probe_daemon_http()
    serial_meta = _resolve_serial_port_meta()
    serial = _serial_status(
        check_open=not bool(http_probe["ok"]),
        port=str(serial_meta["port"]),
    )
    log_tail = _daemon_log_tail()

    return JSONResponse(
        content={
            "ok": bool(http_probe["ok"]) and bool(serial["exists"]),
            "transport": "usb_serial",
            "service": "local_daemon",
            "serial": serial,
            "serial_detection": serial_meta,
            "daemon_http": http_probe,
            "local_daemon": _local_daemon_status_payload(http_probe=http_probe),
            "virtual": {
                "active": _virtual_active(),
                "motor_mode": _virtual.get("motor_mode", "disabled"),
                "playback_count": len(_playback_tasks),
            },
            "recording": {
                "active": bool(_record_session.get("active")),
                "name": _record_session.get("name") or "",
                "sample_count": len(_record_session.get("frames") or []),
            },
            "resolution": resolved,
            "log_tail": log_tail,
            "hints": _reachy_ping_hints(
                serial=serial,
                http_probe=http_probe,
                log_tail=log_tail,
                serial_meta=serial_meta,
            ),
        }
    )


@ws_router.websocket("/ws/reachy/state")
async def websocket_reachy_state(websocket: WebSocket) -> None:
    """Bridge daemon or virtual state into BotverseX's browser origin."""
    await websocket.accept()

    raw_freq = websocket.query_params.get("frequency", "10")
    try:
        frequency = max(float(raw_freq), 1.0)
    except ValueError:
        frequency = 10.0
    interval = 1.0 / frequency

    if _virtual_active():
        try:
            while True:
                await websocket.send_json(_state_envelope(_virtual_state_dict()))
                await asyncio.sleep(interval)
        except WebSocketDisconnect:
            return
        return

    http_probe = await _probe_daemon_http(timeout_s=1.0)
    if not http_probe.get("ok"):
        try:
            while True:
                await websocket.send_json({
                    "type": "reachy_error",
                    "code": "reachy_daemon_unavailable",
                    "message": "Reachy Mini daemon is not reachable.",
                    "hint": "Start local daemon or enable virtual mode via POST /api/reachy/virtual/start.",
                    "virtual_available": True,
                    "timestamp": time.time(),
                })
                await asyncio.sleep(interval)
        except WebSocketDisconnect:
            return
        return

    query = {
        "frequency": str(int(frequency)) if frequency.is_integer() else str(frequency),
        "with_head_joints": "true",
        "with_body_yaw": "true",
        "with_antenna_positions": "true",
        "with_control_mode": "true",
    }
    daemon_ws = f"{_ws_base_url()}/api/state/ws/full?{urlencode(query)}"

    try:
        async with websockets.connect(
            daemon_ws,
            ping_interval=20,
            ping_timeout=20,
            proxy=None,
        ) as upstream:
            while True:
                raw = await upstream.recv()
                try:
                    state = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    await websocket.send_json({
                        "type": "reachy_raw",
                        "timestamp": time.time(),
                        "raw": raw,
                    })
                    continue
                await websocket.send_json(_state_envelope(state))
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await websocket.send_json({
                "type": "reachy_error",
                "code": "reachy_state_stream_failed",
                "message": f"Cannot stream Reachy Mini state from {daemon_ws}",
                "hint": "Check that the Reachy Mini daemon is running, or use virtual mode.",
                "error": f"{type(exc).__name__}: {exc!s}",
                "exception_type": type(exc).__name__,
                "exception_repr": repr(exc),
                "timestamp": time.time(),
            })
        except Exception:
            pass
