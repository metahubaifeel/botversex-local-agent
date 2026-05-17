"""Reachy Mini daemon proxy.

BotverseX owns the platform-facing API. For the USB-only Reachy Mini variant,
the Pollen/Allan daemon runs locally on this machine and talks to the robot over
serial (for example /dev/ttyACM0).
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

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
REACHY_RUNTIME_ID = "reachy_mini"

_local_daemon_process: subprocess.Popen[str] | None = None


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


def _reachy_serial_port() -> str:
    transport = get_transport_config(REACHY_RUNTIME_ID)
    ports = transport.get("ports") or {}
    return os.environ.get("REACHY_MINI_SERIAL_PORT", str(ports.get("serial") or "/dev/ttyACM0"))


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


def _serial_status(check_open: bool = False) -> dict[str, Any]:
    port = _reachy_serial_port()
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
    }


@router.get("/state/full")
async def get_full_state(request: Request) -> Response:
    return await _forward_json(request, "GET", "/api/state/full")


@router.get("/motors/status")
async def get_motor_status(request: Request) -> Response:
    return await _forward_json(request, "GET", "/api/motors/status")


@router.post("/motors/set_mode/{mode}")
async def set_motor_mode(mode: str, request: Request) -> Response:
    return await _forward_json(request, "POST", f"/api/motors/set_mode/{mode}")


@router.post("/move/goto")
async def move_goto(request: Request) -> Response:
    return await _forward_json(
        request,
        "POST",
        "/api/move/goto",
        json_body=await request.json(),
    )


@router.post("/move/stop")
async def stop_move(request: Request) -> Response:
    body = await request.json()
    return await _forward_json(request, "POST", "/api/move/stop", json_body=body)


@router.get("/move/local-categories")
async def get_local_categories(request: Request) -> Response:
    try:
        return await _forward_json(request, "GET", "/api/move/local-categories")
    except HTTPException as exc:
        if exc.status_code != 502:
            raise
        return JSONResponse(content=_local_categories())


@router.get("/move/local-moves/list/{category}")
async def get_local_moves(category: str, request: Request) -> Response:
    try:
        return await _forward_json(request, "GET", f"/api/move/local-moves/list/{category}")
    except HTTPException as exc:
        if exc.status_code != 502:
            raise
        return JSONResponse(content=_local_moves(category))


@router.post("/move/play/local-move/{move_name}")
async def play_local_move(move_name: str, request: Request) -> Response:
    return await _forward_json(request, "POST", f"/api/move/play/local-move/{move_name}")


@router.post("/move/play/wake_up")
async def wake_up(request: Request) -> Response:
    return await _forward_json(request, "POST", "/api/move/play/wake_up")


@router.post("/move/play/goto_sleep")
async def goto_sleep(request: Request) -> Response:
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
    global _local_daemon_process

    http_probe = await _probe_daemon_http(timeout_s=1.0)
    if http_probe["ok"]:
        return JSONResponse(
            content={
                "ok": True,
                "message": "Reachy Mini local daemon is already reachable.",
                **_local_daemon_status_payload(http_probe=http_probe),
            }
        )

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

    serial = _serial_status(check_open=False)
    if not serial["exists"]:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "reachy_serial_missing",
                "message": f"Reachy Mini serial port not found: {_reachy_serial_port()}",
                "hint": "Plug Reachy Mini into USB or set REACHY_MINI_SERIAL_PORT.",
                "serial": serial,
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
            serial=_reachy_serial_port(),
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
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "reachy_local_daemon_start_failed",
                "message": f"Failed to start Reachy Mini local daemon: {exc!s}",
                "command": cmd,
            },
        ) from exc

    # Give uvicorn a moment to bind, then report best-effort status.
    await asyncio.sleep(1.0)
    http_probe = await _probe_daemon_http(timeout_s=1.0)
    return JSONResponse(
        content={
            "ok": _local_daemon_running(),
            "message": "Reachy Mini local daemon start requested.",
            "command": cmd,
            **_local_daemon_status_payload(http_probe=http_probe),
        }
    )


@router.post("/local-daemon/stop")
async def stop_local_daemon() -> Response:
    global _local_daemon_process

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
    http_probe = await _probe_daemon_http(timeout_s=1.0)
    return JSONResponse(
        content={
            "ok": True,
            "message": "BotverseX-managed Reachy Mini local daemon stopped.",
            **_local_daemon_status_payload(http_probe=http_probe),
        }
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
    serial = _serial_status(check_open=not bool(http_probe["ok"]))

    return JSONResponse(
        content={
            "ok": bool(http_probe["ok"]) and bool(serial["exists"]),
            "transport": "usb_serial",
            "service": "local_daemon",
            "serial": serial,
            "daemon_http": http_probe,
            "local_daemon": _local_daemon_status_payload(http_probe=http_probe),
            "resolution": resolved,
            "hints": [
                "Plug Reachy Mini into USB and confirm /dev/ttyACM0 exists.",
                "Click Start Local Daemon if daemon_http.ok=false.",
                "If permission denied, add the user running apps/realtime to dialout or fix udev permissions.",
                "Set REACHY_MINI_SERIAL_PORT if Reachy appears on a different /dev/ttyACM* path.",
                "This endpoint ignores ALL_PROXY/HTTP_PROXY/HTTPS_PROXY by design.",
            ],
        }
    )


@ws_router.websocket("/ws/reachy/state")
async def websocket_reachy_state(websocket: WebSocket) -> None:
    """Bridge the daemon state stream into BotverseX's browser origin."""
    await websocket.accept()

    query = {
        "frequency": websocket.query_params.get("frequency", "10"),
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
            # Disable proxy env (ALL_PROXY / HTTP_PROXY / HTTPS_PROXY). A SOCKS proxy
            # would require python-socks and is not desired for LAN robot daemons.
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
                "hint": "Check that the Reachy Mini daemon is running and reachable.",
                "error": f"{type(exc).__name__}: {exc!s}",
                "exception_type": type(exc).__name__,
                "exception_repr": repr(exc),
                "timestamp": time.time(),
            })
        except Exception:
            pass
