#!/usr/bin/env python3
"""
Start the BotverseX Local Agent FastAPI service (:8002).

Usage:
    cd botversex-local-agent
    python run_api.py
"""

import io
import os
import sys

# Windows 控制台默认 cp936 / gbk, convert_to_lerobot.py 里的 emoji print 会抛
# UnicodeEncodeError. 这里显式把 stdout/stderr 切 utf-8, 否则录制停止时 parquet
# 转换会因 print 失败而中断.
for _stream_name in ("stdout", "stderr"):
    _stream = getattr(sys, _stream_name)
    if hasattr(_stream, "buffer") and getattr(_stream, "encoding", "").lower() != "utf-8":
        try:
            setattr(
                sys,
                _stream_name,
                io.TextIOWrapper(_stream.buffer, encoding="utf-8", line_buffering=True),
            )
        except Exception:
            pass

# 让 `api` 包可被 import (因为 main.py 使用 from .websocket import ...)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import uvicorn


def main():
    print("=" * 60)
    print("BotverseX Local Agent")
    print("=" * 60)
    print("API:                http://localhost:8002")
    print("WebSocket (teleop): ws://localhost:8002/ws/teleop")
    print("WebSocket (UI):     ws://localhost:8002/ws/ui")
    print("URDF:               http://localhost:8002/api/urdf/so101.urdf")
    print("=" * 60)

    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8002,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
