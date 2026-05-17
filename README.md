# BotverseX Local Agent

BotverseX Local Agent is the hardware bridge for BotverseX. It runs on the computer connected to your robot arm and camera, exposes local HTTP/WebSocket APIs on `localhost:8002`, and lets the BotverseX cloud website perform teleoperation, calibration, camera preview, and dataset recording without uploading raw hardware access to the cloud.

Cloud app: http://botversex.feispace.me

## What It Does

- Detect USB cameras and serial robot-arm ports
- Calibrate SO-101 leader/follower arms
- Run leader-to-follower teleoperation
- Record LeRobot-compatible datasets locally
- Serve URDF assets and realtime joint WebSocket streams

## Requirements

- Python 3.10 or newer
- A USB robot arm such as SO-101
- A USB camera for vision recording
- Git

Linux is the primary supported platform for hardware control. macOS and Windows are useful for development, but some serial/camera behavior may differ by driver and device.

## Quick Start

### Linux / macOS

```bash
git clone https://github.com/metahubaifeel/botversex-local-agent.git
cd botversex-local-agent

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python run_api.py
```

### Windows

```powershell
git clone https://github.com/metahubaifeel/botversex-local-agent.git
cd botversex-local-agent

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
python run_api.py
```

When the agent is running, open:

```text
http://localhost:8002/api/setup/health
```

You should see a JSON health response. Then return to the BotverseX website and open the Setup page.

## Linux Device Permissions

If serial ports are detected but cannot be opened, add your user to the `dialout` group and log out/in:

```bash
sudo usermod -aG dialout $USER
```

If cameras are detected but cannot be opened, make sure no other application is using them and that your user has access to `/dev/video*`.

## Common Endpoints

- `GET /api/setup/health` -- Local Agent readiness and hardware summary
- `GET /api/setup/ports` -- serial port scan
- `POST /api/setup/wiggle` -- identify a robot arm by moving its gripper
- `GET /api/camera/devices` -- camera scan
- `POST /api/teleop/start` -- start leader/follower teleoperation
- `POST /api/recording/start` -- start local dataset recording
- `WS /ws/ui` -- realtime joint updates for the web UI

## Data Privacy

The Local Agent runs on your own computer. Camera streams, serial access, calibration files, and recorded datasets stay local unless you explicitly upload or sync them through another tool.

## Repository Contents

```text
api/              FastAPI routes for setup, camera, teleop, recording, calibration
sender/           Robot sender and motor utilities
runtimes/         Runtime registry for supported robots
botclaw_spec/     Shared BotClaw protocol models
dora-bambot/      URDF and mesh assets
config/           Robot runtime configuration
run_api.py        Local Agent entry point
requirements.txt  Python dependencies
```
