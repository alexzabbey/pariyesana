"""Shared SSH tunnel helper.

Opens -L forwards via `ssh -f -N` so local processes can reach services on a remote host.
Idempotent per local port: already-listening ports are left alone.
"""

import socket
import subprocess
import time


def _port_open(port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(1)
        s.connect(("localhost", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def ensure_tunnel(host: str, forwards: list[tuple[int, int]]) -> None:
    """Ensure SSH -L forwards are open: each (local_port, remote_port) reachable as localhost:local_port."""
    missing: list[tuple[int, int]] = []
    for local_port, remote_port in forwards:
        if _port_open(local_port):
            print(f"TUNNEL | localhost:{local_port} already open")
        else:
            missing.append((local_port, remote_port))

    if not missing:
        return

    forward_args: list[str] = []
    for local_port, remote_port in missing:
        forward_args.extend(["-L", f"{local_port}:localhost:{remote_port}"])
        print(f"TUNNEL | Opening localhost:{local_port} -> {host}:{remote_port}...")

    subprocess.run(
        [
            "ssh", "-f", "-N",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            *forward_args,
            host,
        ],
        check=True,
    )

    for local_port, _ in missing:
        for _ in range(10):
            if _port_open(local_port):
                print(f"TUNNEL | localhost:{local_port} ready")
                break
            time.sleep(0.5)
        else:
            raise RuntimeError(f"TUNNEL | Failed to open localhost:{local_port}")
