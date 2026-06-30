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


def _owned_by_ssh_tunnel(local_port: int) -> bool | None:
    """Is an `ssh -L <local_port>:...` forward (our tunnel) what's holding this port?

    True = an ssh tunnel owns it (safe to reuse), False = something else is bound to it
    (collision — connecting would silently hit the wrong service), None = can't tell
    (pgrep missing/failed); caller falls back to trusting the open port.
    """
    try:
        out = subprocess.run(["pgrep", "-fl", "ssh"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode not in (0, 1):  # 0 = matches, 1 = no ssh procs; anything else is an error
        return None
    return any(f"-L {local_port}:" in line for line in out.stdout.splitlines())


def ensure_tunnel(host: str, forwards: list[tuple[int, int]]) -> None:
    """Ensure SSH -L forwards are open: each (local_port, remote_port) reachable as localhost:local_port."""
    missing: list[tuple[int, int]] = []
    for local_port, remote_port in forwards:
        if _port_open(local_port):
            # An open port isn't proof it's *our* tunnel — a foreign service (e.g. another
            # project's Postgres) on the same port gets silently connected to instead, surfacing
            # downstream as a baffling auth error. Fail loud when we're sure it's not ours.
            if _owned_by_ssh_tunnel(local_port) is False:
                raise RuntimeError(
                    f"TUNNEL | localhost:{local_port} is in use but not by an SSH tunnel — "
                    f"another service is bound to it. Free that port or change the local tunnel port."
                )
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
