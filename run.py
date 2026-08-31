#!/usr/bin/env python3
"""
One-command local environment manager for this repo's production-shaped
services: Airbyte (via abctl, a Kubernetes cluster) and Dagster + Superset
(via Docker Compose).

    python run.py up        Install/start everything. Idempotent -- safe to
                             re-run; abctl and docker compose both no-op on
                             what's already in the desired state.
    python run.py down      Stop the Dagster/Superset containers. Leaves the
                             Airbyte cluster and named volumes (Superset's
                             saved dashboards) intact.
    python run.py destroy   Remove everything: containers, volumes, and the
                             Airbyte cluster. Asks for confirmation (skip with
                             --yes). Run `up` again afterward for a clean
                             slate.
    python run.py pause     Freeze everything in place (docker stop/compose
                             stop) to free RAM/CPU, without reinstalling or
                             rebuilding anything on the way back.
    python run.py resume    Bring a paused environment back. Fast -- unlike
                             `up`, skips abctl's install/validation checks.
    python run.py status    Show what's currently running.

Cross-platform (Linux/Mac/Windows) by construction: no shell=True subprocess
calls, OS/arch detected via the stdlib `platform` module, and the abctl
binary is fetched per-OS into tools/ if not already present (tools/abctl on
Linux/Mac, tools/abctl.exe on Windows
"""

import argparse
import os
import platform
import stat
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
TOOLS_DIR = REPO_ROOT / "tools"
ABCTL_VERSION = "v0.30.4"
AIRBYTE_CLUSTER_NAME = "airbyte-abctl"  # abctl's fixed default; never overridden here


def _os_arch() -> tuple[str, str]:
    system = platform.system().lower()  # 'linux', 'darwin', 'windows'
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    return system, arch


def _abctl_binary_name() -> str:
    return "abctl.exe" if platform.system() == "Windows" else "abctl"


def ensure_abctl() -> Path:
    """Downloads tools/abctl (or abctl.exe) for this OS/arch if not already present."""
    binary = TOOLS_DIR / _abctl_binary_name()
    if binary.exists():
        return binary

    import requests

    TOOLS_DIR.mkdir(exist_ok=True)
    system, arch = _os_arch()
    ext = "zip" if system == "windows" else "tar.gz"
    url = (
        f"https://github.com/airbytehq/abctl/releases/download/{ABCTL_VERSION}/"
        f"abctl-{ABCTL_VERSION}-{system}-{arch}.{ext}"
    )
    print(f"Downloading abctl {ABCTL_VERSION} for {system}/{arch}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    archive_path = TOOLS_DIR / f"_abctl_download.{ext}"
    archive_path.write_bytes(resp.content)

    if ext == "zip":
        with zipfile.ZipFile(archive_path) as zf:
            member = next(m for m in zf.namelist() if m.endswith("abctl.exe"))
            binary.write_bytes(zf.read(member))
    else:
        with tarfile.open(archive_path) as tf:
            member = next(m for m in tf.getmembers() if m.name.endswith("/abctl"))
            binary.write_bytes(tf.extractfile(member).read())
        binary.chmod(binary.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    archive_path.unlink()
    print(f"abctl ready at {binary}")
    return binary


def _docker_compose_env() -> dict:
    """Sets DOCKER_UID/DOCKER_GID so it runs as the host user, not root 
    """
    env = os.environ.copy()
    if hasattr(os, "getuid"):
        env["DOCKER_UID"] = str(os.getuid())
        env["DOCKER_GID"] = str(os.getgid())
    return env


def ensure_docker() -> None:
    try:
        subprocess.run(
            ["docker", "compose", "version"], check=True, capture_output=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(
            "Docker (with the compose plugin) is required and wasn't found on PATH.\n"
            "Install Docker Desktop (Mac/Windows) or Docker Engine + the compose "
            "plugin (Linux), then re-run.",
            file=sys.stderr,
        )
        sys.exit(1)


def cmd_up(args: argparse.Namespace) -> None:
    ensure_docker()
    abctl = ensure_abctl()

    print("\n=== Airbyte (abctl local install) ===")
    install_args = [str(abctl), "local", "install"]
    if args.low_resource:
        install_args.append("--low-resource-mode")
    subprocess.run(install_args, check=True, cwd=REPO_ROOT)

    print("\n=== Dagster + Superset (docker compose up) ===")
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build"],
        check=True,
        cwd=REPO_ROOT,
        env=_docker_compose_env(),
    )

    print(
        "\nEverything is up:\n"
        "  Airbyte:  http://localhost:8000\n"
        "  Dagster:  http://localhost:3000\n"
        "  Superset: http://localhost:8088\n"
    )


def cmd_down(_args: argparse.Namespace) -> None:
    print(
        "Stopping Dagster + Superset containers (Airbyte cluster and volumes "
        "left intact -- use `destroy` to remove those too)..."
    )
    subprocess.run(["docker", "compose", "down"], check=True, cwd=REPO_ROOT)


def cmd_destroy(args: argparse.Namespace) -> None:
    if not args.yes:
        confirm = input(
            "This removes all containers, volumes (including Superset's saved "
            "dashboards), and uninstalls the Airbyte cluster. Continue? [y/N] "
        )
        if confirm.strip().lower() != "y":
            print("Aborted.")
            return

    print("\n=== Removing Dagster + Superset (containers and volumes) ===")
    subprocess.run(["docker", "compose", "down", "-v"], check=True, cwd=REPO_ROOT)

    abctl = TOOLS_DIR / _abctl_binary_name()
    if abctl.exists():
        print("\n=== Uninstalling Airbyte ===")
        subprocess.run([str(abctl), "local", "uninstall"], check=True, cwd=REPO_ROOT)
    else:
        print("abctl not installed -- nothing to uninstall.")

    print("\nEverything removed. Run `python run.py up` for a clean start.")


def _airbyte_containers() -> list[str]:
    """Docker containers belonging to the abctl-managed kind cluster, found by
    kind's own label rather than a hardcoded container name (robust to naming
    even though abctl's default is fixed today)."""
    result = subprocess.run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=io.x-k8s.kind.cluster={AIRBYTE_CLUSTER_NAME}",
            "--format",
            "{{.Names}}",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return [name for name in result.stdout.splitlines() if name]


def cmd_pause(_args: argparse.Namespace) -> None:
    print("=== Pausing Dagster + Superset (docker compose stop) ===")
    subprocess.run(["docker", "compose", "stop"], check=True, cwd=REPO_ROOT)

    containers = _airbyte_containers()
    if containers:
        print("\n=== Pausing Airbyte (docker stop) ===")
        subprocess.run(["docker", "stop", *containers], check=True, cwd=REPO_ROOT)
    else:
        print("\nAirbyte not installed -- nothing to pause.")

    print(
        "\nEverything paused. Run `python run.py resume` to bring it back -- fast, "
        "no reinstall or rebuild."
    )


def cmd_resume(_args: argparse.Namespace) -> None:
    containers = _airbyte_containers()
    if containers:
        print("=== Resuming Airbyte (docker start) ===")
        subprocess.run(["docker", "start", *containers], check=True, cwd=REPO_ROOT)
    else:
        print("Airbyte not installed -- run `python run.py up` first.")

    print("\n=== Resuming Dagster + Superset (docker compose start) ===")
    subprocess.run(
        ["docker", "compose", "start"],
        check=True,
        cwd=REPO_ROOT,
        env=_docker_compose_env(),
    )

    print(
        "\nEverything resumed:\n"
        "  Airbyte:  http://localhost:8000\n"
        "  Dagster:  http://localhost:3000\n"
        "  Superset: http://localhost:8088\n"
    )


def cmd_status(_args: argparse.Namespace) -> None:
    print("=== Docker Compose services ===")
    subprocess.run(["docker", "compose", "ps"], cwd=REPO_ROOT)

    print("\n=== Airbyte ===")
    abctl = TOOLS_DIR / _abctl_binary_name()
    if abctl.exists():
        subprocess.run([str(abctl), "local", "status"], cwd=REPO_ROOT)
    else:
        print("abctl not installed yet -- run `python run.py up` first.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_up = sub.add_parser("up", help="Start everything (idempotent)")
    p_up.add_argument(
        "--no-low-resource",
        dest="low_resource",
        action="store_false",
        default=True,
        help="Skip --low-resource-mode on abctl (only if you know this machine has room)",
    )
    p_up.set_defaults(func=cmd_up)

    p_down = sub.add_parser("down", help="Stop containers, keep Airbyte + volumes")
    p_down.set_defaults(func=cmd_down)

    p_destroy = sub.add_parser(
        "destroy", help="Remove everything: containers, volumes, Airbyte cluster"
    )
    p_destroy.add_argument(
        "-y", "--yes", action="store_true", help="Skip the confirmation prompt"
    )
    p_destroy.set_defaults(func=cmd_destroy)

    p_pause = sub.add_parser(
        "pause", help="Freeze everything in place (fast resume, no reinstall)"
    )
    p_pause.set_defaults(func=cmd_pause)

    p_resume = sub.add_parser("resume", help="Bring a paused environment back")
    p_resume.set_defaults(func=cmd_resume)

    p_status = sub.add_parser("status", help="Show what's currently running")
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    try:
        args.func(args)
    except subprocess.CalledProcessError as exc:
        print(f"\n{exc.cmd[0]} exited with code {exc.returncode}.", file=sys.stderr)
        sys.exit(exc.returncode)


if __name__ == "__main__":
    main()
