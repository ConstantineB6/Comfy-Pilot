"""Flox environment integration for skills."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console

console = Console()

FLOX_ENV_PATH = Path.home() / ".topos"


def flox_available() -> bool:
    """Check if flox is installed."""
    return shutil.which("flox") is not None


def flox_env_active() -> str | None:
    """Return active flox environment name, if any."""
    return os.environ.get("FLOX_ENV_DESCRIPTION") or os.environ.get("FLOX_ENV")


def effective_topos_installed() -> bool:
    """Check if effective-topos flox env exists."""
    return (FLOX_ENV_PATH / ".flox").exists()


def flox_activate_cmd(env_name: str = "effective-topos") -> str:
    """Return the command to activate a flox environment."""
    if effective_topos_installed():
        return f"flox activate -d {FLOX_ENV_PATH}"
    return f"flox pull bmorphism/{env_name} && flox activate -d {FLOX_ENV_PATH}"


def run_in_flox(cmd: list[str], env_path: Path | None = None) -> subprocess.CompletedProcess:
    """Run a command inside a flox environment."""
    flox_path = env_path or FLOX_ENV_PATH

    if not (flox_path / ".flox").exists():
        console.print(f"[yellow]Flox environment not found at {flox_path}[/yellow]")
        console.print(f"[dim]Install with: flox pull bmorphism/effective-topos[/dim]")
        sys.exit(1)

    full_cmd = ["flox", "activate", "-d", str(flox_path), "--", *cmd]
    return subprocess.run(full_cmd, capture_output=False)


def list_flox_packages(env_path: Path | None = None) -> list[dict]:
    """List packages in a flox environment."""
    flox_path = env_path or FLOX_ENV_PATH

    result = subprocess.run(
        ["flox", "list", "-d", str(flox_path), "--json"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return []

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def flox_status() -> dict:
    """Get flox environment status."""
    return {
        "flox_installed": flox_available(),
        "effective_topos_installed": effective_topos_installed(),
        "active_env": flox_env_active(),
        "env_path": str(FLOX_ENV_PATH),
    }
