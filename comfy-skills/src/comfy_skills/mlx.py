"""MLX-native skill execution for Apple Silicon."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

console = Console()

MFLUX_MODELS = {
    "schnell": "Fast FLUX.1 (12B, ~10s)",
    "dev": "FLUX.1 Dev (12B, quality)",
    "z-image-turbo": "Z-Image Turbo (6B, fastest)",
    "flux2-klein-4b": "FLUX.2 Klein 4B (compact)",
    "flux2-klein-9b": "FLUX.2 Klein 9B (balanced)",
    "fibo": "FIBO 8B (creative)",
    "qwen": "Qwen Image 20B (highest quality)",
}


@dataclass
class MLXGenerateConfig:
    prompt: str
    model: str = "schnell"
    width: int = 1024
    height: int = 1024
    steps: int | None = None
    seed: int = -1
    quantize: int | None = 8
    output: str = "output.png"

    def to_args(self) -> list[str]:
        args = [
            "--prompt", self.prompt,
            "--base-model", self.model,
            "--width", str(self.width),
            "--height", str(self.height),
            "--output", self.output,
        ]
        if self.steps is not None:
            args.extend(["--steps", str(self.steps)])
        if self.seed >= 0:
            args.extend(["--seed", str(self.seed)])
        if self.quantize is not None:
            args.extend(["--quantize", str(self.quantize)])
        return args


def run_mflux(config: MLXGenerateConfig) -> Path:
    """Run mflux-generate via uvx."""
    args = ["uvx", "--from", "mflux", "mflux-generate"] + config.to_args()

    console.print(f"[bold]Running MLX generation:[/bold] {config.model}")
    console.print(f"[dim]{' '.join(args)}[/dim]\n")

    result = subprocess.run(args, capture_output=False)

    if result.returncode != 0:
        console.print(f"[red]mflux-generate failed with exit code {result.returncode}[/red]")
        sys.exit(1)

    output_path = Path(config.output)
    if output_path.exists():
        console.print(f"\n[green]Generated: {output_path}[/green]")
    return output_path


def list_models() -> dict[str, str]:
    """Return available MLX models."""
    return MFLUX_MODELS
