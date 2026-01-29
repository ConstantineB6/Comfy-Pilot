"""Client for Comfy Pilot on Modal.

Usage:
    python deploy/modal/comfyclient.py --prompt "a puffin on a cliff"
    python deploy/modal/comfyclient.py --skill flux-generation --prompt "cyberpunk city"
    python deploy/modal/comfyclient.py --list-skills
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests


def get_modal_url(workspace: str) -> str:
    """Construct the Modal endpoint URL."""
    return f"https://{workspace}--comfy-pilot-comfyui"


def list_skills(base_url: str) -> None:
    """List available skills from the Modal deployment."""
    resp = requests.get(f"{base_url}-skills.modal.run")
    resp.raise_for_status()
    data = resp.json()

    print("Available Comfy Pilot Skills:")
    print("-" * 50)
    for skill in data.get("skills", []):
        print(f"  {skill['id']:25s} {skill['name']} ({skill['category']})")


def generate(base_url: str, skill: str, prompt: str, output: str, **kwargs) -> None:
    """Generate an image using a Comfy Pilot skill on Modal."""
    payload = {
        "skill": skill,
        "prompt": prompt,
        **{k: v for k, v in kwargs.items() if v is not None},
    }

    print(f"Generating with skill '{skill}'...")
    print(f"Prompt: {prompt}")

    resp = requests.post(f"{base_url}-api.modal.run", json=payload)
    resp.raise_for_status()

    if resp.headers.get("content-type", "").startswith("image/"):
        Path(output).write_bytes(resp.content)
        print(f"Saved to {output} ({len(resp.content):,} bytes)")
    else:
        data = resp.json()
        if "error" in data:
            print(f"Error: {data['error']}", file=sys.stderr)
            sys.exit(1)
        print(data)


def main():
    parser = argparse.ArgumentParser(description="Comfy Pilot Modal Client")
    parser.add_argument(
        "--modal-workspace",
        default=None,
        help="Modal workspace name (default: read from `modal profile current`)",
    )
    parser.add_argument("--prompt", default="a puffin on a cliff at sunset", help="Generation prompt")
    parser.add_argument("--skill", default="flux-generation", help="Skill to use")
    parser.add_argument("--negative-prompt", default=None, help="Negative prompt")
    parser.add_argument("--output", "-o", default="output.png", help="Output file path")
    parser.add_argument("--list-skills", action="store_true", help="List available skills")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--steps", type=int, default=None, help="Sampling steps")

    args = parser.parse_args()

    workspace = args.modal_workspace
    if workspace is None:
        import subprocess

        result = subprocess.run(["modal", "profile", "current"], capture_output=True, text=True)
        workspace = result.stdout.strip()
        if not workspace:
            print("Error: Could not determine Modal workspace. Use --modal-workspace.", file=sys.stderr)
            sys.exit(1)

    base_url = get_modal_url(workspace)

    if args.list_skills:
        list_skills(base_url)
    else:
        generate(
            base_url,
            skill=args.skill,
            prompt=args.prompt,
            output=args.output,
            negative_prompt=args.negative_prompt,
            seed=args.seed,
            steps=args.steps,
        )


if __name__ == "__main__":
    main()
