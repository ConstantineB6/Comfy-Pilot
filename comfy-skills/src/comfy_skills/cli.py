"""CLI entry point for comfy-skills."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from comfy_skills import __version__
from comfy_skills.registry import (
    CACHE_DIR,
    download_skill,
    fetch_registry,
    fetch_skill_detail,
    list_skills,
    load_local_registry,
    search_skills,
)

console = Console()

REPO_URL = "https://github.com/ConstantineB6/Comfy-Pilot"


def _run(coro):
    """Run an async coroutine."""
    return asyncio.run(coro)


def _get_registry(local: str | None) -> dict:
    """Load registry from local path or remote."""
    if local:
        return load_local_registry(Path(local))
    return _run(fetch_registry())


def _show_welcome() -> None:
    console.print(Panel(
        f"[bold]Comfy Pilot Skills[/bold] v{__version__}\n\n"
        "Deploy ComfyUI workflow skills at conversation speed.\n"
        "Tell Claude what you want. Watch it happen.\n\n"
        "[bold]Skills:[/bold]\n"
        "  skills list                    List all available skills\n"
        "  skills search [cyan]<query>[/cyan]          Search by name or tag\n"
        "  skills info [cyan]<skill-id>[/cyan]         Show full details\n"
        "  skills install [cyan]<skill-id>[/cyan]      Download a skill\n"
        "  skills registry                Registry stats\n\n"
        "[bold]MLX (Apple Silicon):[/bold]\n"
        "  skills generate [cyan]<prompt>[/cyan]       Generate image locally via mflux\n"
        "  skills models                  List MLX models\n\n"
        "[bold]Flox:[/bold]\n"
        "  skills env                     Flox + effective-topos status\n"
        "  skills flox-run [cyan]<cmd>[/cyan]          Run inside effective-topos env\n\n"
        "[bold]Ecosystems:[/bold]\n"
        "  skills ecosystems              Browse external skill ecosystems\n\n"
        f"[dim]Love it? Star us:[/dim] [link={REPO_URL}]{REPO_URL}[/link]",
        border_style="cyan",
    ))


@click.group(invoke_without_command=True)
@click.version_option(__version__, prog_name="comfy-skills")
@click.option(
    "--local",
    envvar="COMFY_PILOT_PATH",
    default=None,
    help="Path to local comfy-pilot checkout (or set COMFY_PILOT_PATH)",
)
@click.pass_context
def main(ctx: click.Context, local: str | None) -> None:
    """Deploy ComfyUI workflow skills at conversation speed."""
    ctx.ensure_object(dict)
    ctx.obj["local"] = local
    if ctx.invoked_subcommand is None:
        _show_welcome()


@main.command("list")
@click.option("--category", "-c", default=None, help="Filter by category")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
@click.pass_context
def list_cmd(ctx: click.Context, category: str | None, json_output: bool) -> None:
    """List all available skills."""
    registry = _get_registry(ctx.obj["local"])
    skills = list_skills(registry)

    if category:
        skills = [s for s in skills if s.category == category]

    if json_output:
        click.echo(json.dumps([{"id": s.id, "name": s.name, "version": s.version, "category": s.category, "rating": s.rating, "downloads": s.downloads} for s in skills], indent=2))
        return

    table = Table(title="Available Skills", show_lines=False)
    table.add_column("Skill", style="cyan bold")
    table.add_column("Version", style="green")
    table.add_column("Category", style="yellow")
    table.add_column("Rating", justify="right")
    table.add_column("Downloads", justify="right", style="blue")
    table.add_column("Status", style="magenta")

    for s in skills:
        star = "*" if s.featured else ""
        rating = f"{s.rating:.1f}" if s.rating else "-"
        table.add_row(
            f"{star}{s.name}",
            s.version,
            s.category,
            rating,
            f"{s.downloads:,}",
            s.status,
        )

    console.print(table)

    stats = registry.get("stats", {})
    if stats:
        console.print(
            f"\n[dim]{stats.get('total_downloads', 0):,} total downloads | "
            f"{stats.get('active_users', 0):,} active users | "
            f"{stats.get('contributors', 0)} contributors[/dim]"
        )


@main.command()
@click.argument("query")
@click.pass_context
def search(ctx: click.Context, query: str) -> None:
    """Search skills by name, description, or tags."""
    registry = _get_registry(ctx.obj["local"])
    skills = list_skills(registry)
    results = search_skills(skills, query)

    if not results:
        console.print(f"[yellow]No skills matching '{query}'[/yellow]")
        return

    for s in results:
        featured = " [green]*featured*[/green]" if s.featured else ""
        console.print(f"[cyan bold]{s.name}[/cyan bold] v{s.version}{featured}")
        console.print(f"  {s.description}")
        console.print(f"  [dim]Category: {s.category} | Rating: {s.rating} | Downloads: {s.downloads:,}[/dim]")
        console.print()


@main.command()
@click.argument("skill_id")
@click.pass_context
def info(ctx: click.Context, skill_id: str) -> None:
    """Show detailed information about a skill."""
    registry = _get_registry(ctx.obj["local"])
    skills = list_skills(registry)
    matches = [s for s in skills if s.id == skill_id]

    if not matches:
        console.print(f"[red]Skill '{skill_id}' not found[/red]")
        sys.exit(1)

    skill = matches[0]
    detail = _run(fetch_skill_detail(skill))

    console.print(Panel(
        f"[bold]{detail.skill.name}[/bold] v{detail.skill.version}\n"
        f"by {detail.skill.author}\n\n"
        f"{detail.skill.description}",
        title=f"[cyan]{detail.skill.id}[/cyan]",
        border_style="cyan",
    ))

    if detail.inputs:
        console.print("\n[bold]Inputs:[/bold]")
        for name, spec in detail.inputs.items():
            req = "[red]*[/red]" if spec.get("required") else ""
            default = f" [dim](default: {spec.get('default')})[/dim]" if "default" in spec else ""
            console.print(f"  {req}[cyan]{name}[/cyan]: {spec.get('type', '?')} - {spec.get('description', '')}{default}")

    if detail.nodes_created:
        console.print(f"\n[bold]Nodes Created:[/bold] {', '.join(detail.nodes_created)}")

    if detail.performance:
        perf = detail.performance
        console.print(f"\n[bold]Performance:[/bold]")
        console.print(f"  Time: ~{perf.get('estimated_time_seconds', '?')}s | VRAM: {perf.get('estimated_vram_gb', '?')}GB | RAM: {perf.get('estimated_ram_gb', '?')}GB")
        if perf.get("notes"):
            console.print(f"  [dim]{perf['notes']}[/dim]")

    if detail.examples:
        console.print(f"\n[bold]Examples:[/bold]")
        for ex in detail.examples:
            console.print(f"  [green]{ex.get('name', 'Example')}[/green]: \"{ex.get('positive_prompt', '')}\"")


@main.command()
@click.argument("skill_id")
@click.option("--dest", "-d", default=None, help="Destination directory")
@click.pass_context
def install(ctx: click.Context, skill_id: str, dest: str | None) -> None:
    """Download and install a skill locally."""
    registry = _get_registry(ctx.obj["local"])
    skills = list_skills(registry)
    matches = [s for s in skills if s.id == skill_id]

    if not matches:
        console.print(f"[red]Skill '{skill_id}' not found[/red]")
        sys.exit(1)

    skill = matches[0]
    dest_path = Path(dest) if dest else CACHE_DIR / "installed" / skill.id

    with console.status(f"[bold green]Installing {skill.name}..."):
        result = _run(download_skill(skill, dest_path))

    console.print(f"[green]Installed {skill.name} v{skill.version} to {result}[/green]")

    if (result / "workflow.json").exists():
        console.print(f"\n[bold]Deploy with Claude Code:[/bold]")
        console.print(f'  Tell Claude: "Load the skill from {result}/workflow.json"')


@main.command()
@click.pass_context
def registry(ctx: click.Context) -> None:
    """Show registry statistics."""
    reg = _get_registry(ctx.obj["local"])
    stats = reg.get("stats", {})
    categories = reg.get("categories", {})

    console.print(Panel(
        f"[bold]Registry v{reg.get('version', '?')}[/bold]\n"
        f"Updated: {reg.get('registry_version', '?')}\n\n"
        f"Total skills: {reg.get('total_skills', 0)}\n"
        f"Downloads: {stats.get('total_downloads', 0):,}\n"
        f"Active users: {stats.get('active_users', 0):,}\n"
        f"Contributors: {stats.get('contributors', 0)}\n\n"
        f"Categories: {', '.join(f'{k} ({v})' for k, v in categories.items())}",
        title="[cyan]Comfy Pilot Skills Registry[/cyan]",
        border_style="cyan",
    ))

    upcoming = reg.get("upcoming_skills", [])
    if upcoming:
        console.print("\n[bold]Upcoming Skills:[/bold]")
        for s in upcoming:
            console.print(f"  [yellow]{s['name']}[/yellow] - {s.get('description', '')} [dim](ETA: {s.get('estimated_release', '?')})[/dim]")


@main.command()
@click.pass_context
def ecosystems(ctx: click.Context) -> None:
    """Browse external skill ecosystems (Trail of Bits, Plurigrid, SDF)."""
    registry = _get_registry(ctx.obj["local"])
    ext = registry.get("external_ecosystems", [])

    if not ext:
        console.print("[yellow]No external ecosystems found in registry[/yellow]")
        return

    for eco in ext:
        source = eco.get("source", "")
        name_line = f"[cyan bold]{eco['name']}[/cyan bold]"
        if eco.get("stars"):
            name_line += f"  [dim]{eco['stars']:,} stars[/dim]"

        console.print(name_line)
        console.print(f"  {eco['description']}")

        if eco.get("total_skills"):
            console.print(f"  [green]{eco['total_skills']} skills[/green]  Categories: {', '.join(eco.get('categories', []))}")

        if eco.get("install"):
            console.print(f"  [bold]Install:[/bold] [cyan]{eco['install']}[/cyan]")

        if source.startswith("http"):
            console.print(f"  [dim]{source}[/dim]")
        elif source:
            console.print(f"  [dim]Source: {source}[/dim]")

        if eco.get("highlights"):
            console.print(f"  [bold]Highlights:[/bold]")
            for h in eco["highlights"]:
                console.print(f"    - {h}")

        console.print()


@main.command()
@click.argument("prompt")
@click.option("--model", "-m", default="schnell", help="MLX model (schnell, dev, z-image-turbo, flux2-klein-4b, flux2-klein-9b, fibo, qwen)")
@click.option("--width", "-W", default=1024, help="Image width")
@click.option("--height", "-H", default=1024, help="Image height")
@click.option("--steps", "-s", default=None, type=int, help="Sampling steps")
@click.option("--seed", default=-1, help="Random seed (-1 for random)")
@click.option("--quantize", "-q", default=8, type=int, help="Quantization bits (3-8, lower=faster)")
@click.option("--output", "-o", default="output.png", help="Output file path")
def generate(prompt: str, model: str, width: int, height: int, steps: int | None, seed: int, quantize: int, output: str) -> None:
    """Generate an image with MLX on Apple Silicon (via mflux).

    Runs entirely on-device using Metal. No cloud API needed.

    Example: skills generate "a puffin on a cliff at sunset" -m schnell -q 8
    """
    from comfy_skills.mlx import MLXGenerateConfig, run_mflux

    config = MLXGenerateConfig(
        prompt=prompt,
        model=model,
        width=width,
        height=height,
        steps=steps,
        seed=seed,
        quantize=quantize,
        output=output,
    )
    run_mflux(config)


@main.command()
def models() -> None:
    """List available MLX image generation models."""
    from comfy_skills.mlx import list_models

    table = Table(title="MLX Models (Apple Silicon)", show_lines=False)
    table.add_column("Model", style="cyan bold")
    table.add_column("Description", style="white")

    for name, desc in list_models().items():
        table.add_row(name, desc)

    console.print(table)
    console.print("\n[dim]All models run locally on Apple Silicon via mflux.[/dim]")
    console.print("[dim]Install: uvx --from mflux mflux-generate --help[/dim]")


@main.command()
def env() -> None:
    """Show flox environment status and effective-topos integration."""
    from comfy_skills.flox import flox_activate_cmd, flox_status

    status = flox_status()

    rows = [
        f"[bold]Flox Environment[/bold]\n",
        f"  flox installed:           {'[green]yes[/green]' if status['flox_installed'] else '[red]no[/red]'}",
        f"  effective-topos installed: {'[green]yes[/green]' if status['effective_topos_installed'] else '[yellow]no[/yellow]'}",
        f"  active environment:       {status['active_env'] or '[dim]none[/dim]'}",
        f"  env path:                 [dim]{status['env_path']}[/dim]",
    ]

    if not status["effective_topos_installed"]:
        rows.append(f"\n[bold]Install effective-topos:[/bold]")
        rows.append(f"  [cyan]flox pull bmorphism/effective-topos[/cyan]")
        rows.append(f"  [cyan]{flox_activate_cmd()}[/cyan]")
    else:
        rows.append(f"\n[bold]Activate:[/bold]")
        rows.append(f"  [cyan]{flox_activate_cmd()}[/cyan]")

    rows.append(f"\n[bold]What you get:[/bold]")
    rows.append(f"  606 man pages | 97 info manuals | 62 packages")
    rows.append(f"  Guile/Goblins/Hoot + OCaml + Haskell + Rust + Go")
    rows.append(f"  radare2 + gh + tmux + tree-sitter + Gay.jl colors")

    console.print(Panel(
        "\n".join(rows),
        title="[cyan]Flox + effective-topos[/cyan]",
        border_style="cyan",
    ))


@main.command("flox-run")
@click.argument("command", nargs=-1, required=True)
def flox_run(command: tuple[str, ...]) -> None:
    """Run a command inside the effective-topos flox environment.

    Example: skills flox-run guile -c '(display "hello")'
    """
    from comfy_skills.flox import run_in_flox

    run_in_flox(list(command))


def entry() -> None:
    """Entry point that shows welcome panel on bare invocation."""
    if len(sys.argv) == 1:
        _show_welcome()
        return
    main()


if __name__ == "__main__":
    entry()
