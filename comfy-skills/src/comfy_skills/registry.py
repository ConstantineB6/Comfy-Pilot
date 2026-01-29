"""Skill registry: discover, fetch, and cache skill metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

REGISTRY_URL = "https://raw.githubusercontent.com/ConstantineB6/Comfy-Pilot/main/skills/skill-registry.json"
SKILLS_BASE_URL = "https://raw.githubusercontent.com/ConstantineB6/Comfy-Pilot/main/skills"
CACHE_DIR = Path.home() / ".cache" / "comfy-skills"


@dataclass
class Skill:
    id: str
    name: str
    version: str
    author: str
    category: str
    description: str
    tags: list[str] = field(default_factory=list)
    path: str = ""
    rating: float = 0.0
    downloads: int = 0
    status: str = "unknown"
    featured: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Skill:
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            version=data.get("version", "0.0.0"),
            author=data.get("author", "unknown"),
            category=data.get("category", ""),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            path=data.get("path", ""),
            rating=data.get("rating", 0.0),
            downloads=data.get("downloads", 0),
            status=data.get("status", "unknown"),
            featured=data.get("featured", False),
        )


@dataclass
class SkillDetail:
    """Full skill.json contents."""

    skill: Skill
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    performance: dict[str, Any] = field(default_factory=dict)
    nodes_created: list[str] = field(default_factory=list)
    examples: list[dict[str, Any]] = field(default_factory=list)
    workflow: dict[str, Any] | None = None


async def fetch_registry() -> dict[str, Any]:
    """Fetch the skill registry from GitHub."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / "registry.json"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(REGISTRY_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            cache_file.write_text(json.dumps(data, indent=2))
            return data
        except (httpx.HTTPError, httpx.TimeoutException):
            if cache_file.exists():
                return json.loads(cache_file.read_text())
            raise


def load_local_registry(path: Path) -> dict[str, Any]:
    """Load registry from a local comfy-pilot checkout."""
    registry_file = path / "skills" / "skill-registry.json"
    if not registry_file.exists():
        registry_file = path / "skill-registry.json"
    if not registry_file.exists():
        raise FileNotFoundError(f"No skill-registry.json found in {path}")
    return json.loads(registry_file.read_text())


def list_skills(registry: dict[str, Any]) -> list[Skill]:
    """Extract all skills from registry."""
    skills = []
    for entry in registry.get("core_skills", []):
        skills.append(Skill.from_dict(entry))
    for entry in registry.get("community_skills", []):
        skills.append(Skill.from_dict(entry))
    return skills


def search_skills(skills: list[Skill], query: str) -> list[Skill]:
    """Search skills by name, description, or tags."""
    q = query.lower()
    return [
        s
        for s in skills
        if q in s.name.lower()
        or q in s.description.lower()
        or q in s.id.lower()
        or any(q in t.lower() for t in s.tags)
    ]


async def fetch_skill_detail(skill: Skill) -> SkillDetail:
    """Fetch full skill.json for a specific skill."""
    skill_url = f"{SKILLS_BASE_URL}/{skill.path}skill.json"
    workflow_url = f"{SKILLS_BASE_URL}/{skill.path}workflow.json"

    async with httpx.AsyncClient() as client:
        resp = await client.get(skill_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        workflow = None
        try:
            wf_resp = await client.get(workflow_url, timeout=10)
            wf_resp.raise_for_status()
            workflow = wf_resp.json()
        except (httpx.HTTPError, httpx.TimeoutException):
            pass

    return SkillDetail(
        skill=skill,
        inputs=data.get("inputs", {}),
        outputs=data.get("outputs", {}),
        performance=data.get("performance", {}),
        nodes_created=data.get("nodes_created", []),
        examples=data.get("examples", []),
        workflow=workflow,
    )


async def download_skill(skill: Skill, dest: Path) -> Path:
    """Download skill files to a local directory."""
    dest.mkdir(parents=True, exist_ok=True)

    files = ["skill.json", "workflow.json", "README.md"]
    async with httpx.AsyncClient() as client:
        for filename in files:
            url = f"{SKILLS_BASE_URL}/{skill.path}{filename}"
            try:
                resp = await client.get(url, timeout=10)
                resp.raise_for_status()
                (dest / filename).write_bytes(resp.content)
            except (httpx.HTTPError, httpx.TimeoutException):
                pass  # README might not exist

    return dest
