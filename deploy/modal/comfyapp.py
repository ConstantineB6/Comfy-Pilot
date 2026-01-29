"""Deploy Comfy Pilot skills on Modal serverless GPUs.

Usage:
    modal deploy deploy/modal/comfyapp.py
    python deploy/modal/comfyclient.py --prompt "a puffin on a cliff"

Based on Modal's official ComfyUI example with Comfy Pilot skill integration.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path

import modal

# --- Image build ---

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("git")
    .uv_pip_install("fastapi[standard]==0.115.4")
    .uv_pip_install("comfy-cli==1.5.3")
    .run_commands(
        "comfy --skip-prompt install --fast-deps --nvidia",
    )
)


def download_flux_schnell():
    """Download Flux Schnell fp8 to a persistent volume."""
    from huggingface_hub import hf_hub_download

    model_path = hf_hub_download(
        repo_id="Comfy-Org/flux1-schnell",
        filename="flux1-schnell-fp8.safetensors",
        cache_dir="/cache",
    )
    subprocess.run(
        f"ln -s {model_path} /root/comfy/ComfyUI/models/checkpoints/flux1-schnell-fp8.safetensors",
        shell=True,
        check=True,
    )


def download_sdxl():
    """Download SDXL base for non-Flux skills."""
    from huggingface_hub import hf_hub_download

    model_path = hf_hub_download(
        repo_id="stabilityai/stable-diffusion-xl-base-1.0",
        filename="sd_xl_base_1.0.safetensors",
        cache_dir="/cache",
    )
    subprocess.run(
        f"ln -s {model_path} /root/comfy/ComfyUI/models/checkpoints/sd_xl_base_1.0.safetensors",
        shell=True,
        check=True,
    )


def download_upscale_model():
    """Download Real-ESRGAN 4x upscale model."""
    from huggingface_hub import hf_hub_download

    model_path = hf_hub_download(
        repo_id="ai-forever/Real-ESRGAN",
        filename="RealESRGAN_x4.pth",
        cache_dir="/cache",
    )
    dest = Path("/root/comfy/ComfyUI/models/upscale_models")
    dest.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        f"ln -s {model_path} {dest}/RealESRGAN_x4plus.pth",
        shell=True,
        check=True,
    )


image = (
    image.uv_pip_install("huggingface_hub")
    .run_function(download_flux_schnell)
    .run_function(download_sdxl)
    .run_function(download_upscale_model)
)

vol = modal.Volume.from_name("comfy-pilot-cache", create_if_missing=True)

app = modal.App("comfy-pilot", image=image)

# --- Skill loading ---

SKILLS_DIR = Path(__file__).parent.parent.parent / "skills" / "core"


def load_skill_workflow(skill_id: str) -> dict:
    """Load a Comfy Pilot skill's workflow.json."""
    skill_path = SKILLS_DIR / skill_id / "workflow.json"
    if not skill_path.exists():
        raise FileNotFoundError(f"Skill '{skill_id}' not found at {skill_path}")
    return json.loads(skill_path.read_text())


def list_available_skills() -> list[dict]:
    """List all available skills from the local registry."""
    registry_path = SKILLS_DIR.parent / "skill-registry.json"
    if not registry_path.exists():
        return []
    registry = json.loads(registry_path.read_text())
    return [
        {"id": s["id"], "name": s["name"], "category": s["category"]}
        for s in registry.get("core_skills", [])
    ]


# --- ComfyUI server ---


@app.cls(
    scaledown_window=300,
    gpu="L40S",
    volumes={"/cache": vol},
    timeout=1200,
)
class ComfyUI:
    port: int = 8188

    @modal.enter()
    def launch_comfy_background(self):
        subprocess.run(
            f"comfy launch --background -- --port {self.port}",
            shell=True,
            check=True,
        )

    def poll_server_health(self):
        """Wait for ComfyUI server to be ready."""
        import urllib.request
        import time

        for _ in range(60):
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{self.port}/system_stats")
                urllib.request.urlopen(req, timeout=2)
                return
            except Exception:
                time.sleep(1)
        raise TimeoutError("ComfyUI server did not start")

    @modal.method()
    def infer(self, workflow_path: str = "/root/workflow_api.json"):
        """Run a ComfyUI workflow and return the output image bytes."""
        self.poll_server_health()
        subprocess.run(
            f"comfy run --workflow {workflow_path} --wait --timeout 1200 --verbose",
            shell=True,
            check=True,
        )
        output_dir = Path("/root/comfy/ComfyUI/output")
        workflow = json.loads(Path(workflow_path).read_text())

        # Find the output filename prefix from SaveImage or PreviewImage nodes
        file_prefix = None
        for node in workflow.get("nodes", workflow.values()) if isinstance(workflow, dict) else []:
            node_data = node if isinstance(node, dict) else {}
            if node_data.get("class_type", node_data.get("type", "")) in (
                "SaveImage",
                "PreviewImage",
            ):
                inputs = node_data.get("inputs", {})
                file_prefix = inputs.get("filename_prefix", "ComfyUI")
                break

        file_prefix = file_prefix or "ComfyUI"
        for f in sorted(output_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if f.name.startswith(file_prefix) and f.suffix in (".png", ".jpg", ".jpeg"):
                return f.read_bytes()
        return None

    @modal.fastapi_endpoint(method="GET")
    def skills(self):
        """List available Comfy Pilot skills."""
        return {"skills": list_available_skills()}

    @modal.fastapi_endpoint(method="POST")
    def api(self, item: dict):
        """Run a Comfy Pilot skill with parameters.

        POST body:
        {
            "skill": "sdxl-generation",
            "prompt": "a puffin on a cliff at sunset",
            "negative_prompt": "blurry",
            "seed": -1,
            "steps": 30
        }
        """
        from starlette.responses import Response

        skill_id = item.get("skill", "flux-generation")
        prompt = item.get("prompt", "a beautiful landscape")

        # Load the skill workflow
        try:
            workflow_data = load_skill_workflow(skill_id)
        except FileNotFoundError:
            return {"error": f"Skill '{skill_id}' not found"}

        # Patch prompt into workflow nodes
        for node in workflow_data.get("nodes", []):
            title = node.get("title", "")
            if "Positive Prompt" in title or title == "Prompt":
                node["widgets_values"] = [prompt]
            elif "Negative Prompt" in title:
                neg = item.get("negative_prompt", "blurry, low quality, distorted")
                node["widgets_values"] = [neg]

        # Set unique output prefix for this request
        client_id = uuid.uuid4().hex[:12]
        for node in workflow_data.get("nodes", []):
            if node.get("type") in ("SaveImage", "PreviewImage"):
                node.setdefault("inputs", {})
                node["inputs"]["filename_prefix"] = client_id

        # Write patched workflow and run
        workflow_file = f"/tmp/{client_id}.json"
        Path(workflow_file).write_text(json.dumps(workflow_data))

        img_bytes = self.infer.local(workflow_file)
        if img_bytes is None:
            return {"error": "No output image produced"}

        return Response(img_bytes, media_type="image/png")
