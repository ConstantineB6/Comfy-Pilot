# Comfy Pilot on Modal

Deploy Comfy Pilot skills as serverless GPU API endpoints on [Modal](https://modal.com).

## Setup

```bash
pip install modal
modal setup  # authenticate
```

## Deploy

```bash
modal deploy deploy/modal/comfyapp.py
```

This builds a container with ComfyUI, downloads Flux Schnell + SDXL + Real-ESRGAN models to a persistent volume, and exposes two endpoints:

- `GET /skills` - List available skills
- `POST /api` - Run a skill with parameters

## Usage

```bash
# List skills
python deploy/modal/comfyclient.py --list-skills

# Generate with Flux
python deploy/modal/comfyclient.py --skill flux-generation --prompt "a puffin on a cliff"

# Generate with SDXL
python deploy/modal/comfyclient.py --skill sdxl-generation --prompt "portrait, studio lighting"

# Upscale (requires input image in ComfyUI input folder)
python deploy/modal/comfyclient.py --skill upscale-4x
```

## API

```bash
# POST to the api endpoint
curl -X POST https://<workspace>--comfy-pilot-comfyui-api.modal.run \
  -H "Content-Type: application/json" \
  -d '{"skill": "flux-generation", "prompt": "cyberpunk city at night"}' \
  --output result.png
```

### Request Body

```json
{
  "skill": "flux-generation",
  "prompt": "a puffin on a cliff at sunset",
  "negative_prompt": "blurry",
  "seed": 42,
  "steps": 20
}
```

## Scaling

| Strategy | Latency | Cost | Use Case |
|----------|---------|------|----------|
| Default (1 container/request) | ~5s | $0.18/min | Low-latency API |
| `@modal.concurrent(max_inputs=5)` | ~30s | $0.02/min | Batch processing |
| `min_containers=3` | ~2s (warm) | $0.09/min | Always-on service |

Edit `comfyapp.py` to adjust:
- `gpu="L40S"` - GPU type (A10G, L40S, A100, H100)
- `scaledown_window=300` - Seconds before idle containers shut down
- `@modal.concurrent(max_inputs=N)` - Requests per container

## Models

Models are downloaded at build time and cached on a Modal Volume (`comfy-pilot-cache`):
- Flux Schnell fp8 (fast generation)
- SDXL Base 1.0 (standard generation)
- Real-ESRGAN 4x (upscaling)

To add more models, add download functions in `comfyapp.py` and rebuild.

## Cost Estimate

- L40S: ~$0.89/hr (pay per second)
- Flux Schnell: ~5s/image = ~$0.001/image
- SDXL 30 steps: ~20s/image = ~$0.005/image
- Idle scale-to-zero: $0
