# Comfy Pilot Skills Specification

A **Skill** is a shareable, reusable workflow component that Claude Code can understand, deploy, and optimize.

---

## Skill Anatomy

```
my-workflow-skill/
├── skill.json           # Metadata & configuration
├── workflow.json        # ComfyUI workflow graph
├── README.md            # Human-readable documentation
├── examples/
│   ├── example1.md      # Real usage example
│   └── example2.md
└── tests/
    └── validation.py    # Skill validation tests
```

---

## skill.json Format

```json
{
  "id": "sdxl-generation",
  "name": "SDXL Generation Base",
  "version": "1.0.0",
  "author": "comfy-pilot",
  "description": "Production-ready SDXL pipeline with quality settings and optimization",

  "metadata": {
    "category": "generation",
    "tags": ["sdxl", "generation", "core", "stable"],
    "comfyui_version": ">=0.1.0",
    "dependencies": [
      "comfy_core",
      "comfyui_controlnet"
    ]
  },

  "inputs": {
    "checkpoint_name": {
      "type": "string",
      "description": "Model checkpoint to use",
      "default": "sd_xl_base_1.0.safetensors",
      "required": true
    },
    "positive_prompt": {
      "type": "string",
      "description": "Positive prompt for generation",
      "required": true
    },
    "negative_prompt": {
      "type": "string",
      "description": "Negative prompt",
      "default": "blurry, low quality, artifacts"
    },
    "seed": {
      "type": "integer",
      "description": "Random seed (-1 for random)",
      "default": -1
    },
    "steps": {
      "type": "integer",
      "description": "Sampling steps",
      "min": 1,
      "max": 150,
      "default": 30
    },
    "cfg_scale": {
      "type": "number",
      "description": "Classifier-free guidance scale",
      "min": 1.0,
      "max": 30.0,
      "default": 7.5
    }
  },

  "outputs": {
    "image": {
      "type": "image",
      "description": "Generated image"
    },
    "metadata": {
      "type": "object",
      "description": "Generation metadata (seed, params, etc)"
    }
  },

  "nodes_created": [
    "CheckpointLoader",
    "CLIPTextEncode",
    "KSampler",
    "VAEDecode",
    "PreviewImage"
  ],

  "performance": {
    "estimated_time": "45 seconds",
    "estimated_vram": 8,
    "estimated_ram": 4
  },

  "rating": 4.8,
  "downloads": 1240,
  "updated": "2025-01-28"
}
```

---

## workflow.json Format

Standard ComfyUI workflow JSON with position hints:

```json
{
  "nodes": [
    {
      "id": 1,
      "type": "CheckpointLoader",
      "title": "Checkpoint Loader",
      "pos": [100, 100],
      "size": [300, 100],
      "widgets_values": ["sd_xl_base_1.0.safetensors"],
      "skill_input": "checkpoint_name"
    },
    {
      "id": 2,
      "type": "CLIPTextEncode",
      "title": "Positive Prompt",
      "pos": [420, 100],
      "size": [300, 150],
      "widgets_values": [],
      "skill_input": "positive_prompt"
    }
  ],
  "links": [
    [1, 1, 0, 2, 0, "CLIP"]
  ],
  "groups": [],
  "config": {},
  "extra": {},
  "version": 0.4
}
```

---

## README.md Example

```markdown
# SDXL Generation Base

Production-ready SDXL workflow for high-quality image generation.

## What It Does

Creates a complete SDXL pipeline:
1. Loads checkpoint
2. Encodes positive + negative prompts
3. Samples with optimized settings
4. Decodes to image
5. Previews result

## Quick Start

```python
# Claude Code usage
skill sdxl-generation --positive "cinematic portrait" --steps 30
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| checkpoint_name | string | sd_xl_base_1.0 | Model to use |
| positive_prompt | string | - | What to generate |
| negative_prompt | string | "blurry, artifacts" | What to avoid |
| steps | int | 30 | Sampling steps (1-150) |
| cfg_scale | float | 7.5 | Guidance strength |
| seed | int | -1 | Random seed |

## Performance

- **Time:** ~45 seconds (RTX 4090)
- **VRAM:** 8GB
- **Quality:** Production-ready

## Examples

### Portrait
```
positive: "professional headshot, studio lighting, 50mm lens"
negative: "blurry, oversaturated"
steps: 40
cfg: 8.0
```

### Landscape
```
positive: "cinematic landscape, golden hour, 8k"
negative: "people, artifacts"
steps: 35
cfg: 7.5
```

## Optimization Tips

- Increase `steps` for higher quality (costs time)
- Increase `cfg` for more prompt adherence (10+ can distort)
- Use seed `-1` for randomness, fixed seed for reproducibility
- Negative prompt helps most with artifacts and unwanted elements

## Community Variants

- `sdxl-turbo`: Fast version (4 steps)
- `sdxl-refined`: Quality version (50 steps, cfg 9.0)
- `sdxl-with-controlnet`: Adds ControlNet for pose control

## License

MIT
```

---

## Validation Rules

Skills MUST:
- [ ] Have valid `skill.json` with all required fields
- [ ] Have corresponding `workflow.json`
- [ ] Document all inputs and outputs
- [ ] Include at least one example
- [ ] Pass ComfyUI workflow validation
- [ ] Have clear, honest descriptions
- [ ] Specify performance estimates

Skills SHOULD:
- [ ] Include performance benchmarks
- [ ] Have community examples/variants
- [ ] Be tested and working
- [ ] Have clear usage documentation

---

## Installation & Discovery

Claude Code discovers skills via:
1. Local skill registry
2. ComfyUI Manager integration
3. GitHub searches
4. Community registry

```bash
# Claude Code commands
skill list                    # Show all available skills
skill info sdxl-generation    # Get details
skill install sdxl-generation # Install and register
skill deploy sdxl-generation  # Build workflow from skill
```

---

## Rating & Ranking

Skills are ranked by:
- **Downloads** (most used = most trusted)
- **User rating** (0-5 stars)
- **Stability** (version history, breaking changes)
- **Performance** (estimated vs actual time)
- **Quality** (documentation, examples)

---

## Community Skills

Users can contribute skills:
1. Create workflow + skill.json
2. Validate with `skill validate`
3. Push to GitHub
4. Submit to registry
5. Community votes/rates

Top community skills get featured and promoted.

---

## Versioning

Skills follow semantic versioning:
- **MAJOR:** Workflow structure changes (input/output changes)
- **MINOR:** New optional features, optimizations
- **PATCH:** Bug fixes, documentation updates

Breaking changes require explicit major version bump.

---

## Future: AI Optimization

Claude Code can analyze skills and suggest optimizations:

```
"This skill is slow. I suggest:
 - Reducing steps from 40 to 30 (faster, similar quality)
 - Using DPM++ sampler (30% faster)
 - Enabling memory optimization (half VRAM usage)

Should I apply these changes?"
```

This happens via Claude analyzing your workflow graph + performance data.
