# Flux Generation

Generate images with Flux models - state-of-the-art text-to-image with superior prompt understanding via T5-XXL text encoder.

## Quick Start

```
Tell Claude: "Load the flux skill and generate a puffin on a cliff at sunset"
```

## How It Works

Flux uses a different architecture from SDXL:

1. **Dual text encoders** - CLIP-L + T5-XXL for deep language understanding
2. **UNETLoader** instead of CheckpointLoader (separate model files)
3. **FluxGuidance** replaces CFG scale
4. **No negative prompt** - guidance strength controls adherence
5. **EmptySD3LatentImage** for the latent space

## Model Variants

| Model | Steps | Speed | Quality | VRAM |
|-------|-------|-------|---------|------|
| **flux1-dev** | 20-30 | Medium | Best | 12GB (fp8) |
| **flux1-schnell** | 4 | Very fast | Good | 12GB (fp8) |
| **flux1-dev-fp8** | 20-30 | Medium | Best | 12GB |

## Required Files

Place in the corresponding `ComfyUI/models/` subdirectories:

```
diffusion_models/  flux1-dev.safetensors (or schnell)
text_encoders/     clip_l.safetensors
text_encoders/     t5xxl_fp8_e4m3fn.safetensors
vae/               ae.safetensors
```

## Key Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| **prompt** | string | - | Natural language description |
| **guidance** | number | 3.5 | 1.0-10.0, replaces CFG |
| **steps** | integer | 20 | Dev: 20-30, Schnell: 4 |
| **weight_dtype** | string | fp8_e4m3fn | fp8 saves ~50% VRAM |

## Flux vs SDXL

- Flux understands natural language prompts better (T5 encoder)
- No negative prompt needed
- Generally higher quality at fewer steps
- Requires more VRAM (~12GB vs ~8GB)
- Schnell variant is extremely fast (4 steps)

## License

MIT
