# Image-to-Image Transform

Transform existing images using text prompts. Control the balance between preserving the original and applying new styles.

## Quick Start

```
Tell Claude: "Load the img2img skill and transform my photo into an oil painting"
```

## How It Works

1. Loads your input image
2. Encodes it to latent space via VAE
3. Adds noise controlled by denoise strength
4. Samples with your text prompt guiding the result
5. Decodes back to pixel space

The **denoise** parameter is the key control:
- `0.3-0.5` - Subtle changes, preserves composition and detail
- `0.6-0.8` - Style transfer zone, changes look while keeping structure
- `0.85+` - Major transformation, uses image as loose reference

## Parameters

| Parameter | Type | Default | Range | Notes |
|-----------|------|---------|-------|-------|
| **input_image** | string | - | - | Path to source image |
| **positive_prompt** | string | - | - | Desired transformation |
| **negative_prompt** | string | "blurry..." | - | What to avoid |
| **denoise** | number | 0.75 | 0.0-1.0 | Transformation strength |
| **steps** | integer | 30 | 1-150 | Sampling steps |
| **cfg_scale** | number | 7.5 | 1.0-30.0 | Prompt adherence |
| **seed** | integer | -1 | - | Reproducibility |

## Examples

### Style Transfer
```json
{
  "positive_prompt": "oil painting, thick brushstrokes, impressionist",
  "denoise": 0.7,
  "steps": 35
}
```

### Sketch to Render
```json
{
  "positive_prompt": "detailed architectural render, photorealistic, 8k",
  "denoise": 0.85,
  "steps": 40
}
```

## License

MIT
