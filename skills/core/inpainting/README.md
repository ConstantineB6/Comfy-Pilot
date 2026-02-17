# Inpainting

Selectively regenerate masked regions of an image. Remove unwanted objects, fill backgrounds, or replace content with seamless blending.

## Quick Start

```
Tell Claude: "Load the inpainting skill and remove the person from this photo"
```

## How It Works

1. Load an image and define a mask (transparent = regenerate)
2. Right-click the LoadImage node in ComfyUI to open the mask editor
3. Paint over the area you want to change
4. The prompt describes what should fill the masked region
5. VAEEncodeForInpaint handles blending at mask edges

## Mask Preparation

- **In ComfyUI**: Right-click LoadImage node -> "Open in MaskEditor"
- **In GIMP/Photoshop**: Make the area transparent (alpha channel)
- **grow_mask_by**: Expands the mask by N pixels for smoother edges (default: 6)

## Parameters

| Parameter | Type | Default | Range | Notes |
|-----------|------|---------|-------|-------|
| **input_image** | string | - | - | Image with mask (alpha channel) |
| **positive_prompt** | string | - | - | What to fill in |
| **negative_prompt** | string | "blurry..." | - | What to avoid |
| **grow_mask_by** | integer | 6 | 0-64 | Edge blending pixels |
| **steps** | integer | 30 | 1-150 | Sampling steps |
| **cfg_scale** | number | 8.0 | 1.0-30.0 | Prompt adherence |

## Tips

- Use dedicated inpainting models for best edge blending
- Non-inpainting models also work but may show seams
- Higher CFG (8-10) helps match the surrounding context
- Grow mask by 6-8px for natural transitions

## License

MIT
