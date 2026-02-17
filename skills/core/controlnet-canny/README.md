# ControlNet Canny Edge

Generate images guided by the edge structure of a reference image. Canny edge detection extracts outlines, and ControlNet uses them to guide generation.

## Quick Start

```
Tell Claude: "Load the controlnet-canny skill and generate an anime version of this building photo"
```

## How It Works

1. Load a reference image
2. Canny node extracts edge map (black and white outlines)
3. ControlNet conditions the diffusion model to follow those edges
4. Generation produces new content that matches the structure

## Key Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| **reference_image** | string | - | Image to extract edges from |
| **positive_prompt** | string | - | What to generate |
| **controlnet_strength** | number | 1.0 | 0.5 = loose, 1.0 = strict |
| **canny_low** | integer | 100 | Lower = more edges |
| **canny_high** | integer | 200 | Higher = only strong edges |
| **steps** | integer | 30 | Sampling steps |

## Strength Guide

- **1.0** - Strict edge following (architecture, technical)
- **0.7-0.8** - Natural following with artistic freedom
- **0.4-0.6** - Loose inspiration from structure
- **< 0.3** - Minimal influence

## Edge Detection Tips

- **Low threshold (80-120)**: Catches fine details, noisy edges
- **High threshold (150-250)**: Clean major edges only
- Ratio of ~1:2 (low:high) generally works well

## Other ControlNet Types

This skill uses Canny edges. Other ControlNet types available:
- **Depth** - 3D structure from depth maps
- **Pose** - Human poses from skeleton detection
- **Scribble** - Freehand drawings as guides

## License

MIT
