# Purz ComfyUI Resources

Official examples from Purz (ComfyUI Tutor & Community Manager).

## Workflows Repository

**Source**: https://github.com/purzbeats/purz-comfyui-workflows

### AnimateDiff-Evolved

| Workflow | Description |
|----------|-------------|
| Vid2Vid 2024 | AnimateLCM + AnimateDiff v3 Gen2, IPA support, multi-ControlNet, upscaling |
| Masking - Subject Replacement | Selective subject replacement in animations |
| Masking - Background Replacement | Background swapping in animations |

### Stable Video Diffusion (SVD)

| Workflow | Description |
|----------|-------------|
| SVD Simple | Basic image-to-video with interpolation |
| SVD Clip Extension 8x | Advanced SVD with 8x extension, uses KJ Nodes Set/Get |

### LTX Video

Check the LTX2 folder for latest video generation workflows.

## Custom Nodes

**Source**: https://github.com/purzbeats/ComfyUI-Purz

### Interactive

| Node | Description |
|------|-------------|
| Interactive Filter | Real-time layer-based filter, 42 effects, WebGL preview, video batch |

### Image Processing - Color

| Node | Description |
|------|-------------|
| Image to Black & White | Grayscale conversion with luminance weighting |
| Color Adjust | Brightness, contrast, saturation controls |

### Image Processing - Transform

| Node | Description |
|------|-------------|
| Rotate Image | Rotation with custom background color |
| Flip/Mirror Image | Horizontal, vertical, or both flip |

### Image Processing - Effects

| Node | Description |
|------|-------------|
| Blur Image | Gaussian, box, motion blur |
| Pixelate Effect | Retro pixel art style |
| Edge Detection | Sobel, Canny, Laplacian algorithms |

### Pattern Generation - Static

| Node | Description |
|------|-------------|
| Checkerboard Pattern | Customizable checkerboard |
| Stripes Pattern | Directional stripes |
| Polka Dot Pattern | Circular dot arrangements |
| Grid Pattern | Solid, dashed, dotted overlays |
| Gradient Pattern | Linear, radial, diagonal gradients |
| Simple Noise Pattern | Random, smooth, cloudy noise |

### Pattern Generation - Animated

| Node | Description |
|------|-------------|
| Animated Checkerboard | Wave modulation patterns |
| Animated Stripes | Moving stripe sequences |
| Animated Polka Dots | Pulsating dot animations |
| Animated Noise | Temporally coherent evolving noise |

## Usage Tips

1. **Vid2Vid workflow**: Start here for video-to-video with AnimateDiff
2. **Pattern nodes**: Great for masks, overlays, and creative effects
3. **Interactive Filter**: Test effects in real-time before batch processing

## Installation

```bash
# Workflows - download JSON files directly
git clone https://github.com/purzbeats/purz-comfyui-workflows

# Nodes - install via ComfyUI-Manager or:
cd ComfyUI/custom_nodes
git clone https://github.com/purzbeats/ComfyUI-Purz
```
