# 4x Upscale (Real-ESRGAN)

Upscale images to 4x resolution using neural network upscaling models. Fast, deterministic, no diffusion sampling needed.

## Quick Start

```
Tell Claude: "Load the upscale skill and upscale my image to 4x"
```

## How It Works

1. Load your image
2. Load an upscale model (Real-ESRGAN, UltraSharp, etc.)
3. Run the image through the model
4. Output is 4x the input resolution

Unlike diffusion-based upscaling, this is a single forward pass - no iterative sampling, no prompts needed.

## Models

| Model | Best For |
|-------|----------|
| **RealESRGAN_x4plus.pth** | Photos, general purpose |
| **RealESRGAN_x4plus_anime_6B.pth** | Anime, illustrations |
| **4x-UltraSharp.pth** | Maximum sharpness |
| **4x_NMKD-Siax_200k.pth** | Balanced quality |

Place models in `ComfyUI/models/upscale_models/`. Find more at [OpenModelDB](https://openmodeldb.info).

## Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| **input_image** | string | - | Image to upscale |
| **upscale_model** | string | RealESRGAN_x4plus.pth | Which model to use |

## Performance

- 512x512 -> 2048x2048 in ~5 seconds
- Minimal VRAM (~2GB)
- Deterministic output (same input = same output)

## License

MIT
