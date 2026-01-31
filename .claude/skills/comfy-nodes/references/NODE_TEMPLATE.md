# ComfyUI Custom Node Development Guide

## File Structure

```
custom_nodes/
└── my_node_pack/
    ├── __init__.py      # NODE_CLASS_MAPPINGS export
    ├── nodes.py         # Node classes
    ├── utils.py         # Shared utilities (optional)
    └── requirements.txt # Dependencies (optional)
```

**__init__.py** (required):
```python
from .nodes import MyNode, AnotherNode

NODE_CLASS_MAPPINGS = {
    "MyNode": MyNode,
    "AnotherNode": AnotherNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "MyNode": "My Node",
    "AnotherNode": "Another Node",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
```

## Basic Node Template

```python
class MyNode:
    """Node description shown in UI."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "strength": ("FLOAT", {
                    "default": 1.0,
                    "min": 0.0,
                    "max": 2.0,
                    "step": 0.01,
                    "display": "slider"  # or "number"
                }),
            },
            "optional": {
                "mask": ("MASK",),
            },
            "hidden": {
                "unique_id": "UNIQUE_ID",  # Node's unique ID
                "prompt": "PROMPT",         # Full workflow prompt
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("output",)
    FUNCTION = "execute"
    CATEGORY = "image/transform"

    # Optional class attributes
    OUTPUT_NODE = False          # True if terminal node (SaveImage)
    OUTPUT_IS_LIST = (False,)    # Per-output list flag
    INPUT_IS_LIST = False        # Accept list inputs

    def execute(self, image, strength, mask=None, unique_id=None, prompt=None):
        # image shape: [B, H, W, C] torch.Tensor, values 0-1
        result = image * strength
        return (result,)
```

## Data Types Reference

| Type | Python Type | Shape/Format | UI Color |
|------|-------------|--------------|----------|
| IMAGE | torch.Tensor | [B, H, W, C] float32 0-1 | Blue |
| MASK | torch.Tensor | [H, W] or [B, H, W] float32 0-1 | Green |
| LATENT | dict | {"samples": [B, C, H, W]} | Pink |
| MODEL | ModelPatcher | Diffusion model wrapper | Lavender |
| CLIP | CLIP | Text encoder | Yellow |
| VAE | VAE | Autoencoder | Rose |
| CONDITIONING | list | [(cond, {"pooled": ...}), ...] | Orange |
| INT | int | - | Light green |
| FLOAT | float | - | Light green |
| STRING | str | - | Cyan |
| BOOLEAN | bool | - | Pink |
| COMBO | str | One of list values | - |

## Widget Options

**Numeric types (INT, FLOAT):**
```python
"value": ("FLOAT", {
    "default": 1.0,
    "min": 0.0,
    "max": 10.0,
    "step": 0.1,
    "round": 0.01,        # Round to precision
    "display": "slider",  # "slider" or "number"
})
```

**String:**
```python
"text": ("STRING", {
    "default": "",
    "multiline": True,    # Text area vs single line
    "dynamicPrompts": True,  # Enable wildcards
})
```

**Combo (dropdown):**
```python
"sampler": (["euler", "euler_ancestral", "dpmpp_2m", "dpmpp_sde"],),
# Or dynamic:
"sampler": (comfy.samplers.KSampler.SAMPLERS,),
```

**File inputs:**
```python
"image": ("IMAGE",),  # Drag & drop or select
# For specific folder:
@classmethod
def INPUT_TYPES(cls):
    return {"required": {
        "ckpt_name": (folder_paths.get_filename_list("checkpoints"),),
    }}
```

## Example Nodes

### Image Processor
```python
import torch

class ImageBrightnessContrast:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "brightness": ("FLOAT", {"default": 0.0, "min": -1.0, "max": 1.0, "step": 0.01}),
                "contrast": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 3.0, "step": 0.01}),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "apply"
    CATEGORY = "image/adjustments"

    def apply(self, image, brightness, contrast):
        # image: [B, H, W, C]
        result = (image - 0.5) * contrast + 0.5 + brightness
        result = torch.clamp(result, 0, 1)
        return (result,)
```

### Model Modifier
```python
class LoRAStrengthModifier:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "model": ("MODEL",),
                "clip": ("CLIP",),
                "lora_name": (folder_paths.get_filename_list("loras"),),
                "strength_model": ("FLOAT", {"default": 1.0, "min": -10.0, "max": 10.0, "step": 0.01}),
                "strength_clip": ("FLOAT", {"default": 1.0, "min": -10.0, "max": 10.0, "step": 0.01}),
            }
        }

    RETURN_TYPES = ("MODEL", "CLIP")
    FUNCTION = "load"
    CATEGORY = "loaders"

    def load(self, model, clip, lora_name, strength_model, strength_clip):
        lora_path = folder_paths.get_full_path("loras", lora_name)
        model_lora, clip_lora = comfy.sd.load_lora_for_models(
            model, clip, comfy.utils.load_torch_file(lora_path),
            strength_model, strength_clip
        )
        return (model_lora, clip_lora)
```

### Conditioning Node
```python
class ConditioningCombineWeighted:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "conditioning_1": ("CONDITIONING",),
                "conditioning_2": ("CONDITIONING",),
                "weight": ("FLOAT", {"default": 0.5, "min": 0.0, "max": 1.0}),
            }
        }

    RETURN_TYPES = ("CONDITIONING",)
    FUNCTION = "combine"
    CATEGORY = "conditioning"

    def combine(self, conditioning_1, conditioning_2, weight):
        # Interpolate between conditionings
        out = []
        for c1, c2 in zip(conditioning_1, conditioning_2):
            cond = c1[0] * (1 - weight) + c2[0] * weight
            pooled = {}
            if "pooled_output" in c1[1] and "pooled_output" in c2[1]:
                pooled["pooled_output"] = c1[1]["pooled_output"] * (1-weight) + c2[1]["pooled_output"] * weight
            out.append((cond, pooled))
        return (out,)
```

### Output Node (Terminal)
```python
class SaveImageCustom:
    def __init__(self):
        self.output_dir = folder_paths.get_output_directory()

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "filename_prefix": ("STRING", {"default": "ComfyUI"}),
            },
            "hidden": {"prompt": "PROMPT", "extra_pnginfo": "EXTRA_PNGINFO"},
        }

    RETURN_TYPES = ()  # No outputs
    OUTPUT_NODE = True  # Mark as terminal
    FUNCTION = "save"
    CATEGORY = "image"

    def save(self, images, filename_prefix, prompt=None, extra_pnginfo=None):
        # Save logic here
        results = []
        for image in images:
            # ... save image ...
            results.append({"filename": filename, "subfolder": "", "type": "output"})
        return {"ui": {"images": results}}
```

## V3 Schema (New Features)

ComfyUI V3 adds optional enhancements:

```python
class MyV3Node:
    # V3: Dynamic input/output types
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mode": (["image", "latent"],),
            },
            "optional": {
                "image": ("IMAGE",),
                "latent": ("LATENT",),
            }
        }

    # V3: Dynamic return types based on input
    RETURN_TYPES = comfy.utils.ComfyNodeOutputTypes  # Dynamic

    # V3: Validate inputs before execution
    @classmethod
    def VALIDATE_INPUTS(cls, mode, image=None, latent=None):
        if mode == "image" and image is None:
            return "Image required when mode is 'image'"
        if mode == "latent" and latent is None:
            return "Latent required when mode is 'latent'"
        return True

    # V3: Check if re-execution needed
    @classmethod
    def IS_CHANGED(cls, **kwargs):
        # Return value that changes when node needs re-run
        return float("nan")  # Always re-run
        # Or: return hash(tuple(kwargs.values()))
```

## Common Imports

```python
import torch
import numpy as np
from PIL import Image
import folder_paths  # ComfyUI paths helper
import comfy.utils
import comfy.sd
import comfy.samplers
import comfy.model_management
```

## Tensor Conversions

```python
# PIL to tensor
def pil_to_tensor(pil_image):
    return torch.from_numpy(np.array(pil_image).astype(np.float32) / 255.0).unsqueeze(0)

# Tensor to PIL
def tensor_to_pil(tensor):
    # tensor: [B, H, W, C] or [H, W, C]
    if tensor.dim() == 4:
        tensor = tensor[0]
    return Image.fromarray((tensor.cpu().numpy() * 255).astype(np.uint8))

# Mask to tensor
def mask_to_tensor(mask_pil):
    return torch.from_numpy(np.array(mask_pil).astype(np.float32) / 255.0)
```

## Tips

1. **Always return tuples**: Even single outputs need `return (result,)`
2. **Batch dimension**: Images are [B,H,W,C], handle batches properly
3. **Memory management**: Use `comfy.model_management` for GPU memory
4. **Lazy imports**: Import heavy libraries inside methods, not at module level
5. **Error messages**: Raise descriptive errors for user feedback
6. **Test with batches**: Ensure nodes work with batch_size > 1
