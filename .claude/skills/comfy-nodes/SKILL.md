---
name: comfy-nodes
description: Use when the user wants to create a ComfyUI custom node, convert Python code to a node, make a node from a script, or needs help with ComfyUI node development, INPUT_TYPES, RETURN_TYPES, or node class structure.
version: 1.0.0
---

# ComfyUI Custom Node Development

This skill helps you create custom ComfyUI nodes from Python code.

## Quick Template

```python
class MyNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "value": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 1.0}),
            },
            "optional": {
                "mask": ("MASK",),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("output",)
    FUNCTION = "execute"
    CATEGORY = "Custom/MyNodes"

    def execute(self, image, value, mask=None):
        result = image * value
        return (result,)

NODE_CLASS_MAPPINGS = {"MyNode": MyNode}
NODE_DISPLAY_NAME_MAPPINGS = {"MyNode": "My Node"}
```

## Converting Python to Node

When you have Python code to wrap:

### Step 1: Identify inputs and outputs
```python
# Original function
def apply_blur(image, radius=5):
    from PIL import ImageFilter
    return image.filter(ImageFilter.GaussianBlur(radius))
```

### Step 2: Map types

| Python Type | ComfyUI Type | Conversion |
|-------------|--------------|------------|
| PIL Image | IMAGE | `torch.from_numpy(np.array(pil) / 255.0)` |
| numpy array | IMAGE | `torch.from_numpy(arr.astype(np.float32))` |
| cv2 BGR | IMAGE | `torch.from_numpy(cv2.cvtColor(img, cv2.COLOR_BGR2RGB) / 255.0)` |
| float 0-255 | IMAGE | Divide by 255.0 |
| Single image | Batch | `tensor.unsqueeze(0)` |

### Step 3: Handle batch dimension

ComfyUI images are `[B,H,W,C]` - always process all batch items:

```python
def execute(self, image, radius):
    batch_results = []
    for i in range(image.shape[0]):
        # Convert to PIL
        img_np = (image[i].cpu().numpy() * 255).astype(np.uint8)
        pil_img = Image.fromarray(img_np)

        # Your processing
        result = pil_img.filter(ImageFilter.GaussianBlur(radius))

        # Convert back
        result_np = np.array(result).astype(np.float32) / 255.0
        batch_results.append(torch.from_numpy(result_np))

    return (torch.stack(batch_results),)
```

## Common Input Types

| Type | Shape/Format | Widget Options |
|------|--------------|----------------|
| IMAGE | [B,H,W,C] float 0-1 | - |
| MASK | [H,W] or [B,H,W] float 0-1 | - |
| LATENT | {"samples": [B,C,H,W]} | - |
| MODEL | ModelPatcher | - |
| CLIP | CLIP encoder | - |
| VAE | VAE model | - |
| CONDITIONING | [(cond, pooled), ...] | - |
| INT | integer | default, min, max, step |
| FLOAT | float | default, min, max, step, display |
| STRING | str | default, multiline |
| BOOLEAN | bool | default |
| COMBO | str | List of options as type |

## Checklist

- [ ] `INPUT_TYPES` is a `@classmethod`
- [ ] Return value is a tuple: `return (result,)`
- [ ] Handle batch dimension `[B,H,W,C]`
- [ ] Add to `NODE_CLASS_MAPPINGS`
- [ ] Category uses `/` for submenus

## References

- [NODE_TEMPLATE.md](references/NODE_TEMPLATE.md) - Full template with V3 schema
- [OFFICIAL_DOCS.md](references/OFFICIAL_DOCS.md) - Official ComfyUI documentation
- [PURZ_EXAMPLES.md](references/PURZ_EXAMPLES.md) - Example nodes and workflows

## Finding Similar Nodes

Use the MCP tools to find existing nodes for reference:

```
comfy_search("blur")        → Find blur implementations
comfy_spec("GaussianBlur")  → See how inputs are defined
```
