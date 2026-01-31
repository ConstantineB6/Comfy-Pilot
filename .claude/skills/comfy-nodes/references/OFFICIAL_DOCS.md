# Official ComfyUI Documentation

Source: [docs.comfy.org](https://docs.comfy.org) | [Comfy-Org/docs](https://github.com/Comfy-Org/docs)

## Custom Node Architecture

ComfyUI is a **client-server system**:
- **Server (Python)**: Data processing, models, diffusion
- **Client (JavaScript)**: User interface
- **API Mode**: External workflow submission

### Four Node Categories

| Type | Description | API Compatible |
|------|-------------|----------------|
| Server-only | Python class with inputs/outputs | Yes |
| Client-only | UI modifications only | Yes |
| Independent Client+Server | Both features, auto-communication | Yes |
| Connected Client+Server | Direct UI-server interaction | **No** |

## Server-Side Node Structure

### Required Class Properties

```python
class MyNode:
    @classmethod
    def INPUT_TYPES(cls):
        """Define inputs - required, optional, hidden."""
        return {
            "required": {
                "image": ("IMAGE",),
                "value": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 10.0}),
            },
            "optional": {
                "mask": ("MASK",),
            },
            "hidden": {
                "unique_id": "UNIQUE_ID",
                "prompt": "PROMPT",
            }
        }

    RETURN_TYPES = ("IMAGE",)      # Output types tuple
    RETURN_NAMES = ("output",)     # Optional output labels
    FUNCTION = "execute"           # Method name to call
    CATEGORY = "image/processing"  # Menu location

    def execute(self, image, value, mask=None):
        result = image * value
        return (result,)  # Must return tuple
```

### Optional Class Properties

| Property | Type | Purpose |
|----------|------|---------|
| `OUTPUT_NODE` | bool | Mark as output node (always executes) |
| `IS_CHANGED` | classmethod | Override change detection |
| `VALIDATE_INPUTS` | classmethod | Input validation before execution |
| `INPUT_IS_LIST` | bool | Accept list inputs |
| `OUTPUT_IS_LIST` | tuple | Per-output list flags |
| `SEARCH_ALIASES` | list | Alternative search terms |

### IS_CHANGED Example

```python
@classmethod
def IS_CHANGED(cls, **kwargs):
    # Return NaN to always re-execute
    return float("NaN")
    # Or return hash for conditional re-execution
    return hash(tuple(kwargs.values()))
```

### VALIDATE_INPUTS Example

```python
@classmethod
def VALIDATE_INPUTS(cls, image, value):
    if value < 0:
        return "Value must be positive"
    return True
```

## Data Types and Colors

| Type | Color | Format |
|------|-------|--------|
| MODEL | Lavender | Diffusion model |
| CLIP | Yellow | Text encoder |
| VAE | Rose | Autoencoder |
| CONDITIONING | Orange | Encoded prompt |
| LATENT | Pink | {"samples": [B,C,H,W]} |
| IMAGE | Blue | [B,H,W,C] float 0-1 |
| MASK | Green | [H,W] or [B,H,W] |
| INT/FLOAT | Light green | Numbers |
| MESH | Bright green | 3D mesh |

**Connection rule**: Types must match exactly between output and input.

## Workflow Concepts

### What is a Workflow?
- Collection of nodes connected in a network (graph)
- Equivalent to scene graphs in 3D software
- Stored as human-readable JSON
- Automatically embedded in generated image metadata

### Workflow Storage

```json
{
  "nodes": [...],
  "links": [...],
  "groups": [...],
  "config": {...},
  "extra": {...},
  "version": 0.4
}
```

### API Format vs UI Format

- **UI Format**: Includes visual info (positions, colors)
- **API Format**: Minimal, for programmatic execution
- Export API format: Settings → Enable Dev Mode → Save (API Format)

### API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/ws` | WebSocket | Real-time status updates |
| `/prompt` | POST | Queue workflow execution |
| `/history/{id}` | GET | Retrieve results |
| `/view` | GET | Get generated images |
| `/upload/{type}` | POST | Upload images/masks |

## Widget Options Reference

### Numeric (INT, FLOAT)

```python
"value": ("FLOAT", {
    "default": 1.0,
    "min": 0.0,
    "max": 10.0,
    "step": 0.1,
    "round": 0.01,
    "display": "slider",  # or "number"
})
```

### String

```python
"text": ("STRING", {
    "default": "",
    "multiline": True,
    "dynamicPrompts": True,
})
```

### Combo (Dropdown)

```python
"option": (["choice1", "choice2", "choice3"],)
# Or dynamic from folder:
"model": (folder_paths.get_filename_list("checkpoints"),)
```

## Node Execution Modes

| Mode | Behavior |
|------|----------|
| Always | Default - executes normally |
| Never | Blocked - prevents execution |
| Bypass | Skipped but data passes through |

## Best Practices

1. **Always use classmethod** for `INPUT_TYPES` - enables dynamic options
2. **Return tuples** - even single outputs need `(result,)`
3. **Handle batches** - images are [B,H,W,C], process all batch items
4. **Validate inputs** - use `VALIDATE_INPUTS` for early error detection
5. **Category paths** - use `/` for submenus: `"image/filters/blur"`
6. **Lazy imports** - import heavy libraries inside methods, not at module level

## Templates & Examples

- [cookiecutter-comfy-extension](https://github.com/Comfy-Org/cookiecutter-comfy-extension)
- [ComfyUI-React-Extension-Template](https://github.com/Comfy-Org/ComfyUI-React-Extension-Template)
- [ComfyUI_frontend_vue_basic](https://github.com/jtydhr88/ComfyUI_frontend_vue_basic)

## V3 Schema (Coming)

New features being developed:
- Stable public API with backward compatibility
- Dependency isolation (separate Python processes)
- Dynamic input/output types
- Better type hints
- Versioned API guarantees
