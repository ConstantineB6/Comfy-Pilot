# ComfyUI Claude Code Plugin - Guidelines

## Working with ComfyUI Workflows

When creating or modifying ComfyUI workflows, follow these best practices:

### Node Titles

**Always assign descriptive custom titles to nodes** when creating them. This makes workflows much easier to understand when reading them back later.

Instead of generic type names like:
- `CLIPTextEncode` → Use `"Positive Prompt"` or `"Style Description"`
- `KSampler` → Use `"Initial Generation"` or `"Refine Pass"`
- `LoadImage` → Use `"Reference Photo"` or `"Input Image"`
- `VAEDecode` → Use `"Final Decode"` or `"Preview Decode"`
- `ImageScale` → Use `"Upscale 2x"` or `"Resize for ControlNet"`

Example:
```
comfyui_create_node(node_type="CLIPTextEncode", title="Positive Prompt", pos_x=200, pos_y=100)
comfyui_create_node(node_type="CLIPTextEncode", title="Negative Prompt", pos_x=200, pos_y=300)
```

### Node Layout

Position nodes logically from left to right following the data flow:
- Loaders on the left (pos_x ~100-300)
- Processing in the middle (pos_x ~400-700)
- Output/preview on the right (pos_x ~800+)
- Keep related nodes vertically aligned
- **Minimum padding**: Always leave at least 20px of padding between nodes (2 grid units, default grid = 10px)

**Node sizing**: When placing a node directly below another node, match the width of the node above it for visual consistency. Use `edit_graph` with `resize` action after creation if needed.

**Special case: Load 3D & Animation nodes**
These nodes expand after creation to show a 3D preview. The preview adds ~180px in height. When placing nodes below a Load 3D & Animation node, add at least 200px of extra vertical spacing to avoid overlap.

### Batch Operations with edit_graph

**Use `edit_graph` for all graph modifications** - it batches multiple operations in a single tool call:

```
edit_graph(operations=[
  // Create nodes (use 'ref' to reference in later operations)
  {action: "create", node_type: "KSampler", title: "Main Sampler", pos_x: 400, pos_y: 100, ref: "sampler"},
  {action: "create", node_type: "CLIPTextEncode", title: "Positive Prompt", pos_x: 200, pos_y: 100, ref: "pos"},
  {action: "create", node_type: "CLIPTextEncode", title: "Negative Prompt", pos_x: 200, pos_y: 300, ref: "neg"},

  // Set properties (can use 'ref' from create or actual node_id)
  {action: "set", node_id: "sampler", property: "steps", value: 30},
  {action: "set", node_id: "sampler", properties: {cfg: 7.5, seed: 12345}},

  // Connect nodes
  {action: "connect", from_node: "pos", from_slot: 0, to_node: "sampler", to_slot: 1},
  {action: "connect", from_node: "neg", from_slot: 0, to_node: "sampler", to_slot: 2},

  // Move/resize
  {action: "move", node_id: "5", x: 400, y: 100},
  {action: "move", node_id: "6", relative_to: "5", direction: "below", gap: 30},
  {action: "resize", node_id: "7", width: 300, height: 200},

  // Delete
  {action: "delete", node_id: "3"},
  {action: "delete", node_ids: ["4", "5"]}
])
```

**Running**: Use `run(action="queue")` to execute, `run(action="interrupt")` to stop.

**Status**: Use `get_status(include=["queue", "system", "history"])` for all status info.

### Searching for Nodes

**Always search minimal first** - don't request `inputs`/`outputs` on broad searches:

1. **First search without extra fields**:
   ```
   get_node_types(search=["camera", "sampler", "preview"])
   ```

2. **Review results** - identify promising nodes from display_name and category

3. **Get details only for specific nodes you'll use**:
   ```
   get_node_types(search="Stg_CameraInfo", fields=["inputs", "outputs"])
   ```

Requesting `inputs`/`outputs` on broad searches wastes tokens when you get many matches. Narrow down first, then get details for the 1-3 nodes you actually need.

### Connections

When connecting nodes:
- Use `get_node_info` to check available input/output slots
- Slot indices are 0-based
- Verify connections match types (e.g., MODEL to MODEL, CLIP to CLIP)

### Previewing Results

When the user wants to "see", "check", "test", or "debug" something:

1. **Add preview nodes**: Connect unconnected outputs to appropriate preview nodes:
   - Image outputs → Preview Image
   - Text/debug data → Search for available text preview nodes (see below)
   - 3D data → Preview 3D (if available)

2. **Auto-run when safe**: If the workflow only involves lightweight operations, run it automatically so the user can see results immediately:
   - Safe to auto-run: Debug nodes, camera calculations, text previews, metadata extraction, math operations
   - Ask first or inform user: KSampler, image generation, model loading, anything GPU-intensive

3. **Example**: If user asks "let me see what the camera info looks like", don't just add nodes - add a preview node, connect it, and run the workflow so they can actually see the output.

### Finding Preview/Display Nodes

Text preview nodes vary by ComfyUI installation. To find available ones:

1. **Search efficiently** - use minimal fields first:
   ```
   get_node_types(search=["preview", "show", "display"])
   ```

2. **Common text preview nodes** (availability varies):
   - `PreviewAny` - Shows any data type as text (Comfy Core, newer versions)
   - `ShowText|pysssss` - From pythongosssss custom nodes
   - Look for nodes with "preview", "show", "display", or "print" in the name

3. **If unsure**, search first then pick from results. Don't assume a node exists - verify with `get_node_types`.

4. **Request more details when needed** - use `fields` parameter:
   ```
   get_node_types(search="preview", fields=["input_types", "output_types"])
   ```
# Package Management

Prefer `uv` for package and environment management. Check if `uv` is available first:

```bash
which uv
```

If `uv` is available, use it:
- `uv pip install <package>`
- `uv venv`
- `uv sync`

If `uv` is NOT available, ask the user before proceeding:
> "I'd prefer to use `uv` for package management as it's faster and more reliable. Can I install it? (`curl -LsSf https://astral.sh/uv/install.sh | sh`). If you'd rather use pip or another tool, let me know."

Only fall back to pip/other tools if the user explicitly prefers them.

