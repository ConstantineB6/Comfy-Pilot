# Comfy Pilot

![Comfy Pilot](thumbnail.png)

MCP server + embedded terminal that lets Claude Code see and edit your ComfyUI workflows.

## Features

- **MCP Server** - Gives Claude Code direct access to view, edit, and run your ComfyUI workflows
- **Embedded Terminal** - Full xterm.js terminal running Claude Code right inside ComfyUI
- **Image Viewing** - Claude can see outputs from Preview Image and Save Image nodes
- **Graph Editing** - Create, delete, move, and connect nodes programmatically

## Requirements

- ComfyUI
- Python 3.8+

## Installation

### Option 1: ComfyUI Manager (Recommended)

1. Open ComfyUI
2. Click **Manager** → **Install Custom Nodes**
3. Search for "Comfy Pilot"
4. Click **Install**
5. Restart ComfyUI

### Option 2: Registry CLI

```bash
comfy node install comfy-pilot
```

### Option 3: Git Clone

```bash
cd /path/to/ComfyUI/custom_nodes
git clone https://github.com/ConstantineB6/comfy-pilot.git
```

Claude Code CLI will be installed automatically if not found.

## Usage

1. Restart ComfyUI after installation
2. The floating Claude Code terminal appears in the top-right corner
3. The MCP server is automatically configured for Claude Code
4. Ask Claude to help with your workflow:
   - "What nodes are in my current workflow?"
   - "Add a KSampler node connected to my checkpoint loader"
   - "Look at the preview image and tell me what you see"
   - "Run the workflow up to node 5"

## MCP Tools

The MCP server provides these tools to Claude Code:

| Tool | Description |
|------|-------------|
| `get_workflow` | Get the current workflow from the browser |
| `summarize_workflow` | Human-readable workflow summary |
| `get_node_types` | Search available node types with filtering |
| `get_node_info` | Get detailed info about a specific node type |
| `get_status` | Queue status, system stats, and execution history |
| `run` | Run workflow (optionally up to a specific node) or interrupt |
| `edit_graph` | Batch create, delete, move, connect, and configure nodes |
| `view_image` | View images from Preview Image / Save Image nodes |
| `search_custom_nodes` | Search ComfyUI Manager registry for custom nodes |
| `install_custom_node` | Install a custom node from the registry |
| `uninstall_custom_node` | Uninstall a custom node |
| `update_custom_node` | Update a custom node to latest version |
| `download_model` | Download models from Hugging Face, CivitAI, or direct URLs |

### Example: Creating Nodes

```
Create a KSampler and connect it to my checkpoint loader
```

Claude will use `edit_graph` to:
1. Create the KSampler node
2. Connect the MODEL output from CheckpointLoader to KSampler's model input
3. Position it appropriately in the graph

### Example: Viewing Images

```
Look at the preview image and describe what you see
```

Claude will use `view_image` to fetch and analyze the image output.

### Example: Downloading Models

```
Download the FLUX.1 schnell model for me
```

Claude will use `download_model` to download from Hugging Face to your ComfyUI models folder. Supports:
- Hugging Face (including gated models with token auth)
- CivitAI
- Direct download URLs

## Terminal Controls

- **Drag** title bar to move
- **Drag** bottom-right corner to resize
- **−** Minimize
- **×** Close
- **↻** Reconnect session

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Browser (ComfyUI)                                  │
│  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │  xterm.js       │  │  Workflow State          │  │
│  │  Terminal       │  │  (synced to backend)     │  │
│  └────────┬────────┘  └────────────┬─────────────┘  │
│           │ WebSocket              │ REST API       │
└───────────┼────────────────────────┼────────────────┘
            │                        │
            ▼                        ▼
┌─────────────────────────────────────────────────────┐
│  ComfyUI Server                                     │
│  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │  PTY Process    │  │  Plugin Endpoints        │  │
│  │  (claude CLI)   │  │  /claude-code/*          │  │
│  └─────────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────┘
            │                        │
            │                        ▼
            │           ┌──────────────────────────┐
            └──────────▶│  MCP Server              │
                        │  (stdio transport)       │
                        └──────────────────────────┘
```

## Files

- `__init__.py` - Plugin backend: WebSocket terminal, REST endpoints
- `js/claude-code.js` - Frontend: xterm.js terminal, workflow sync
- `mcp_server.py` - MCP server for Claude Code integration
- `CLAUDE.md` - Instructions for Claude when working with ComfyUI

## Troubleshooting

### "Command 'claude' not found"

Install Claude Code CLI:

**macOS / Linux / WSL:**
```bash
curl -fsSL https://claude.ai/install.sh | bash
```

**Windows (PowerShell):**
```powershell
irm https://claude.ai/install.ps1 | iex
```

**Windows (CMD):**
```cmd
curl -fsSL https://claude.ai/install.cmd -o install.cmd && install.cmd && del install.cmd
```

### MCP server not connecting

The plugin auto-configures MCP on startup. Check ComfyUI console for errors, or manually add to `~/.claude.json`:

```json
{
  "mcpServers": {
    "comfyui": {
      "command": "python3",
      "args": ["/path/to/comfy-pilot/mcp_server.py"]
    }
  }
}
```

### Terminal disconnected

Click the ↻ button to reconnect, or check ComfyUI console for errors.

## License

MIT
