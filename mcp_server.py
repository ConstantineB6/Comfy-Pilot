#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCP Server for ComfyUI - Exposes workflow tools to Claude Code.

This server connects to ComfyUI's API and provides tools to:
- View the current workflow
- List all nodes
- Get node details
- And more...

Usage:
    python mcp_server.py

Configure in Claude Code's settings (~/.claude/settings.json):
{
    "mcpServers": {
        "comfyui": {
            "command": "python",
            "args": ["/path/to/mcp_server.py"]
        }
    }
}
"""

import json
import socket
import sys
import urllib.request
import urllib.error
import urllib.parse
import base64
from typing import Any

# ComfyUI API endpoint
def get_comfyui_url() -> str:
    """Get the ComfyUI URL - try common ports."""
    import os

    # Try to read from the URL file written by the plugin
    script_dir = os.path.dirname(os.path.abspath(__file__))
    url_file = os.path.join(script_dir, ".comfyui_url")

    if os.path.exists(url_file):
        try:
            with open(url_file, "r") as f:
                url = f.read().strip()
                if url:
                    # Test if this URL works
                    try:
                        req = urllib.request.Request(f"{url}/system_stats", method="GET")
                        with urllib.request.urlopen(req, timeout=2):
                            return url
                    except Exception:
                        pass
        except Exception:
            pass

    # Try common ports
    for port in [8000, 8188, 8189]:
        url = f"http://127.0.0.1:{port}"
        try:
            req = urllib.request.Request(f"{url}/system_stats", method="GET")
            with urllib.request.urlopen(req, timeout=2):
                return url
        except Exception:
            continue

    return "http://127.0.0.1:8000"  # Default for desktop version

COMFYUI_URL = None  # Will be set on first request

# Cache for object_info (node types) - this rarely changes
_object_info_cache = None
_object_info_cache_time = 0
CACHE_TTL = 300  # 5 minutes


def get_object_info_cached() -> dict:
    """Get object_info with caching to avoid slow repeated requests."""
    global _object_info_cache, _object_info_cache_time
    import time

    current_time = time.time()

    # Return cached if still valid
    if _object_info_cache is not None and (current_time - _object_info_cache_time) < CACHE_TTL:
        return _object_info_cache

    # Fetch fresh data
    result = make_request("/object_info")

    if "error" not in result:
        _object_info_cache = result
        _object_info_cache_time = current_time

    return result


def make_request(endpoint: str, method: str = "GET", data: dict = None, timeout: int = None) -> dict:
    """Make a request to ComfyUI's API."""
    global COMFYUI_URL
    if COMFYUI_URL is None:
        COMFYUI_URL = get_comfyui_url()

    url = f"{COMFYUI_URL}{endpoint}"

    # Use longer timeout for /object_info since it can be large
    if timeout is None:
        timeout = 30 if endpoint == "/object_info" else 10

    try:
        if data:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method=method
            )
        else:
            req = urllib.request.Request(url, method=method)

        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as e:
        return {"error": f"Failed to connect to ComfyUI: {e}"}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP error from ComfyUI: {e.code} {e.reason}"}
    except socket.timeout:
        return {"error": f"Request to ComfyUI timed out after {timeout}s"}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON response from ComfyUI"}
    except Exception as e:
        return {"error": f"Unexpected error: {type(e).__name__}: {e}"}


def get_workflow() -> dict:
    """Get the current workflow from ComfyUI."""
    # First try to get the live workflow from our plugin endpoint
    live_workflow = make_request("/claude-code/workflow")

    if live_workflow and live_workflow.get("workflow"):
        return {
            "source": "live",
            "workflow": live_workflow.get("workflow"),
            "workflow_api": live_workflow.get("workflow_api"),
            "timestamp": live_workflow.get("timestamp")
        }

    # Fallback to history if live workflow not available
    history = make_request("/history")

    if "error" in history:
        return history

    if not history:
        return {"message": "No workflow found. Make sure ComfyUI is open in a browser with the Claude Code plugin loaded."}

    # Get the most recent prompt
    latest_id = list(history.keys())[-1] if history else None
    if latest_id:
        return {
            "source": "history",
            "prompt_id": latest_id,
            "workflow": history[latest_id].get("prompt", {}),
            "outputs": history[latest_id].get("outputs", {})
        }

    return {"message": "No workflow found"}


def get_node_types(search = None, category: str = None, fields: list = None) -> str:
    """Get available node types in ComfyUI, optionally filtered.

    Returns compact TOON-like format.

    Args:
        search: Search term(s) - string or list of strings
        category: Category name to filter by
        fields: Optional list of fields to include. By default only returns minimal info.
                Available fields: "inputs", "outputs", "description", "input_types", "output_types"
    """
    all_nodes = get_object_info_cached()

    if "error" in all_nodes:
        return f"error: {all_nodes.get('error')}"

    fields = fields or []

    # Helper to format a single node in TOON
    def format_node(node_name: str, node_info: dict) -> list:
        """Return lines for a single node."""
        lines = []
        display_name = node_info.get("display_name") or node_name
        cat = node_info.get("category", "uncategorized")

        # Escape commas
        display_name = display_name.replace(",", ";")
        cat = cat.replace(",", ";")

        header = f"  {node_name},{display_name},{cat}"

        if "description" in fields:
            desc = (node_info.get("description") or "").replace("\n", " ").replace(",", ";")[:100]
            header += f",{desc}"

        lines.append(header)

        # Input types (compact)
        if "input_types" in fields or "inputs" in fields:
            input_info = node_info.get("input", {})
            inputs = []
            for group in ["required", "optional"]:
                if group in input_info:
                    req_marker = "*" if group == "required" else ""
                    for inp_name, inp_def in input_info[group].items():
                        if isinstance(inp_def, list) and len(inp_def) > 0:
                            inp_type = inp_def[0] if isinstance(inp_def[0], str) else type(inp_def[0]).__name__
                            inputs.append(f"{inp_name}{req_marker}:{inp_type}")
            if inputs:
                lines.append(f"    in: {','.join(inputs)}")

        # Output types (compact)
        if "output_types" in fields or "outputs" in fields:
            outputs = node_info.get("output", [])
            output_names = node_info.get("output_name", outputs)
            if outputs:
                out_parts = []
                for i, out_type in enumerate(outputs):
                    out_name = output_names[i] if i < len(output_names) else out_type
                    out_parts.append(f"{out_name}:{out_type}")
                lines.append(f"    out: {','.join(out_parts)}")

        return lines

    # If no filters, return category summary
    if not search and not category:
        categories = {}
        for node_name, node_info in all_nodes.items():
            cat = node_info.get("category", "uncategorized")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(node_name)

        lines = [f"total: {len(all_nodes)} nodes"]
        lines.append(f"categories[{len(categories)}]{{name,count}}:")
        for cat in sorted(categories.keys()):
            lines.append(f"  {cat},{len(categories[cat])}")
        lines.append("hint: use 'search' or 'category' parameter to filter")
        return "\n".join(lines)

    # Helper function to search for a single term
    def search_nodes(term: str) -> list:
        term_lower = term.lower()
        matches = []
        for node_name, node_info in all_nodes.items():
            display_name = node_info.get("display_name") or ""
            description = node_info.get("description") or ""
            if (term_lower in node_name.lower() or
                term_lower in display_name.lower() or
                term_lower in description.lower()):
                matches.append((node_name, node_info))
        return matches

    # Filter by search term(s)
    if search:
        search_terms = search if isinstance(search, list) else [search]
        lines = []

        for term in search_terms:
            matches = search_nodes(term)
            lines.append(f"search \"{term}\": {len(matches)} matches")
            if matches:
                lines.append(f"nodes[{len(matches)}]{{name,display,category}}:")
                for node_name, node_info in sorted(matches, key=lambda x: x[0]):
                    lines.extend(format_node(node_name, node_info))

        return "\n".join(lines)

    # Filter by category
    if category:
        category_lower = category.lower()
        matches = []
        for node_name, node_info in all_nodes.items():
            cat = node_info.get("category", "")
            if category_lower in cat.lower():
                matches.append((node_name, node_info))

        lines = [f"category \"{category}\": {len(matches)} matches"]
        if matches:
            lines.append(f"nodes[{len(matches)}]{{name,display,category}}:")
            for node_name, node_info in sorted(matches, key=lambda x: x[0]):
                lines.extend(format_node(node_name, node_info))

        return "\n".join(lines)


def get_queue() -> dict:
    """Get the current queue status."""
    return make_request("/queue")


def get_system_stats() -> dict:
    """Get system stats from ComfyUI."""
    return make_request("/system_stats")


def interrupt_generation() -> dict:
    """Interrupt the current generation."""
    return make_request("/interrupt", method="POST")


def get_history(prompt_id: str = None) -> dict:
    """Get prompt history, optionally for a specific prompt ID."""
    if prompt_id:
        return make_request(f"/history/{prompt_id}")
    return make_request("/history")


def clear_history() -> dict:
    """Clear the prompt history."""
    return make_request("/history", method="POST", data={"clear": True})


# ============== CONSOLIDATED TOOLS ==============

def get_status(include: list = None, detail: str = "summary", history_limit: int = 5, history_offset: int = 0) -> str:
    """Get status information from ComfyUI in compact TOON-like format.

    Args:
        include: List of what to include: "queue", "system", "history".
                 Default is ["queue", "system"].
        detail: Level of detail - "summary" (default) or "full".
                Summary returns counts and IDs only. Full returns more detail but still paginated.
        history_limit: Max number of history entries to return (default 5, max 20).
        history_offset: Skip this many entries from the most recent (for pagination).
    """
    if include is None:
        include = ["queue", "system"]

    # Clamp history_limit
    history_limit = max(1, min(history_limit, 20))

    lines = []

    if "queue" in include:
        raw_queue = make_request("/queue")
        if "error" in raw_queue:
            lines.append(f"queue: error - {raw_queue.get('error')}")
        else:
            running = raw_queue.get("queue_running", [])
            pending = raw_queue.get("queue_pending", [])
            lines.append(f"queue: {len(running)} running, {len(pending)} pending")
            if running:
                prompt_ids = [item[1][:8] if len(item) > 1 and item[1] else "?" for item in running]
                lines.append(f"  running: {','.join(prompt_ids)}")
            if pending:
                prompt_ids = [item[1][:8] if len(item) > 1 and item[1] else "?" for item in pending]
                lines.append(f"  pending: {','.join(prompt_ids)}")

    if "system" in include:
        sys_stats = make_request("/system_stats")
        if "error" in sys_stats:
            lines.append(f"system: error - {sys_stats.get('error')}")
        else:
            # Format system stats compactly
            system = sys_stats.get("system", {})
            devices = sys_stats.get("devices", [])

            os_info = system.get("os", "unknown")
            py_ver = system.get("python_version", "?")
            lines.append(f"system: {os_info}, python {py_ver}")

            for i, dev in enumerate(devices):
                name = dev.get("name", "GPU")
                vram_total = dev.get("vram_total", 0)
                vram_free = dev.get("vram_free", 0)
                vram_used = vram_total - vram_free
                # Convert to GB
                if vram_total > 0:
                    vram_total_gb = vram_total / (1024**3)
                    vram_used_gb = vram_used / (1024**3)
                    pct = (vram_used / vram_total) * 100
                    lines.append(f"  gpu{i}: {name[:30]}, {vram_used_gb:.1f}/{vram_total_gb:.1f}GB ({pct:.0f}% used)")

    if "history" in include:
        raw_history = make_request("/history")
        if "error" in raw_history:
            lines.append(f"history: error - {raw_history.get('error')}")
        else:
            # Sort by timestamp (most recent first)
            history_items = []
            for prompt_id, data in raw_history.items():
                if not isinstance(data, dict):
                    continue
                status = data.get("status", {})
                timestamp = status.get("messages", [[0, {}]])[0][0] if status.get("messages") else 0
                history_items.append((prompt_id, data, timestamp))

            history_items.sort(key=lambda x: x[2], reverse=True)
            total_count = len(history_items)
            history_items = history_items[history_offset:history_offset + history_limit]

            lines.append(f"history: {total_count} total (showing {history_offset+1}-{history_offset+len(history_items)})")

            if detail == "full":
                lines.append("entries{id,status,time,outputs}:")
                for prompt_id, data, _ in history_items:
                    outputs = data.get("outputs", {})
                    status = data.get("status", {})
                    status_str = status.get("status_str", "?")
                    exec_time = _get_execution_time(status)
                    output_nodes = ",".join(outputs.keys()) if outputs else "none"
                    lines.append(f"  {prompt_id[:8]},{status_str},{exec_time},{output_nodes}")
            else:
                lines.append("entries{id,status,completed}:")
                for prompt_id, data, _ in history_items:
                    status = data.get("status", {})
                    status_str = status.get("status_str", "?")
                    completed = "yes" if status.get("completed", False) else "no"
                    lines.append(f"  {prompt_id[:8]},{status_str},{completed}")

    return "\n".join(lines)


def _get_execution_time(status: dict) -> str:
    """Extract execution time from status messages."""
    messages = status.get("messages", [])
    start_time = None
    end_time = None

    for msg in messages:
        if len(msg) >= 2:
            msg_type = msg[0]
            msg_data = msg[1] if isinstance(msg[1], dict) else {}
            if msg_type == "execution_start":
                start_time = msg_data.get("timestamp")
            elif msg_type == "execution_success" or msg_type == "execution_error":
                end_time = msg_data.get("timestamp")

    if start_time and end_time:
        try:
            duration = float(end_time) - float(start_time)
            return f"{duration:.2f}s"
        except (ValueError, TypeError):
            pass

    return "unknown"


def run(action: str = "queue", node_ids = None) -> dict:
    """Run or control workflow execution.

    Args:
        action: "queue" to run workflow, "interrupt" to stop current generation
        node_ids: Optional node ID(s) to run (validates they exist). If not provided, runs whole workflow.
    """
    if action == "interrupt":
        return make_request("/interrupt", method="POST")

    if action == "queue":
        # Fetch workflow API on demand
        workflow_api_result = send_graph_command("get_workflow_api", {})

        if "error" in workflow_api_result:
            return workflow_api_result

        workflow_api = workflow_api_result.get("workflow_api")
        if not workflow_api:
            return {"error": "No workflow available. Make sure ComfyUI is open in browser."}

        # Validate node_ids if provided
        if node_ids:
            if isinstance(node_ids, str):
                node_ids = [node_ids]
            elif not isinstance(node_ids, list):
                node_ids = [str(node_ids)]
            else:
                node_ids = [str(n) for n in node_ids]

            prompt = workflow_api.get("output", workflow_api)
            invalid = [n for n in node_ids if str(n) not in prompt]
            if invalid:
                return {"error": f"Node(s) not found in workflow: {invalid}"}

        # Queue via frontend
        result = send_graph_command("queue_prompt", {})
        if "error" in result:
            return result

        return {"status": "queued", "prompt_id": result.get("prompt_id")}

    return {"error": f"Unknown action: {action}. Use 'queue' or 'interrupt'."}


def edit_graph(operations) -> str:
    """Edit the workflow graph with one or more operations.

    Args:
        operations: Single operation dict or list of operations.
                    Each operation has an "action" field and action-specific params.

    Actions:
        - create: {action: "create", node_type, pos_x, pos_y, title}
        - delete: {action: "delete", node_id} or {action: "delete", node_ids: [...]}
        - move: {action: "move", node_id, x, y} or {action: "move", node_id, relative_to, direction, gap}
        - resize: {action: "resize", node_id, width, height}
        - set: {action: "set", node_id, property, value} or {action: "set", node_id, properties: {k: v, ...}}
        - connect: {action: "connect", from_node, from_slot, to_node, to_slot}
        - disconnect: {action: "disconnect", from_node, from_slot, to_node, to_slot}

    Returns node_id for create operations so subsequent operations can reference it.
    """
    if isinstance(operations, dict):
        operations = [operations]

    # Cache node types for validation
    all_nodes = get_object_info_cached()
    if "error" in all_nodes:
        return f"error: {all_nodes.get('error', 'Failed to get node types')}"

    results = []
    created_nodes = {}  # Map temp refs to real node IDs
    viewport_offset = 0  # Horizontal offset for place_in_view nodes

    for i, op in enumerate(operations):
        action = op.get("action", "")
        result = {"action": action, "index": i}

        try:
            if action == "create":
                node_type = op.get("node_type", "")
                if not node_type:
                    result["error"] = "node_type is required"
                elif node_type not in all_nodes:
                    result["error"] = f"Unknown node type: {node_type}"
                else:
                    place_in_view = op.get("place_in_view", False)
                    r = send_graph_command("create_node", {
                        "type": node_type,
                        "pos_x": op.get("pos_x", 100),
                        "pos_y": op.get("pos_y", 100),
                        "title": op.get("title"),
                        "place_in_view": place_in_view,
                        "viewport_offset": viewport_offset if place_in_view else 0
                    })
                    result.update(r)
                    # Store created node ID for reference
                    if "node_id" in r:
                        ref = op.get("ref")
                        if ref:
                            created_nodes[ref] = r["node_id"]
                        # Increment viewport offset for next place_in_view node
                        if place_in_view:
                            # Use node size + gap for offset (default 300 + 30 if size unknown)
                            node_width = r.get("size", [300, 100])[0] if isinstance(r.get("size"), list) else 300
                            viewport_offset += node_width + 30

            elif action == "delete":
                node_ids = op.get("node_ids") or [op.get("node_id")]
                for node_id in node_ids:
                    if node_id:
                        r = send_graph_command("delete_node", {"node_id": str(node_id)})
                        result.update(r)

            elif action == "move":
                node_id = op.get("node_id", "")
                if not node_id:
                    result["error"] = "node_id is required"
                else:
                    # Resolve reference if needed
                    if node_id in created_nodes:
                        node_id = created_nodes[node_id]
                    relative_to = op.get("relative_to")
                    if relative_to and relative_to in created_nodes:
                        relative_to = created_nodes[relative_to]

                    r = send_graph_command("move_node", {
                        "node_id": str(node_id),
                        "x": op.get("x"),
                        "y": op.get("y"),
                        "relative_to": str(relative_to) if relative_to else None,
                        "direction": op.get("direction"),
                        "gap": op.get("gap", 30),
                        "width": op.get("width"),
                        "height": op.get("height")
                    })
                    result.update(r)

            elif action == "resize":
                node_id = op.get("node_id", "")
                if not node_id:
                    result["error"] = "node_id is required"
                else:
                    if node_id in created_nodes:
                        node_id = created_nodes[node_id]
                    r = send_graph_command("move_node", {
                        "node_id": str(node_id),
                        "width": op.get("width"),
                        "height": op.get("height")
                    })
                    result.update(r)

            elif action == "set":
                node_id = op.get("node_id", "")
                if not node_id:
                    result["error"] = "node_id is required"
                else:
                    if node_id in created_nodes:
                        node_id = created_nodes[node_id]

                    # Support both single property and multiple properties
                    properties = op.get("properties", {})
                    if "property" in op:
                        properties[op["property"]] = op.get("value")

                    for prop_name, value in properties.items():
                        r = send_graph_command("set_node_property", {
                            "node_id": str(node_id),
                            "property_name": prop_name,
                            "value": value
                        })
                        result.update(r)

            elif action == "connect":
                from_node = op.get("from_node", "")
                to_node = op.get("to_node", "")
                if not from_node or not to_node:
                    result["error"] = "from_node and to_node are required"
                else:
                    if from_node in created_nodes:
                        from_node = created_nodes[from_node]
                    if to_node in created_nodes:
                        to_node = created_nodes[to_node]

                    r = send_graph_command("connect_nodes", {
                        "from_node_id": str(from_node),
                        "from_slot": op.get("from_slot", 0),
                        "to_node_id": str(to_node),
                        "to_slot": op.get("to_slot", 0)
                    })
                    result.update(r)

            elif action == "disconnect":
                from_node = op.get("from_node", "")
                to_node = op.get("to_node", "")
                if not from_node or not to_node:
                    result["error"] = "from_node and to_node are required"
                else:
                    if from_node in created_nodes:
                        from_node = created_nodes[from_node]
                    if to_node in created_nodes:
                        to_node = created_nodes[to_node]

                    r = send_graph_command("disconnect_nodes", {
                        "from_node_id": str(from_node),
                        "from_slot": op.get("from_slot", 0),
                        "to_node_id": str(to_node),
                        "to_slot": op.get("to_slot", 0)
                    })
                    result.update(r)

            else:
                result["error"] = f"Unknown action: {action}"

        except Exception as e:
            result["error"] = str(e)

        results.append(result)

    # Build TOON response
    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]

    lines = []

    # Status line
    if failed:
        lines.append(f"failed: {len(failed)}/{len(results)}")
    else:
        lines.append(f"ok: {len(results)}/{len(results)}")

    # Show created node IDs
    created_ids = [str(r.get("node_id")) for r in results if r.get("action") == "create" and "node_id" in r]
    if created_ids:
        lines.append(f"created: {','.join(created_ids)}")

    # Show errors only if there are failures
    if failed:
        lines.append("errors:")
        for r in failed:
            idx = r.get("index", "?")
            action = r.get("action", "?")
            error = r.get("error", "unknown error")
            lines.append(f"  [{idx}] {action}: {error}")

    # Get affected node IDs (created, moved, resized)
    affected_ids = set()
    for r in results:
        if "error" not in r:
            if "node_id" in r:
                affected_ids.add(int(r["node_id"]))
            # For move/resize, the node_id is in the original op

    # Also track from operations directly
    for i, op in enumerate(operations if isinstance(operations, list) else [operations]):
        action = op.get("action", "")
        if action in ("move", "resize", "create"):
            node_id = op.get("node_id")
            if node_id:
                # Resolve refs
                if node_id in created_nodes:
                    node_id = created_nodes[node_id]
                try:
                    affected_ids.add(int(node_id))
                except (ValueError, TypeError):
                    pass

    # Get current workflow state to find affected nodes and collisions
    workflow_data = get_workflow()
    if "error" not in workflow_data and "workflow" in workflow_data:
        workflow = workflow_data.get("workflow", {})

        if "nodes" in workflow:
            all_nodes = []
            affected_nodes = []

            for node in workflow.get("nodes", []):
                pos = node.get("pos", [0, 0])
                size = node.get("size", [200, 100])

                # Handle both array and object formats
                if isinstance(pos, dict):
                    x, y = pos.get("0", 0), pos.get("1", 0)
                else:
                    x, y = pos[0] if len(pos) > 0 else 0, pos[1] if len(pos) > 1 else 0
                if isinstance(size, dict):
                    w, h = size.get("0", 200), size.get("1", 100)
                else:
                    w, h = size[0] if len(size) > 0 else 200, size[1] if len(size) > 1 else 100

                x, y, w, h = round(x), round(y), round(w), round(h)
                title = (node.get("title") or node.get("type") or "").replace(",", ";")

                node_data = {
                    "id": node.get("id"),
                    "title": title,
                    "x": x, "y": y, "w": w, "h": h
                }
                all_nodes.append(node_data)

                if node.get("id") in affected_ids:
                    affected_nodes.append(node_data)

            # Show affected nodes (only the ones we touched)
            if affected_nodes:
                lines.append(f"affected[{len(affected_nodes)}]{{id,title,x,y,w,h}}:")
                for n in affected_nodes:
                    lines.append(f"  {n['id']},{n['title']},{n['x']},{n['y']},{n['w']},{n['h']}")

            # Check for collisions involving affected nodes
            collisions = []
            for affected in affected_nodes:
                for other in all_nodes:
                    if affected["id"] == other["id"]:
                        continue
                    # Check rectangle intersection
                    x_overlap = max(0, min(affected["x"] + affected["w"], other["x"] + other["w"]) - max(affected["x"], other["x"]))
                    y_overlap = max(0, min(affected["y"] + affected["h"], other["y"] + other["h"]) - max(affected["y"], other["y"]))
                    if x_overlap > 0 and y_overlap > 0:
                        # Avoid duplicate pairs
                        pair = tuple(sorted([affected["id"], other["id"]]))
                        collision_str = f"  {pair[0]}<->{pair[1]} (overlap: {x_overlap}x{y_overlap})"
                        if collision_str not in collisions:
                            collisions.append(collision_str)

            if collisions:
                lines.append(f"collisions[{len(collisions)}]:")
                lines.extend(collisions)

    return "\n".join(lines)


def center_on_node(node_id: str) -> str:
    """Center the user's view on a specific node.

    Args:
        node_id: The ID of the node to center on.

    Returns:
        Status message in TOON format.
    """
    result = send_graph_command("center_on_node", {"node_id": str(node_id)})

    if "error" in result:
        return f"error: {result['error']}"

    return f"ok: centered on node {node_id}"


def run_node(node_ids) -> dict:
    """Run the workflow for one or more nodes.

    Args:
        node_ids: Single node ID (string) or list of node IDs
    """
    # Normalize to list
    if isinstance(node_ids, str):
        node_ids = [node_ids]
    elif isinstance(node_ids, list):
        node_ids = [str(n) for n in node_ids]
    else:
        node_ids = [str(node_ids)]

    # Fetch workflow API on demand via graph command (avoids constant polling flicker)
    workflow_api_result = send_graph_command("get_workflow_api", {})

    if "error" in workflow_api_result:
        return workflow_api_result

    workflow_api = workflow_api_result.get("workflow_api")
    if not workflow_api:
        return {"error": "No workflow available. Make sure ComfyUI is open in browser."}

    prompt = workflow_api.get("output", workflow_api)

    # Validate all node IDs first
    invalid_nodes = []
    valid_nodes = []
    for node_id in node_ids:
        node_id_str = str(node_id)
        if node_id_str not in prompt:
            invalid_nodes.append(node_id_str)
        else:
            valid_nodes.append(node_id_str)

    if invalid_nodes and not valid_nodes:
        return {"error": f"Node(s) not found in workflow: {invalid_nodes}"}

    # Queue via frontend so preview images show in the UI (uses browser's client_id)
    result = send_graph_command("queue_prompt", {})

    results = []
    if invalid_nodes:
        for node_id in invalid_nodes:
            results.append({"error": f"Node {node_id} not found", "node_id": node_id})

    for node_id in valid_nodes:
        if "error" in result:
            results.append({"error": result["error"], "node_id": node_id})
        else:
            results.append({"status": "queued", "node_id": node_id})

        if "error" in result:
            results.append({"error": result["error"], "node_id": node_id_str})
        else:
            results.append({
                "status": "queued",
                "prompt_id": result.get("prompt_id"),
                "node_id": node_id_str
            })

    # Return single result for single input
    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def send_graph_command(action: str, params: dict) -> dict:
    """Send a graph manipulation command to the frontend."""
    result = make_request("/claude-code/graph-command", method="POST", data={
        "action": action,
        "params": params
    })
    return result


def create_node(nodes) -> dict:
    """Create one or more nodes in the workflow.

    Args:
        nodes: Either a single node dict or a list of node dicts.
               Each dict should have: node_type (required), pos_x, pos_y, title (optional)
    """
    # Normalize to list
    if isinstance(nodes, dict):
        nodes = [nodes]

    # Get the node type info to validate (cached)
    all_nodes = get_object_info_cached()
    if "error" in all_nodes:
        return all_nodes

    results = []
    for node in nodes:
        node_type = node.get("node_type", "")
        pos_x = node.get("pos_x", 100)
        pos_y = node.get("pos_y", 100)
        title = node.get("title")

        if not node_type:
            results.append({"error": "node_type is required", "input": node})
            continue

        if node_type not in all_nodes:
            results.append({"error": f"Unknown node type: {node_type}", "input": node})
            continue

        result = send_graph_command("create_node", {
            "type": node_type,
            "pos_x": pos_x,
            "pos_y": pos_y,
            "title": title
        })
        results.append(result)

    # Return single result for single input, array for multiple
    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def delete_nodes(node_ids) -> dict:
    """Delete one or more nodes from the workflow.

    Args:
        node_ids: Single node ID (string) or list of node IDs
    """
    # Normalize to list
    if isinstance(node_ids, str):
        node_ids = [node_ids]
    elif not isinstance(node_ids, list):
        node_ids = [str(node_ids)]

    results = []
    for node_id in node_ids:
        result = send_graph_command("delete_node", {
            "node_id": str(node_id)
        })
        results.append(result)

    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def set_node_property(properties) -> dict:
    """Set one or more properties on nodes.

    Args:
        properties: Either a single property dict or a list of property dicts.
                    Each dict should have: node_id, property_name, value
    """
    # Normalize to list
    if isinstance(properties, dict):
        properties = [properties]

    results = []
    for prop in properties:
        node_id = prop.get("node_id", "")
        property_name = prop.get("property_name", "")
        value = prop.get("value")

        if not node_id or not property_name:
            results.append({"error": "node_id and property_name are required", "input": prop})
            continue

        result = send_graph_command("set_node_property", {
            "node_id": str(node_id),
            "property_name": property_name,
            "value": value
        })
        results.append(result)

    # Return single result for single input, array for multiple
    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }

    return result


def connect_nodes(connections) -> dict:
    """Connect one or more pairs of nodes.

    Args:
        connections: Single connection dict or list of dicts.
                     Each dict: {from_node_id, from_slot, to_node_id, to_slot}
    """
    # Normalize to list
    if isinstance(connections, dict):
        connections = [connections]

    results = []
    for conn in connections:
        from_node_id = conn.get("from_node_id", "")
        from_slot = conn.get("from_slot", 0)
        to_node_id = conn.get("to_node_id", "")
        to_slot = conn.get("to_slot", 0)

        if not from_node_id or not to_node_id:
            results.append({"error": "from_node_id and to_node_id are required", "input": conn})
            continue

        result = send_graph_command("connect_nodes", {
            "from_node_id": str(from_node_id),
            "from_slot": from_slot,
            "to_node_id": str(to_node_id),
            "to_slot": to_slot
        })
        results.append(result)

    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def disconnect_nodes(disconnections) -> dict:
    """Disconnect one or more pairs of nodes.

    Args:
        disconnections: Single disconnection dict or list of dicts.
                        Each dict: {from_node_id, from_slot, to_node_id, to_slot}
    """
    # Normalize to list
    if isinstance(disconnections, dict):
        disconnections = [disconnections]

    results = []
    for disc in disconnections:
        from_node_id = disc.get("from_node_id", "")
        from_slot = disc.get("from_slot", 0)
        to_node_id = disc.get("to_node_id", "")
        to_slot = disc.get("to_slot", 0)

        if not from_node_id or not to_node_id:
            results.append({"error": "from_node_id and to_node_id are required", "input": disc})
            continue

        result = send_graph_command("disconnect_nodes", {
            "from_node_id": str(from_node_id),
            "from_slot": from_slot,
            "to_node_id": str(to_node_id),
            "to_slot": to_slot
        })
        results.append(result)

    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def move_nodes(moves) -> dict:
    """Move and/or resize one or more nodes.

    Args:
        moves: Single move dict or list of dicts.
               Each dict: {node_id (required), x, y, relative_to, direction, gap, width, height}
    """
    # Normalize to list
    if isinstance(moves, dict):
        moves = [moves]

    results = []
    for move in moves:
        node_id = move.get("node_id", "")
        if not node_id:
            results.append({"error": "node_id is required", "input": move})
            continue

        result = send_graph_command("move_node", {
            "node_id": str(node_id),
            "x": move.get("x"),
            "y": move.get("y"),
            "relative_to": str(move.get("relative_to")) if move.get("relative_to") else None,
            "direction": move.get("direction"),
            "gap": move.get("gap", 30),
            "width": move.get("width"),
            "height": move.get("height")
        })
        results.append(result)

    if len(results) == 1:
        return results[0]

    succeeded = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results
    }


def get_node_info(node_id: str) -> str:
    """Get detailed info about a specific node in the workflow in compact TOON-like format."""
    workflow_data = get_workflow()

    if "error" in workflow_data or "message" in workflow_data:
        return f"error: {workflow_data.get('error') or workflow_data.get('message')}"

    workflow = workflow_data.get("workflow", {})
    node_id_str = str(node_id)
    try:
        node_id_int = int(node_id)
    except ValueError:
        return f"error: invalid node_id '{node_id}'"

    # Handle graph serialize format
    if "nodes" in workflow:
        for node in workflow.get("nodes", []):
            if node.get("id") == node_id_int:
                node_type = node.get("type")
                title = node.get("title") or node_type
                pos = node.get("pos", [0, 0])
                size = node.get("size", [200, 100])

                # Handle array/dict formats
                if isinstance(pos, dict):
                    x, y = pos.get("0", 0), pos.get("1", 0)
                else:
                    x, y = pos[0] if len(pos) > 0 else 0, pos[1] if len(pos) > 1 else 0
                if isinstance(size, dict):
                    w, h = size.get("0", 200), size.get("1", 100)
                else:
                    w, h = size[0] if len(size) > 0 else 200, size[1] if len(size) > 1 else 100

                lines = []
                lines.append(f"node {node_id_int}: {title}")
                lines.append(f"type: {node_type}")
                lines.append(f"pos: {round(x)},{round(y)} size: {round(w)}x{round(h)}")

                # Get type info for input/output details
                type_info = {}
                all_nodes = get_object_info_cached()
                if "error" not in all_nodes and node_type in all_nodes:
                    type_info = all_nodes[node_type]

                if type_info:
                    cat = type_info.get("category", "")
                    desc = type_info.get("description", "")
                    if cat:
                        lines.append(f"category: {cat}")
                    if desc:
                        lines.append(f"desc: {desc[:100]}")

                    # Inputs from type info
                    input_info = type_info.get("input", {})
                    inputs = []
                    for group in ["required", "optional"]:
                        if group in input_info:
                            req_marker = "*" if group == "required" else ""
                            for inp_name, inp_def in input_info[group].items():
                                if isinstance(inp_def, list) and len(inp_def) > 0:
                                    inp_type = inp_def[0] if isinstance(inp_def[0], str) else type(inp_def[0]).__name__
                                    inputs.append(f"{inp_name}{req_marker}:{inp_type}")
                    if inputs:
                        lines.append(f"inputs: {','.join(inputs)}")

                    # Outputs from type info
                    outputs = type_info.get("output", [])
                    output_names = type_info.get("output_name", outputs)
                    if outputs:
                        out_parts = []
                        for i, out_type in enumerate(outputs):
                            out_name = output_names[i] if i < len(output_names) else out_type
                            out_parts.append(f"{out_name}:{out_type}")
                        lines.append(f"outputs: {','.join(out_parts)}")

                # Current connections (from workflow node data)
                node_inputs = node.get("inputs", [])
                if node_inputs:
                    conn_parts = []
                    for inp in node_inputs:
                        if isinstance(inp, dict) and inp.get("link"):
                            inp_name = inp.get("name", "?")
                            link_id = inp.get("link")
                            conn_parts.append(f"{inp_name}=link{link_id}")
                    if conn_parts:
                        lines.append(f"connected_inputs: {','.join(conn_parts)}")

                node_outputs = node.get("outputs", [])
                if node_outputs:
                    conn_parts = []
                    for out in node_outputs:
                        if isinstance(out, dict) and out.get("links"):
                            out_name = out.get("name", "?")
                            links = out.get("links", [])
                            conn_parts.append(f"{out_name}->links{links}")
                    if conn_parts:
                        lines.append(f"connected_outputs: {','.join(conn_parts)}")

                # Widget values
                widgets = node.get("widgets_values")
                if widgets:
                    # Compact widget display - truncate long values
                    widget_strs = []
                    for i, val in enumerate(widgets):
                        val_str = str(val)
                        if len(val_str) > 50:
                            val_str = val_str[:47] + "..."
                        widget_strs.append(val_str.replace(",", ";").replace("\n", "\\n"))
                    lines.append(f"widgets[{len(widgets)}]: {','.join(widget_strs)}")

                return "\n".join(lines)

        return f"error: node {node_id} not found in workflow"

    # Handle API format
    if node_id_str in workflow:
        node_data = workflow[node_id_str]
        node_type = node_data.get("class_type", "?")
        inputs = node_data.get("inputs", {})

        lines = [f"node {node_id_str}: {node_type}"]
        if inputs:
            inp_parts = [f"{k}={v}" for k, v in inputs.items()]
            lines.append(f"inputs: {','.join(inp_parts)}")
        return "\n".join(lines)

    return f"error: node {node_id} not found in workflow"


def summarize_workflow() -> str:
    """Get a compact summary of the current workflow in TOON-like format.

    Returns a token-efficient text representation with:
    - Canvas bounds
    - Node list with id, type, title, position, size
    - Connections list

    Format:
        canvas: min_x,min_y to max_x,max_y
        nodes[N]{id,type,title,x,y,w,h}:
          id,type,title,x,y,w,h
          ...
        connections[N]{from_node:slot->to_node:slot,type}:
          from:slot->to:slot,TYPE
          ...
    """
    workflow_data = get_workflow()

    if "error" in workflow_data or "message" in workflow_data:
        return f"error: {workflow_data.get('error') or workflow_data.get('message')}"

    workflow = workflow_data.get("workflow", {})

    if "nodes" not in workflow:
        return "error: No nodes in workflow"

    lines = []
    nodes = []
    min_x, min_y = float('inf'), float('inf')
    max_x, max_y = float('-inf'), float('-inf')

    # Graph serialize format
    for node in workflow.get("nodes", []):
        pos = node.get("pos", [0, 0])
        size = node.get("size", [200, 100])

        # Handle both array and object formats for pos
        if isinstance(pos, dict):
            x, y = pos.get("0", 0), pos.get("1", 0)
        else:
            x, y = pos[0] if len(pos) > 0 else 0, pos[1] if len(pos) > 1 else 0

        # Handle both array and object formats for size
        if isinstance(size, dict):
            w, h = size.get("0", 200), size.get("1", 100)
        else:
            w, h = size[0] if len(size) > 0 else 200, size[1] if len(size) > 1 else 100

        x, y, w, h = round(x), round(y), round(w), round(h)

        min_x = min(min_x, x)
        min_y = min(min_y, y)
        max_x = max(max_x, x + w)
        max_y = max(max_y, y + h)

        # Escape commas in title/type
        node_type = (node.get("type") or "").replace(",", ";")
        title = (node.get("title") or "").replace(",", ";")

        nodes.append({
            "id": node.get("id"),
            "type": node_type,
            "title": title,
            "x": x, "y": y, "w": w, "h": h
        })

    # Sort by id for consistent output
    nodes.sort(key=lambda n: int(n["id"]) if str(n["id"]).isdigit() else 0)

    # Canvas bounds
    if nodes:
        lines.append(f"canvas: {round(min_x)},{round(min_y)} to {round(max_x)},{round(max_y)}")

    # Nodes section
    lines.append(f"nodes[{len(nodes)}]{{id,type,title,x,y,w,h}}:")
    for n in nodes:
        lines.append(f"  {n['id']},{n['type']},{n['title']},{n['x']},{n['y']},{n['w']},{n['h']}")

    # Connections section
    links = workflow.get("links", [])
    if links:
        lines.append(f"connections[{len(links)}]{{from:slot->to:slot,type}}:")
        for link in links:
            if len(link) >= 6:
                # link format: [link_id, from_node, from_slot, to_node, to_slot, type]
                lines.append(f"  {link[1]}:{link[2]}->{link[3]}:{link[4]},{link[5]}")

    # Collision detection - O(n²) but fast for typical workflow sizes
    collisions = []
    for i, a in enumerate(nodes):
        for b in nodes[i+1:]:
            # Check rectangle intersection
            # Two rects overlap if they overlap on both axes
            x_overlap = max(0, min(a["x"] + a["w"], b["x"] + b["w"]) - max(a["x"], b["x"]))
            y_overlap = max(0, min(a["y"] + a["h"], b["y"] + b["h"]) - max(a["y"], b["y"]))
            if x_overlap > 0 and y_overlap > 0:
                collisions.append(f"  {a['id']}<->{b['id']} (overlap: {x_overlap}x{y_overlap})")

    if collisions:
        lines.append(f"collisions[{len(collisions)}]:")
        lines.extend(collisions)

    return "\n".join(lines)


def get_layout_summary() -> str:
    """Get a compact layout summary showing node positions and sizes.

    Returns bounding boxes for all nodes in TOON-like format (token-efficient)
    so Claude can see occupied space and avoid collisions when placing new nodes.

    Format:
        canvas: min_x,min_y to max_x,max_y
        nodes[N]{id,title,x,y,w,h}:
        id,title,x,y,w,h
        ...
    """
    workflow_data = get_workflow()

    if "error" in workflow_data or "message" in workflow_data:
        return "error: Could not get workflow layout"

    workflow = workflow_data.get("workflow", {})

    if "nodes" not in workflow:
        return "error: No nodes in workflow"

    nodes = []
    min_x, min_y = float('inf'), float('inf')
    max_x, max_y = float('-inf'), float('-inf')

    for node in workflow.get("nodes", []):
        pos = node.get("pos", [0, 0])
        size = node.get("size", [200, 100])  # Default size if not available

        # Handle both array and object formats for pos
        if isinstance(pos, dict):
            x, y = pos.get("0", 0), pos.get("1", 0)
        else:
            x, y = pos[0] if len(pos) > 0 else 0, pos[1] if len(pos) > 1 else 0

        # Handle both array and object formats for size
        if isinstance(size, dict):
            w, h = size.get("0", 200), size.get("1", 100)
        else:
            w, h = size[0] if len(size) > 0 else 200, size[1] if len(size) > 1 else 100

        # Round for cleaner output
        x, y, w, h = round(x), round(y), round(w), round(h)

        # Track canvas bounds
        min_x = min(min_x, x)
        min_y = min(min_y, y)
        max_x = max(max_x, x + w)
        max_y = max(max_y, y + h)

        # Escape commas in title
        title = (node.get("title") or node.get("type") or "").replace(",", ";")

        nodes.append({
            "id": node.get("id"),
            "title": title,
            "x": x,
            "y": y,
            "w": w,
            "h": h
        })

    # Sort by position (top-left to bottom-right) for easier reading
    nodes.sort(key=lambda n: (n["y"], n["x"]))

    # Build TOON-like output
    lines = []

    # Canvas bounds
    bounds_min_x = round(min_x) if min_x != float('inf') else 0
    bounds_min_y = round(min_y) if min_y != float('inf') else 0
    bounds_max_x = round(max_x) if max_x != float('-inf') else 0
    bounds_max_y = round(max_y) if max_y != float('-inf') else 0
    lines.append(f"canvas: {bounds_min_x},{bounds_min_y} to {bounds_max_x},{bounds_max_y}")

    # Nodes in tabular format
    lines.append(f"nodes[{len(nodes)}]{{id,title,x,y,w,h}}:")
    for n in nodes:
        lines.append(f"  {n['id']},{n['title']},{n['x']},{n['y']},{n['w']},{n['h']}")

    return "\n".join(lines)


def view_image(node_id: str = None, image_index: int = 0) -> dict:
    """View an image from a Preview Image or Save Image node.

    Args:
        node_id: The ID of the Preview Image or Save Image node.
                 If not provided, finds the first/most recent image node.
        image_index: Which image to view if the node has multiple (0-based). Default: 0

    Returns:
        Image data as base64 with metadata, or error message.
    """
    # Get the current workflow to find image nodes
    workflow_data = get_workflow()
    if "error" in workflow_data:
        return workflow_data

    workflow = workflow_data.get("workflow", {})

    # Find image nodes (Preview Image, Save Image, etc.)
    image_nodes = []
    if "nodes" in workflow:
        for node in workflow.get("nodes", []):
            node_type = node.get("type", "")
            if any(t in node_type.lower() for t in ["preview", "saveimage", "save image"]):
                image_nodes.append({
                    "id": node.get("id"),
                    "type": node_type,
                    "title": node.get("title") or node_type
                })

    if not image_nodes:
        return {"error": "No Preview Image or Save Image nodes found in workflow"}

    # Find target node
    target_node = None
    if node_id:
        node_id_int = int(node_id)
        for n in image_nodes:
            if n["id"] == node_id_int:
                target_node = n
                break
        if not target_node:
            return {
                "error": f"Node {node_id} is not an image node",
                "available_image_nodes": image_nodes
            }
    else:
        # Use the first image node
        target_node = image_nodes[0]

    # Get history to find the actual image files
    history = get_history()
    if "error" in history:
        return {"error": "Could not get history. Run the workflow first to generate images."}

    # Search through history for outputs from this node
    # Sort by timestamp (most recent first) to get the latest image
    target_node_id = str(target_node["id"])
    image_info = None

    # Build list with timestamps for sorting
    history_items = []
    for prompt_id, prompt_data in history.items():
        if not isinstance(prompt_data, dict):
            continue
        status = prompt_data.get("status", {})
        # Get timestamp from status messages
        messages = status.get("messages", [])
        timestamp = 0
        for msg in messages:
            if len(msg) >= 2 and isinstance(msg[1], dict):
                ts = msg[1].get("timestamp", 0)
                if ts > timestamp:
                    timestamp = ts
        history_items.append((prompt_id, prompt_data, timestamp))

    # Sort by timestamp descending (most recent first)
    history_items.sort(key=lambda x: x[2], reverse=True)

    for prompt_id, prompt_data, _ in history_items:
        outputs = prompt_data.get("outputs", {})
        if target_node_id in outputs:
            node_outputs = outputs[target_node_id]
            if "images" in node_outputs:
                images = node_outputs["images"]
                if images and len(images) > image_index:
                    image_info = images[image_index]
                    break

    if not image_info:
        return {
            "error": f"No images found for node {target_node_id}. Run the workflow first.",
            "node": target_node,
            "available_image_nodes": image_nodes
        }

    # Fetch the actual image
    filename = image_info.get("filename", "")
    subfolder = image_info.get("subfolder", "")
    img_type = image_info.get("type", "output")  # 'output' for Save Image, 'temp' for Preview Image

    params = f"filename={urllib.parse.quote(filename)}&type={img_type}"
    if subfolder:
        params += f"&subfolder={urllib.parse.quote(subfolder)}"

    url = f"{get_comfyui_url()}/view?{params}"

    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=30) as response:
            image_data = response.read()
            content_type = response.headers.get("Content-Type", "image/png")

            # Convert to base64
            base64_data = base64.b64encode(image_data).decode("utf-8")

            # Determine media type
            if "jpeg" in content_type or "jpg" in content_type:
                media_type = "image/jpeg"
            elif "webp" in content_type:
                media_type = "image/webp"
            else:
                media_type = "image/png"

            return {
                "node_id": target_node["id"],
                "node_title": target_node["title"],
                "node_type": target_node["type"],
                "filename": filename,
                "image_index": image_index,
                "media_type": media_type,
                "base64_data": base64_data
            }

    except urllib.error.HTTPError as e:
        return {"error": f"Failed to fetch image: HTTP {e.code}"}
    except Exception as e:
        return {"error": f"Failed to fetch image: {str(e)}"}


# ============== NODE MANAGEMENT (via ComfyUI Registry API + git) ==============

COMFY_REGISTRY_API = "https://api.comfy.org"


def get_comfyui_custom_nodes_dir() -> str:
    """Get the ComfyUI custom_nodes directory."""
    import os

    # From the plugin directory (we're inside custom_nodes)
    plugin_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(plugin_dir)

    # Verify parent is actually custom_nodes
    if os.path.basename(parent_dir) == "custom_nodes":
        return parent_dir

    # Fallback: common locations
    possible_paths = [
        os.path.expanduser("~/ComfyUI/custom_nodes"),
        os.path.expanduser("~/comfyui/custom_nodes"),
        "/opt/ComfyUI/custom_nodes",
        "/workspace/ComfyUI/custom_nodes",
        os.path.expanduser("~/Documents/ComfyUI/custom_nodes"),
    ]

    for path in possible_paths:
        if os.path.isdir(path):
            return os.path.abspath(path)

    return None


def query_registry(endpoint: str, params: dict = None) -> dict:
    """Query the ComfyUI Registry API."""
    import urllib.parse

    url = f"{COMFY_REGISTRY_API}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ComfyPilot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": f"Registry API error: {e.code} {e.reason}"}
    except Exception as e:
        return {"error": f"Failed to query registry: {str(e)}"}


def get_installed_nodes() -> dict:
    """Get a map of installed custom nodes by scanning the custom_nodes directory."""
    import os

    custom_nodes_dir = get_comfyui_custom_nodes_dir()
    if not custom_nodes_dir:
        return {}

    installed = {}
    for name in os.listdir(custom_nodes_dir):
        path = os.path.join(custom_nodes_dir, name)
        if os.path.isdir(path) and not name.startswith(".") and not name.startswith("__"):
            # Check if it's a git repo
            is_git = os.path.isdir(os.path.join(path, ".git"))
            installed[name.lower()] = {
                "name": name,
                "path": path,
                "is_git": is_git
            }

    return installed


def search_custom_nodes(query: str = None, status: str = "all", category: str = None, limit: int = 10) -> dict:
    """Search for custom nodes in the ComfyUI Registry.

    Args:
        query: Search term (matches name, description, author). Case-insensitive.
        status: Filter by installation status: "all", "installed", "not-installed"
        category: Filter by category (not yet supported by registry API)
        limit: Maximum results to return (default 10)

    Returns:
        List of matching nodes with basic info.
    """
    # Get installed nodes for status filtering
    installed_map = get_installed_nodes()

    # If filtering for installed only, just return local info
    if status == "installed":
        results = []
        for node_name, info in installed_map.items():
            if query and query.lower() not in node_name.lower():
                continue
            results.append({
                "id": node_name,
                "name": info["name"],
                "installed": True,
                "path": info["path"],
                "is_git": info["is_git"]
            })
            if len(results) >= limit:
                break

        return {
            "total_matches": len(results),
            "limit": limit,
            "query": query,
            "status_filter": status,
            "nodes": results
        }

    # Query the registry API
    params = {"limit": min(limit * 2, 50)}  # Fetch extra in case we need to filter
    if query:
        params["search"] = query

    result = query_registry("/nodes/search", params)
    if "error" in result:
        return result

    nodes = result.get("nodes", [])
    results = []

    for node in nodes:
        node_id = node.get("id", "")
        name = node.get("name", node_id)
        repo = node.get("repository", "")
        description = node.get("description", "")
        author = node.get("publisher", {}).get("name", "") if isinstance(node.get("publisher"), dict) else ""
        stars = node.get("github_stars", 0)
        downloads = node.get("downloads", 0)

        # Check if installed (match by id or repo folder name)
        is_installed = False
        repo_name = repo.rstrip("/").split("/")[-1] if repo else ""
        if node_id.lower() in installed_map or repo_name.lower() in installed_map:
            is_installed = True

        # Status filter
        if status == "not-installed" and is_installed:
            continue

        results.append({
            "id": node_id,
            "name": name,
            "author": author,
            "description": description[:150] + "..." if len(description) > 150 else description,
            "repository": repo,
            "installed": is_installed,
            "stars": stars,
            "downloads": downloads
        })

        if len(results) >= limit:
            break

    return {
        "total_matches": result.get("total", len(results)),
        "limit": limit,
        "query": query,
        "status_filter": status,
        "nodes": results
    }


def install_custom_node(node_id: str) -> dict:
    """Install a custom node by cloning from git.

    Args:
        node_id: The node ID, name, or git URL to install

    Returns:
        Installation status.
    """
    import subprocess
    import shutil
    import os

    custom_nodes_dir = get_comfyui_custom_nodes_dir()
    if not custom_nodes_dir:
        return {"error": "Could not find ComfyUI custom_nodes directory"}

    # Check if git is available
    git = shutil.which("git")
    if not git:
        return {"error": "git is not installed. Please install git to use this feature."}

    # Determine the git URL
    if node_id.startswith("http://") or node_id.startswith("https://"):
        git_url = node_id
        repo_name = git_url.rstrip("/").split("/")[-1].replace(".git", "")
    else:
        # Look up in the registry - first try direct lookup
        result = query_registry(f"/nodes/{node_id}")
        if "error" in result or not result.get("repository"):
            # Try search
            search_result = query_registry("/nodes/search", {"search": node_id, "limit": 5})
            if "error" in search_result:
                return search_result

            # Find best match
            nodes = search_result.get("nodes", [])
            target = None
            for node in nodes:
                if node.get("id", "").lower() == node_id.lower() or node.get("name", "").lower() == node_id.lower():
                    target = node
                    break
            if not target and nodes:
                target = nodes[0]  # Take first result

            if not target:
                return {"error": f"Node '{node_id}' not found in registry. Use search_custom_nodes to find available nodes."}

            result = target

        git_url = result.get("repository", "")
        if not git_url:
            return {"error": f"No repository URL found for '{node_id}'"}

        repo_name = git_url.rstrip("/").split("/")[-1].replace(".git", "")

    # Check if already installed
    dest_path = os.path.join(custom_nodes_dir, repo_name)
    if os.path.exists(dest_path):
        return {
            "status": "already_installed",
            "path": dest_path,
            "message": f"'{repo_name}' is already installed at {dest_path}"
        }

    # Clone the repository
    try:
        subprocess.run(
            [git, "clone", "--depth", "1", git_url, dest_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=300
        )
    except subprocess.CalledProcessError as e:
        return {"error": f"git clone failed: {e.stderr}"}
    except subprocess.TimeoutExpired:
        return {"error": "git clone timed out after 5 minutes"}

    # Check for requirements.txt and install dependencies
    requirements_file = os.path.join(dest_path, "requirements.txt")
    pip_message = ""
    if os.path.exists(requirements_file):
        pip_message = " Note: This node has a requirements.txt - dependencies may need to be installed."

    return {
        "status": "installed",
        "node_id": node_id,
        "repository": git_url,
        "path": dest_path,
        "message": f"Installed to {dest_path}. Restart ComfyUI to load the node.{pip_message}"
    }


def uninstall_custom_node(node_id: str) -> dict:
    """Uninstall a custom node by removing its directory.

    Args:
        node_id: The node ID or folder name to uninstall

    Returns:
        Uninstallation status.
    """
    import shutil
    import os

    custom_nodes_dir = get_comfyui_custom_nodes_dir()
    if not custom_nodes_dir:
        return {"error": "Could not find ComfyUI custom_nodes directory"}

    # Find the installed node
    installed = get_installed_nodes()

    target_path = None
    target_name = None

    # Try exact match first
    if node_id.lower() in installed:
        target_path = installed[node_id.lower()]["path"]
        target_name = installed[node_id.lower()]["name"]
    else:
        # Try partial match
        for name, info in installed.items():
            if node_id.lower() in name:
                target_path = info["path"]
                target_name = info["name"]
                break

    if not target_path:
        return {"error": f"Node '{node_id}' is not installed. Use search_custom_nodes(status='installed') to see installed nodes."}

    # Confirm the path is inside custom_nodes (safety check)
    if not os.path.abspath(target_path).startswith(os.path.abspath(custom_nodes_dir)):
        return {"error": "Security error: target path is outside custom_nodes directory"}

    # Remove the directory
    try:
        shutil.rmtree(target_path)
    except Exception as e:
        return {"error": f"Failed to remove directory: {str(e)}"}

    return {
        "status": "uninstalled",
        "node_id": target_name,
        "path": target_path,
        "message": f"Removed {target_name}. Restart ComfyUI to complete uninstallation."
    }


def update_custom_node(node_id: str) -> dict:
    """Update a custom node by running git pull.

    Args:
        node_id: The node ID or folder name to update

    Returns:
        Update status.
    """
    import subprocess
    import shutil
    import os

    custom_nodes_dir = get_comfyui_custom_nodes_dir()
    if not custom_nodes_dir:
        return {"error": "Could not find ComfyUI custom_nodes directory"}

    # Check if git is available
    git = shutil.which("git")
    if not git:
        return {"error": "git is not installed. Please install git to use this feature."}

    # Find the installed node
    installed = get_installed_nodes()

    target_path = None
    target_name = None
    is_git = False

    # Try exact match first
    if node_id.lower() in installed:
        info = installed[node_id.lower()]
        target_path = info["path"]
        target_name = info["name"]
        is_git = info["is_git"]
    else:
        # Try partial match
        for name, info in installed.items():
            if node_id.lower() in name:
                target_path = info["path"]
                target_name = info["name"]
                is_git = info["is_git"]
                break

    if not target_path:
        return {"error": f"Node '{node_id}' is not installed. Install it first with install_custom_node."}

    if not is_git:
        return {"error": f"'{target_name}' is not a git repository and cannot be updated this way."}

    # Run git pull
    try:
        result = subprocess.run(
            [git, "pull"],
            cwd=target_path,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            return {"error": f"git pull failed: {result.stderr}"}

        # Check if there were updates
        if "Already up to date" in result.stdout:
            return {
                "status": "up_to_date",
                "node_id": target_name,
                "message": f"'{target_name}' is already up to date."
            }

        return {
            "status": "updated",
            "node_id": target_name,
            "path": target_path,
            "message": f"Updated '{target_name}'. Restart ComfyUI to load changes."
        }

    except subprocess.TimeoutExpired:
        return {"error": "git pull timed out after 2 minutes"}


# ============== MODEL DOWNLOAD ==============

# Model type to ComfyUI folder mapping
MODEL_TYPE_FOLDERS = {
    "checkpoint": "checkpoints",
    "checkpoints": "checkpoints",
    "lora": "loras",
    "loras": "loras",
    "vae": "vae",
    "controlnet": "controlnet",
    "clip": "clip",
    "clip_vision": "clip_vision",
    "unet": "unet",
    "diffusion_model": "diffusion_models",
    "diffusion_models": "diffusion_models",
    "text_encoder": "text_encoders",
    "text_encoders": "text_encoders",
    "upscale_model": "upscale_models",
    "upscale_models": "upscale_models",
    "embeddings": "embeddings",
    "embedding": "embeddings",
    "hypernetwork": "hypernetworks",
    "hypernetworks": "hypernetworks",
    "style_model": "style_models",
    "style_models": "style_models",
    "ipadapter": "ipadapter",
    "instantid": "instantid",
    "insightface": "insightface",
    "pulid": "pulid",
    "reactor": "reactor",
    "animatediff": "animatediff_models",
}


def get_comfyui_models_dir() -> str:
    """Get the ComfyUI models directory."""
    import os

    # Try to get from ComfyUI's folder_paths if available
    try:
        # Query ComfyUI for its base path
        result = make_request("/system_stats")
        if "error" not in result:
            # ComfyUI is running, try to find models folder
            pass
    except Exception:
        pass

    # Common locations
    possible_paths = [
        # From the plugin directory (go up to ComfyUI root)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "models"),
        # Standard ComfyUI locations
        os.path.expanduser("~/ComfyUI/models"),
        os.path.expanduser("~/comfyui/models"),
        "/opt/ComfyUI/models",
        "/workspace/ComfyUI/models",
        # ComfyUI Desktop (macOS)
        os.path.expanduser("~/Documents/comfy/ComfyUI/models"),
        os.path.expanduser("~/ComfyUI-Desktop/ComfyUI/models"),
    ]

    for path in possible_paths:
        if os.path.isdir(path):
            return os.path.abspath(path)

    return None


def parse_hf_url(url: str) -> dict:
    """Parse a Hugging Face URL to extract repo and file info.

    Supports:
    - https://huggingface.co/user/repo/blob/main/file.safetensors
    - https://huggingface.co/user/repo/resolve/main/file.safetensors
    - user/repo (assumes root of repo)
    - user/repo/file.safetensors
    """
    import re

    # Full URL format
    hf_pattern = r"https?://huggingface\.co/([^/]+)/([^/]+)(?:/(?:blob|resolve)/([^/]+)/(.+))?"
    match = re.match(hf_pattern, url)
    if match:
        user, repo, branch, filepath = match.groups()
        return {
            "type": "huggingface",
            "repo": f"{user}/{repo}",
            "branch": branch or "main",
            "filepath": filepath,
        }

    # Short format: user/repo or user/repo/file.safetensors
    if "/" in url and not url.startswith("http"):
        parts = url.split("/")
        if len(parts) >= 2:
            repo = f"{parts[0]}/{parts[1]}"
            filepath = "/".join(parts[2:]) if len(parts) > 2 else None
            return {
                "type": "huggingface",
                "repo": repo,
                "branch": "main",
                "filepath": filepath,
            }

    return None


def parse_civitai_url(url: str) -> dict:
    """Parse a CivitAI URL to extract model info.

    Supports:
    - https://civitai.com/models/123456
    - https://civitai.com/models/123456/model-name
    - https://civitai.com/api/download/models/789
    """
    import re

    # API download URL
    api_pattern = r"https?://civitai\.com/api/download/models/(\d+)"
    match = re.match(api_pattern, url)
    if match:
        return {
            "type": "civitai",
            "model_version_id": match.group(1),
            "download_url": url,
        }

    # Model page URL
    model_pattern = r"https?://civitai\.com/models/(\d+)"
    match = re.match(model_pattern, url)
    if match:
        return {
            "type": "civitai",
            "model_id": match.group(1),
        }

    return None


def download_model(url: str, model_type: str, filename: str = None, hf_token: str = None, subfolder: str = None) -> dict:
    """Download a model to the appropriate ComfyUI folder.

    Args:
        url: Model URL (Hugging Face, CivitAI, or direct download URL)
        model_type: Type of model - determines destination folder
                   (checkpoint, lora, vae, controlnet, clip, unet, embeddings, etc.)
        filename: Optional filename override. Auto-detected from URL if not provided.
        hf_token: Hugging Face token for gated models. Only needed if download fails with auth error.
        subfolder: Optional subfolder within the model type directory

    Returns:
        Download status with file path, or error with instructions for gated models.
    """
    import subprocess
    import os
    import shutil
    import re

    # Validate model_type
    folder_name = MODEL_TYPE_FOLDERS.get(model_type.lower())
    if not folder_name:
        return {
            "error": f"Unknown model_type: {model_type}",
            "valid_types": list(set(MODEL_TYPE_FOLDERS.values()))
        }

    # Get ComfyUI models directory
    models_dir = get_comfyui_models_dir()
    if not models_dir:
        return {"error": "Could not find ComfyUI models directory. Is ComfyUI installed?"}

    # Build destination path
    dest_dir = os.path.join(models_dir, folder_name)
    if subfolder:
        dest_dir = os.path.join(dest_dir, subfolder)

    # Create directory if needed
    os.makedirs(dest_dir, exist_ok=True)

    # Parse URL to determine download method
    hf_info = parse_hf_url(url)
    civitai_info = parse_civitai_url(url)

    if hf_info:
        return _download_from_huggingface(hf_info, dest_dir, filename, hf_token)
    elif civitai_info:
        return _download_from_civitai(civitai_info, dest_dir, filename)
    elif url.startswith("http://") or url.startswith("https://"):
        return _download_direct(url, dest_dir, filename)
    else:
        # Assume it's a HF repo shorthand
        hf_info = parse_hf_url(url)
        if hf_info:
            return _download_from_huggingface(hf_info, dest_dir, filename, hf_token)
        return {"error": f"Could not parse URL: {url}"}


def _download_from_huggingface(hf_info: dict, dest_dir: str, filename: str = None, hf_token: str = None) -> dict:
    """Download a model from Hugging Face."""
    import subprocess
    import shutil
    import os

    repo = hf_info["repo"]
    filepath = hf_info.get("filepath")

    # Check if huggingface-cli is available
    hf_cli = shutil.which("huggingface-cli")
    if not hf_cli:
        # Try common locations
        common_paths = [
            os.path.expanduser("~/.local/bin/huggingface-cli"),
            "/usr/local/bin/huggingface-cli",
            "/opt/homebrew/bin/huggingface-cli",
        ]
        for path in common_paths:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                hf_cli = path
                break

    if not hf_cli:
        return {
            "error": "huggingface-cli not found",
            "instructions": "Install with: pip install huggingface_hub[cli]"
        }

    # Build command
    cmd = [hf_cli, "download", repo]

    if filepath:
        cmd.append(filepath)

    cmd.extend(["--local-dir", dest_dir])

    # Add token if provided
    if hf_token:
        cmd.extend(["--token", hf_token])

    # Run download
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode == 0:
            # Determine the downloaded file path
            if filepath:
                downloaded_file = os.path.join(dest_dir, os.path.basename(filepath))
            else:
                downloaded_file = dest_dir

            return {
                "status": "success",
                "repo": repo,
                "filepath": filepath,
                "destination": downloaded_file,
                "message": f"Downloaded to {downloaded_file}"
            }
        else:
            stderr = result.stderr.lower()
            # Check for auth errors
            if "401" in stderr or "403" in stderr or "gated" in stderr or "access" in stderr:
                return {
                    "error": "gated_model",
                    "repo": repo,
                    "message": "This model requires authentication. Please provide your Hugging Face token.",
                    "instructions": "Get your token from https://huggingface.co/settings/tokens (read access is sufficient). Then call this tool again with hf_token parameter.",
                    "accept_url": f"https://huggingface.co/{repo}"
                }
            return {
                "error": "download_failed",
                "message": result.stderr or result.stdout or "Unknown error"
            }

    except subprocess.TimeoutExpired:
        return {"error": "Download timed out after 10 minutes"}
    except Exception as e:
        return {"error": f"Download failed: {str(e)}"}


def _download_from_civitai(civitai_info: dict, dest_dir: str, filename: str = None) -> dict:
    """Download a model from CivitAI."""
    import subprocess
    import os

    # If we have a direct download URL
    if civitai_info.get("download_url"):
        download_url = civitai_info["download_url"]
    elif civitai_info.get("model_version_id"):
        download_url = f"https://civitai.com/api/download/models/{civitai_info['model_version_id']}"
    elif civitai_info.get("model_id"):
        # Need to fetch the model info to get download URL
        return {
            "error": "civitai_model_page",
            "model_id": civitai_info["model_id"],
            "message": "This is a CivitAI model page URL. Please provide the direct download URL.",
            "instructions": f"Go to https://civitai.com/models/{civitai_info['model_id']}, click Download, and copy the direct download URL."
        }
    else:
        return {"error": "Could not parse CivitAI URL"}

    return _download_direct(download_url, dest_dir, filename, source="civitai")


def _download_direct(url: str, dest_dir: str, filename: str = None, source: str = "direct") -> dict:
    """Download a file directly via wget or curl."""
    import subprocess
    import shutil
    import os
    import re

    # Determine filename
    if not filename:
        # Try to extract from URL
        url_path = url.split("?")[0]  # Remove query params
        filename = os.path.basename(url_path)
        if not filename or "." not in filename:
            filename = "downloaded_model.safetensors"

    dest_path = os.path.join(dest_dir, filename)

    # Check if already exists
    if os.path.exists(dest_path):
        return {
            "status": "exists",
            "destination": dest_path,
            "message": f"File already exists at {dest_path}"
        }

    # Try wget first, then curl
    wget = shutil.which("wget")
    curl = shutil.which("curl")

    if wget:
        cmd = [wget, "-O", dest_path, "--progress=bar:force", url]
    elif curl:
        cmd = [curl, "-L", "-o", dest_path, "--progress-bar", url]
    else:
        # Fallback to Python urllib
        return _download_with_urllib(url, dest_path)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout for large files
        )

        if result.returncode == 0 and os.path.exists(dest_path):
            file_size = os.path.getsize(dest_path)
            return {
                "status": "success",
                "source": source,
                "destination": dest_path,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "message": f"Downloaded to {dest_path}"
            }
        else:
            # Clean up partial file
            if os.path.exists(dest_path):
                os.remove(dest_path)
            return {
                "error": "download_failed",
                "message": result.stderr or result.stdout or "Unknown error"
            }

    except subprocess.TimeoutExpired:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return {"error": "Download timed out after 30 minutes"}
    except Exception as e:
        return {"error": f"Download failed: {str(e)}"}


def _download_with_urllib(url: str, dest_path: str) -> dict:
    """Fallback download using urllib."""
    import os

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ComfyUI-Model-Downloader"})
        with urllib.request.urlopen(req, timeout=1800) as response:
            with open(dest_path, "wb") as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)

        file_size = os.path.getsize(dest_path)
        return {
            "status": "success",
            "destination": dest_path,
            "size_mb": round(file_size / (1024 * 1024), 2),
            "message": f"Downloaded to {dest_path}"
        }

    except urllib.error.HTTPError as e:
        return {"error": f"HTTP error: {e.code} {e.reason}"}
    except Exception as e:
        return {"error": f"Download failed: {str(e)}"}


# MCP Protocol Implementation
def send_response(response: dict):
    """Send a JSON-RPC response."""
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()


def handle_request(request: dict) -> dict:
    """Handle an MCP request."""
    method = request.get("method", "")
    params = request.get("params", {})
    request_id = request.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": "comfyui-mcp",
                    "version": "1.0.0"
                },
                "capabilities": {
                    "tools": {}
                }
            }
        }

    elif method == "notifications/initialized":
        # No response needed for notifications
        return None

    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {
                        "name": "get_workflow",
                        "description": "Get the current workflow from ComfyUI. Returns full node graph with all nodes, connections, and widget values. Use summarize_workflow for a lighter overview.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": []
                        }
                    },
                    {
                        "name": "summarize_workflow",
                        "description": "Get a concise summary of the current workflow: node IDs, types, titles, positions, and connections. Lighter than get_workflow.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": []
                        }
                    },
                    {
                        "name": "get_node_types",
                        "description": "Search available node types. Returns minimal info by default. Use 'fields' for more details.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "search": {
                                    "oneOf": [
                                        {"type": "string"},
                                        {"type": "array", "items": {"type": "string"}}
                                    ],
                                    "description": "Search term(s). Array for multiple: [\"camera\", \"sampler\"]"
                                },
                                "category": {
                                    "type": "string",
                                    "description": "Filter by category (e.g., 'loaders', 'sampling')"
                                },
                                "fields": {
                                    "type": "array",
                                    "items": {"type": "string", "enum": ["inputs", "outputs", "description", "input_types", "output_types"]},
                                    "description": "Extra fields to include"
                                }
                            },
                            "required": []
                        }
                    },
                    {
                        "name": "get_node_info",
                        "description": "Get detailed info about a specific node in the workflow: type, properties, inputs, outputs, widget values.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {"type": "string", "description": "Node ID"}
                            },
                            "required": ["node_id"]
                        }
                    },
                    {
                        "name": "get_status",
                        "description": "Get ComfyUI status: queue, system stats, and/or history. Returns lightweight summaries by default (counts, IDs). Use detail='full' for more info. History is always paginated.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "include": {
                                    "type": "array",
                                    "items": {"type": "string", "enum": ["queue", "system", "history"]},
                                    "description": "What to include. Default: [\"queue\", \"system\"]"
                                },
                                "detail": {
                                    "type": "string",
                                    "enum": ["summary", "full"],
                                    "description": "\"summary\" (default): counts and IDs only. \"full\": includes output summaries and execution times."
                                },
                                "history_limit": {
                                    "type": "integer",
                                    "description": "Max history entries to return (default 5, max 20). Use with history_offset for pagination."
                                },
                                "history_offset": {
                                    "type": "integer",
                                    "description": "Skip this many entries from most recent (default 0). Use for pagination."
                                }
                            },
                            "required": []
                        }
                    },
                    {
                        "name": "run",
                        "description": "Run workflow or interrupt current generation.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": ["queue", "interrupt"],
                                    "description": "\"queue\" to run, \"interrupt\" to stop. Default: queue"
                                },
                                "node_ids": {
                                    "oneOf": [
                                        {"type": "string"},
                                        {"type": "array", "items": {"type": "string"}}
                                    ],
                                    "description": "Optional: validate these nodes exist before running"
                                }
                            },
                            "required": []
                        }
                    },
                    {
                        "name": "edit_graph",
                        "description": "Edit workflow graph with batched operations. Actions: create, delete, move, resize, set, connect, disconnect. Operations execute in order; 'create' returns node_id for chaining.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "operations": {
                                    "oneOf": [
                                        {"type": "object"},
                                        {"type": "array", "items": {"type": "object"}}
                                    ],
                                    "description": "Operation(s). Each has 'action' + params. Actions: create {node_type, pos_x, pos_y, title, ref, place_in_view}, delete {node_id or node_ids}, move {node_id, x, y} or {node_id, relative_to, direction, gap}, resize {node_id, width, height}, set {node_id, property, value} or {node_id, properties: {k:v}}, connect/disconnect {from_node, from_slot, to_node, to_slot}. Use 'ref' in create to reference node in later ops. Use 'place_in_view: true' to position new nodes at the center of the user's current viewport (nodes are offset horizontally to avoid overlap)."
                                }
                            },
                            "required": ["operations"]
                        }
                    },
                    {
                        "name": "view_image",
                        "description": "View an image from a Preview Image or Save Image node. Returns the image as base64 so you can see it. Run the workflow first to generate images.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {
                                    "type": "string",
                                    "description": "ID of the Preview Image or Save Image node. If not provided, uses the first image node found."
                                },
                                "image_index": {
                                    "type": "integer",
                                    "description": "Which image to view if node has multiple (0-based). Default: 0"
                                }
                            },
                            "required": []
                        }
                    },
                    {
                        "name": "center_on_node",
                        "description": "Center the user's viewport on a specific node. Useful after creating nodes to show the user where they were placed.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {
                                    "type": "string",
                                    "description": "ID of the node to center the view on."
                                }
                            },
                            "required": ["node_id"]
                        }
                    },
                    {
                        "name": "search_custom_nodes",
                        "description": "Search for custom nodes in the ComfyUI Manager registry. Returns name, author, description, install status, and star count.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Search term (matches name, description, author). Case-insensitive."
                                },
                                "status": {
                                    "type": "string",
                                    "enum": ["all", "installed", "not-installed"],
                                    "description": "Filter by installation status. Default: all"
                                },
                                "category": {
                                    "type": "string",
                                    "description": "Filter by category (e.g., 'animation', '3d', 'video')"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Maximum results to return. Default: 10"
                                }
                            },
                            "required": []
                        }
                    },
                    {
                        "name": "install_custom_node",
                        "description": "Install a custom node via ComfyUI Manager. Requires restart to complete.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {
                                    "type": "string",
                                    "description": "Node ID, name, or git URL to install"
                                }
                            },
                            "required": ["node_id"]
                        }
                    },
                    {
                        "name": "uninstall_custom_node",
                        "description": "Uninstall a custom node via ComfyUI Manager. Requires restart to complete.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {
                                    "type": "string",
                                    "description": "Node ID or name to uninstall"
                                }
                            },
                            "required": ["node_id"]
                        }
                    },
                    {
                        "name": "update_custom_node",
                        "description": "Update a custom node to the latest version via ComfyUI Manager. Requires restart to complete.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "node_id": {
                                    "type": "string",
                                    "description": "Node ID or name to update"
                                }
                            },
                            "required": ["node_id"]
                        }
                    },
                    {
                        "name": "download_model",
                        "description": "Download a model to the ComfyUI models folder. Supports Hugging Face, CivitAI, and direct URLs. For gated HF models, will return instructions to provide a token.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "url": {
                                    "type": "string",
                                    "description": "Model URL: HF (https://huggingface.co/user/repo/...), CivitAI (https://civitai.com/...), direct URL, or HF shorthand (user/repo/file.safetensors)"
                                },
                                "model_type": {
                                    "type": "string",
                                    "enum": ["checkpoint", "lora", "vae", "controlnet", "clip", "clip_vision", "unet", "diffusion_models", "text_encoders", "upscale_models", "embeddings", "hypernetworks", "ipadapter", "instantid", "insightface", "pulid", "animatediff"],
                                    "description": "Model type - determines destination folder in ComfyUI/models/"
                                },
                                "filename": {
                                    "type": "string",
                                    "description": "Optional: Override the filename. Auto-detected from URL if not provided."
                                },
                                "hf_token": {
                                    "type": "string",
                                    "description": "Hugging Face token for gated models. Only provide if download fails with auth error."
                                },
                                "subfolder": {
                                    "type": "string",
                                    "description": "Optional: Subfolder within the model type directory"
                                }
                            },
                            "required": ["url", "model_type"]
                        }
                    }
                ]
            }
        }

    elif method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments", {})

        try:
            result = None

            # New consolidated tools
            if tool_name == "get_workflow":
                result = get_workflow()
            elif tool_name == "summarize_workflow":
                result = summarize_workflow()
            elif tool_name == "get_node_types":
                result = get_node_types(
                    search=tool_args.get("search"),
                    category=tool_args.get("category"),
                    fields=tool_args.get("fields")
                )
            elif tool_name == "get_node_info":
                result = get_node_info(tool_args.get("node_id", ""))
            elif tool_name == "get_status":
                result = get_status(
                    include=tool_args.get("include"),
                    detail=tool_args.get("detail", "summary"),
                    history_limit=tool_args.get("history_limit", 5),
                    history_offset=tool_args.get("history_offset", 0)
                )
            elif tool_name == "run":
                result = run(
                    action=tool_args.get("action", "queue"),
                    node_ids=tool_args.get("node_ids")
                )
            elif tool_name == "edit_graph":
                result = edit_graph(tool_args.get("operations", []))
            elif tool_name == "view_image":
                result = view_image(
                    node_id=tool_args.get("node_id"),
                    image_index=tool_args.get("image_index", 0)
                )
            elif tool_name == "center_on_node":
                result = center_on_node(tool_args.get("node_id", ""))

            # Legacy tools (keep for backwards compatibility)
            elif tool_name == "get_queue":
                result = get_queue()
            elif tool_name == "get_system_stats":
                result = get_system_stats()
            elif tool_name == "get_history":
                result = get_history(tool_args.get("prompt_id"))
            elif tool_name == "interrupt":
                result = interrupt_generation()
            elif tool_name == "run_node":
                result = run_node(tool_args.get("node_ids", ""))
            elif tool_name == "create_node":
                result = create_node(tool_args.get("nodes", {}))
            elif tool_name == "delete_nodes":
                result = delete_nodes(tool_args.get("node_ids", ""))
            elif tool_name == "set_node_property":
                result = set_node_property(tool_args.get("properties", {}))
            elif tool_name == "connect_nodes":
                result = connect_nodes(tool_args.get("connections", {}))
            elif tool_name == "disconnect_nodes":
                result = disconnect_nodes(tool_args.get("disconnections", {}))
            elif tool_name == "move_nodes":
                result = move_nodes(tool_args.get("moves", {}))

            # Node management tools
            elif tool_name == "search_custom_nodes":
                result = search_custom_nodes(
                    query=tool_args.get("query"),
                    status=tool_args.get("status", "all"),
                    category=tool_args.get("category"),
                    limit=tool_args.get("limit", 10)
                )
            elif tool_name == "install_custom_node":
                result = install_custom_node(tool_args.get("node_id", ""))
            elif tool_name == "uninstall_custom_node":
                result = uninstall_custom_node(tool_args.get("node_id", ""))
            elif tool_name == "update_custom_node":
                result = update_custom_node(tool_args.get("node_id", ""))

            # Model download
            elif tool_name == "download_model":
                result = download_model(
                    url=tool_args.get("url", ""),
                    model_type=tool_args.get("model_type", ""),
                    filename=tool_args.get("filename"),
                    hf_token=tool_args.get("hf_token"),
                    subfolder=tool_args.get("subfolder")
                )

            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Unknown tool: {tool_name}"
                    }
                }
        except Exception as e:
            # Return error as tool result instead of crashing
            result = {"error": f"Tool execution failed: {type(e).__name__}: {e}"}

        # Handle image results specially - return as image content type
        if tool_name == "view_image" and "base64_data" in result:
            # Extract image data and return as image content
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Image from node {result.get('node_id')} ({result.get('node_title')}): {result.get('filename')}"
                        },
                        {
                            "type": "image",
                            "data": result["base64_data"],
                            "mimeType": result.get("media_type", "image/png")
                        }
                    ]
                }
            }

        # If result is already a string (e.g., TOON-like format), use it directly
        # Otherwise JSON-serialize it
        if isinstance(result, str):
            text_content = result
        else:
            text_content = json.dumps(result, indent=2)

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": text_content
                    }
                ]
            }
        }

    else:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}"
            }
        }


def main():
    """Main loop - read JSON-RPC requests from stdin, write responses to stdout."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        request_id = None
        try:
            request = json.loads(line)
            request_id = request.get("id")
            response = handle_request(request)
            if response:  # Don't send response for notifications
                send_response(response)
        except json.JSONDecodeError as e:
            send_response({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32700,
                    "message": f"Parse error: {e}"
                }
            })
        except Exception as e:
            # Catch any unhandled exceptions to prevent MCP connection from closing
            send_response({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": f"Internal error: {type(e).__name__}: {e}"
                }
            })


if __name__ == "__main__":
    main()
