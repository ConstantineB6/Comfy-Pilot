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


def get_node_types(search = None, category: str = None, fields: list = None) -> dict:
    """Get available node types in ComfyUI, optionally filtered.

    Args:
        search: Search term(s) - string or list of strings
        category: Category name to filter by
        fields: Optional list of fields to include. By default only returns minimal info.
                Available fields: "inputs", "outputs", "description", "input_types", "output_types"
    """
    all_nodes = get_object_info_cached()

    if "error" in all_nodes:
        return all_nodes

    # Helper to create minimal node info
    def minimal_node_info(node_name: str, node_info: dict) -> dict:
        result = {
            "display_name": node_info.get("display_name") or node_name,
            "category": node_info.get("category", "uncategorized")
        }

        # Add requested fields
        if fields:
            if "description" in fields:
                result["description"] = node_info.get("description") or ""
            if "inputs" in fields:
                result["inputs"] = node_info.get("input", {})
            if "outputs" in fields:
                result["outputs"] = node_info.get("output", [])
            if "input_types" in fields:
                # Just the type names for inputs
                input_info = node_info.get("input", {})
                input_types = {}
                for group in ["required", "optional"]:
                    if group in input_info:
                        for inp_name, inp_def in input_info[group].items():
                            if isinstance(inp_def, list) and len(inp_def) > 0:
                                input_types[inp_name] = inp_def[0] if isinstance(inp_def[0], str) else type(inp_def[0]).__name__
                result["input_types"] = input_types
            if "output_types" in fields:
                result["output_types"] = node_info.get("output", [])

        return result

    # If no filters, return just a summary list of node names grouped by category
    if not search and not category:
        categories = {}
        for node_name, node_info in all_nodes.items():
            cat = node_info.get("category", "uncategorized")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(node_name)

        # Sort categories and nodes within them
        sorted_categories = {}
        for cat in sorted(categories.keys()):
            sorted_categories[cat] = sorted(categories[cat])

        return {
            "total_nodes": len(all_nodes),
            "categories": sorted_categories,
            "hint": "Use 'search' parameter to find specific nodes, or 'category' to list nodes in a category."
        }

    # Helper function to search for a single term
    def search_nodes(term: str) -> dict:
        term_lower = term.lower()
        filtered = {}
        for node_name, node_info in all_nodes.items():
            display_name = node_info.get("display_name") or ""
            description = node_info.get("description") or ""
            if (term_lower in node_name.lower() or
                term_lower in display_name.lower() or
                term_lower in description.lower()):
                filtered[node_name] = minimal_node_info(node_name, node_info)
        return filtered

    # Filter by search term(s) - supports single string or array of strings
    if search:
        # Normalize to list
        search_terms = search if isinstance(search, list) else [search]

        if len(search_terms) == 1:
            # Single search - return flat result for backwards compatibility
            filtered = search_nodes(search_terms[0])
            return {
                "search": search_terms[0],
                "matches": len(filtered),
                "nodes": filtered
            }
        else:
            # Multiple searches - return grouped results
            results = {}
            for term in search_terms:
                filtered = search_nodes(term)
                results[term] = {
                    "matches": len(filtered),
                    "nodes": filtered
                }
            return {
                "searches": results,
                "total_searches": len(search_terms)
            }

    # Filter by category
    if category:
        category_lower = category.lower()
        filtered = {}
        for node_name, node_info in all_nodes.items():
            cat = node_info.get("category", "")
            if category_lower in cat.lower():
                filtered[node_name] = minimal_node_info(node_name, node_info)
        return {
            "category": category,
            "matches": len(filtered),
            "nodes": filtered
        }


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

def get_status(include: list = None, detail: str = "summary") -> dict:
    """Get status information from ComfyUI.

    Args:
        include: List of what to include: "queue", "system", "history".
                 Default is ["queue", "system"].
        detail: Level of detail - "summary" (default) or "full".
                Summary returns counts and IDs only. Full returns raw API data.
    """
    if include is None:
        include = ["queue", "system"]

    result = {}

    if "queue" in include:
        raw_queue = make_request("/queue")
        if "error" in raw_queue:
            result["queue"] = raw_queue
        elif detail == "full":
            result["queue"] = raw_queue
        else:
            # Summarize queue - just counts and prompt IDs
            running = raw_queue.get("queue_running", [])
            pending = raw_queue.get("queue_pending", [])
            result["queue"] = {
                "running": len(running),
                "pending": len(pending),
                "running_prompts": [item[1] if len(item) > 1 else None for item in running],
                "pending_prompts": [item[1] if len(item) > 1 else None for item in pending]
            }

    if "system" in include:
        result["system"] = make_request("/system_stats")

    if "history" in include:
        raw_history = make_request("/history")
        if "error" in raw_history:
            result["history"] = raw_history
        elif detail == "full":
            result["history"] = raw_history
        else:
            # Summarize history - list of prompt IDs with status and output node IDs
            history_summary = []
            for prompt_id, data in raw_history.items():
                if not isinstance(data, dict):
                    continue
                outputs = data.get("outputs", {})
                status = data.get("status", {})
                history_summary.append({
                    "prompt_id": prompt_id,
                    "status": status.get("status_str", "unknown"),
                    "completed": status.get("completed", False),
                    "output_nodes": list(outputs.keys()) if outputs else []
                })
            result["history"] = {
                "total": len(history_summary),
                "prompts": history_summary[-10:]  # Last 10 only
            }

    return result


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


def edit_graph(operations) -> dict:
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
        return all_nodes

    results = []
    created_nodes = {}  # Map temp refs to real node IDs

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
                    r = send_graph_command("create_node", {
                        "type": node_type,
                        "pos_x": op.get("pos_x", 100),
                        "pos_y": op.get("pos_y", 100),
                        "title": op.get("title")
                    })
                    result.update(r)
                    # Store created node ID for reference
                    if "node_id" in r:
                        ref = op.get("ref")
                        if ref:
                            created_nodes[ref] = r["node_id"]

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

    # Return single result for single op
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


def get_node_info(node_id: str) -> dict:
    """Get detailed info about a specific node in the workflow."""
    workflow_data = get_workflow()

    if "error" in workflow_data or "message" in workflow_data:
        return workflow_data

    workflow = workflow_data.get("workflow", {})
    node_id_str = str(node_id)
    node_id_int = int(node_id)

    # Handle graph serialize format
    if "nodes" in workflow:
        for node in workflow.get("nodes", []):
            if node.get("id") == node_id_int:
                # Get node type info for input/output details (cached)
                node_type = node.get("type")
                type_info = {}
                all_nodes = get_object_info_cached()
                if "error" not in all_nodes and node_type in all_nodes:
                    type_info = all_nodes[node_type]

                return {
                    "id": node.get("id"),
                    "type": node_type,
                    "title": node.get("title") or node_type,
                    "pos": node.get("pos"),
                    "size": node.get("size"),
                    "widgets_values": node.get("widgets_values"),
                    "inputs": node.get("inputs", []),
                    "outputs": node.get("outputs", []),
                    "type_info": {
                        "display_name": type_info.get("display_name"),
                        "description": type_info.get("description"),
                        "category": type_info.get("category"),
                        "input": type_info.get("input"),
                        "output": type_info.get("output"),
                        "output_name": type_info.get("output_name")
                    } if type_info else None
                }

        return {"error": f"Node {node_id} not found in workflow"}

    # Handle API format
    if node_id_str in workflow:
        node_data = workflow[node_id_str]
        return {
            "id": node_id_str,
            "type": node_data.get("class_type"),
            "inputs": node_data.get("inputs", {})
        }

    return {"error": f"Node {node_id} not found in workflow"}


def summarize_workflow() -> dict:
    """Get a human-readable summary of the current workflow."""
    workflow_data = get_workflow()

    if "error" in workflow_data or "message" in workflow_data:
        return workflow_data

    workflow = workflow_data.get("workflow", {})

    # Handle both formats - the graph serialize format and the API format
    nodes = []

    if "nodes" in workflow:
        # Graph serialize format
        for node in workflow.get("nodes", []):
            node_info = {
                "id": node.get("id"),
                "type": node.get("type"),
                "title": node.get("title") or node.get("type"),
                "pos": node.get("pos"),
            }

            # Get widget values if present
            if "widgets_values" in node:
                node_info["widgets"] = node.get("widgets_values")

            nodes.append(node_info)

        # Get links/connections
        links = workflow.get("links", [])
        connections = []
        for link in links:
            if len(link) >= 6:
                connections.append({
                    "link_id": link[0],
                    "from_node": link[1],
                    "from_slot": link[2],
                    "to_node": link[3],
                    "to_slot": link[4],
                    "type": link[5]
                })

        return {
            "total_nodes": len(nodes),
            "nodes": nodes,
            "total_connections": len(connections),
            "connections": connections
        }
    else:
        # API format (prompt format)
        for node_id, node_data in workflow.items():
            if isinstance(node_data, dict):
                node_info = {
                    "id": node_id,
                    "type": node_data.get("class_type"),
                    "inputs": node_data.get("inputs", {})
                }
                nodes.append(node_info)

        return {
            "total_nodes": len(nodes),
            "nodes": nodes
        }


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
    target_node_id = str(target_node["id"])
    image_info = None

    for prompt_id, prompt_data in history.items():
        if not isinstance(prompt_data, dict):
            continue
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
                        "description": "Get ComfyUI status: queue, system stats, and/or history. Returns lightweight summaries by default (counts, IDs). Use detail='full' for raw API data.",
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
                                    "description": "\"summary\" (default): counts and IDs only. \"full\": raw API data (can be large)."
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
                                    "description": "Operation(s). Each has 'action' + params. Actions: create {node_type, pos_x, pos_y, title, ref}, delete {node_id or node_ids}, move {node_id, x, y} or {node_id, relative_to, direction, gap}, resize {node_id, width, height}, set {node_id, property, value} or {node_id, properties: {k:v}}, connect/disconnect {from_node, from_slot, to_node, to_slot}. Use 'ref' in create to reference node in later ops."
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
                    detail=tool_args.get("detail", "summary")
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

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, indent=2)
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
