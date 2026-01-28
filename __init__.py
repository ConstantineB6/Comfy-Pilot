# ComfyUI Claude Code Plugin
# A floating window extension for Claude Code integration

import asyncio
import json
import os
import pty
import select
import struct
import fcntl
import termios
import signal
import hashlib
import resource
import sys
from pathlib import Path
from aiohttp import web

WEB_DIRECTORY = "./js"

NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]


def has_claude_conversation(working_dir=None):
    """Check if there's an existing Claude conversation for the given directory."""
    if working_dir is None:
        working_dir = os.getcwd()

    # Claude stores projects in ~/.claude/projects/<path-with-dashes>/
    # e.g., /Users/const/projects/foo -> -Users-const-projects-foo
    claude_dir = Path.home() / ".claude" / "projects"

    if not claude_dir.exists():
        return False

    # Convert path to Claude's folder naming format
    abs_path = os.path.abspath(working_dir)
    folder_name = abs_path.replace("/", "-")

    project_dir = claude_dir / folder_name

    if not project_dir.exists():
        return False

    # Check for conversation JSONL files
    conversation_files = list(project_dir.glob("*.jsonl"))
    return len(conversation_files) > 0


def find_executable(name):
    """Find an executable, checking common paths if not in PATH."""
    import shutil

    # First try the standard PATH
    path = shutil.which(name)
    if path:
        return path

    # Common locations for npm/node/claude on different systems
    common_paths = [
        # macOS Homebrew
        f"/opt/homebrew/bin/{name}",
        f"/usr/local/bin/{name}",
        # Linux common paths
        f"/usr/bin/{name}",
        f"/usr/local/bin/{name}",
        # nvm default location
        os.path.expanduser(f"~/.nvm/versions/node/*/bin/{name}"),
        # npm global installs
        os.path.expanduser(f"~/.npm-global/bin/{name}"),
        os.path.expanduser(f"~/node_modules/.bin/{name}"),
        # n (node version manager)
        f"/usr/local/n/versions/node/*/bin/{name}",
        # Conda
        os.path.expanduser(f"~/anaconda3/bin/{name}"),
        os.path.expanduser(f"~/miniconda3/bin/{name}"),
        f"/opt/conda/bin/{name}",
    ]

    import glob
    for pattern in common_paths:
        matches = glob.glob(pattern)
        if matches:
            # Return the first match (or latest version for nvm-style paths)
            matches.sort(reverse=True)
            if os.path.isfile(matches[0]) and os.access(matches[0], os.X_OK):
                return matches[0]

    return None


def is_claude_installed():
    """Check if claude CLI is installed."""
    return find_executable("claude") is not None


def install_claude_code():
    """Attempt to install Claude Code CLI. Returns (success, message)."""
    import subprocess

    # Check if npm is available
    npm_path = find_executable("npm")
    if npm_path is None:
        return False, "npm not found. Please install Node.js first: https://nodejs.org/"

    try:
        print(f"[Claude Code] Installing Claude Code CLI using {npm_path}...")
        result = subprocess.run(
            [npm_path, "install", "-g", "@anthropic-ai/claude-code"],
            capture_output=True,
            text=True,
            timeout=120
        )
        if result.returncode == 0:
            print("[Claude Code] Claude Code CLI installed successfully!")
            return True, "Claude Code CLI installed successfully!"
        else:
            error_msg = result.stderr or result.stdout or "Unknown error"
            print(f"[Claude Code] Installation failed: {error_msg}")
            return False, f"Installation failed: {error_msg}"
    except subprocess.TimeoutExpired:
        return False, "Installation timed out after 120 seconds"
    except Exception as e:
        return False, f"Installation error: {str(e)}"


def get_claude_command(working_dir=None):
    """Get the appropriate claude command based on whether a conversation exists."""
    if has_claude_conversation(working_dir):
        return "claude -c"
    else:
        return "claude"


class WebSocketTerminal:
    """Manages a PTY session connected via WebSocket."""

    def __init__(self):
        self.fd = None
        self.pid = None
        self.websocket = None
        self.read_thread = None
        self.running = False
        self._decoder = None  # UTF-8 incremental decoder

    def spawn(self, command=None):
        """Spawn a new PTY with an optional command."""
        # Get the user's default shell
        shell = os.environ.get("SHELL", "/bin/bash")

        # Fork a new PTY
        self.pid, self.fd = pty.fork()

        if self.pid == 0:
            # Child process
            env = os.environ.copy()
            env["TERM"] = "xterm-256color"
            env["COLORTERM"] = "truecolor"

            if command:
                # Execute as interactive login shell with command
                # -l = login shell (loads profile), -i = interactive, -c = command
                os.execlpe(shell, shell, "-l", "-i", "-c", command, env)
            else:
                # Execute the shell as a login shell
                shell_name = os.path.basename(shell)
                os.execlpe(shell, f"-{shell_name}", env)
        else:
            # Parent process - set non-blocking mode for async reads
            import fcntl
            flags = fcntl.fcntl(self.fd, fcntl.F_GETFL)
            fcntl.fcntl(self.fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            self.running = True
            return True

    def resize(self, rows, cols):
        """Resize the PTY and notify the child process."""
        if self.fd:
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(self.fd, termios.TIOCSWINSZ, winsize)
            # Send SIGWINCH to notify the child process of the size change
            if self.pid:
                try:
                    os.kill(self.pid, signal.SIGWINCH)
                except OSError:
                    pass

    def write(self, data):
        """Write data to the PTY."""
        if self.fd:
            os.write(self.fd, data.encode("utf-8"))

    def read_nonblock(self):
        """Non-blocking read from PTY, returns None if no data available."""
        if self.fd:
            try:
                data = os.read(self.fd, 4096)
                if data:
                    # Use incremental decoder to handle partial UTF-8 sequences
                    if self._decoder is None:
                        import codecs
                        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                    return self._decoder.decode(data)
            except BlockingIOError:
                # No data available
                return None
            except (OSError, IOError):
                self.running = False
        return None

    def read_blocking(self):
        """Blocking read with short select timeout for use with run_in_executor."""
        if self.fd:
            try:
                ready, _, _ = select.select([self.fd], [], [], 0.001)  # 1ms timeout
                if ready:
                    data = os.read(self.fd, 4096)
                    if self._decoder is None:
                        import codecs
                        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                    return self._decoder.decode(data)
            except (OSError, IOError):
                self.running = False
        return None

    def close(self):
        """Close the PTY."""
        self.running = False
        if self.fd:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None
        if self.pid:
            try:
                os.kill(self.pid, 9)
                os.waitpid(self.pid, 0)
            except (OSError, ChildProcessError):
                pass
            self.pid = None


# Global terminal sessions (keyed by websocket id)
terminal_sessions = {}

# Global storage for the current workflow (updated by frontend)
current_workflow = {"workflow": None, "timestamp": None}

# Pending graph commands to be executed by frontend
pending_commands = []
command_results = {}

# Memory logging
_last_memory_log = 0
MEMORY_LOG_INTERVAL = 60  # Log every 60 seconds at most


def get_memory_mb():
    """Get current memory usage in MB."""
    # ru_maxrss is in bytes on Linux, kilobytes on macOS
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return usage.ru_maxrss / (1024 * 1024)  # KB to MB
    else:
        return usage.ru_maxrss / 1024  # bytes to MB


def log_memory(context=""):
    """Log memory usage if enough time has passed since last log."""
    global _last_memory_log
    import time
    now = time.time()
    if now - _last_memory_log >= MEMORY_LOG_INTERVAL:
        _last_memory_log = now
        breakdown = get_plugin_memory_breakdown()
        print(f"[Claude Code] Plugin data: {breakdown['total_plugin_kb']:.1f}KB | Sessions: {breakdown['terminal_sessions']}" + (f" | {context}" if context else ""))


def get_plugin_memory_breakdown():
    """Get memory breakdown of plugin data structures."""
    workflow_size = len(json.dumps(current_workflow)) if current_workflow.get("workflow") else 0
    commands_size = len(json.dumps(pending_commands)) if pending_commands else 0
    results_size = len(json.dumps(command_results)) if command_results else 0

    return {
        "workflow_bytes": workflow_size,
        "pending_commands_bytes": commands_size,
        "command_results_bytes": results_size,
        "terminal_sessions": len(terminal_sessions),
        "total_plugin_kb": round((workflow_size + commands_size + results_size) / 1024, 2)
    }


async def memory_stats_handler(request):
    """Return current memory stats as JSON."""
    mem_mb = get_memory_mb()
    breakdown = get_plugin_memory_breakdown()

    return web.json_response({
        "process_memory_mb": round(mem_mb, 2),
        "note": "process_memory_mb is the entire ComfyUI process, not just this plugin",
        "plugin_data": breakdown
    })


async def workflow_handler(request):
    """Handle workflow GET/POST requests."""
    global current_workflow

    if request.method == "POST":
        # Frontend is sending the current workflow
        try:
            data = await request.json()
            current_workflow = {
                "workflow": data.get("workflow"),
                "workflow_api": data.get("workflow_api"),
                "timestamp": data.get("timestamp")
            }
            log_memory("workflow update")
            return web.json_response({"status": "ok"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)
    else:
        # GET - return the current workflow
        return web.json_response(current_workflow)


async def graph_command_handler(request):
    """Handle graph manipulation commands from MCP server."""
    global pending_commands, command_results

    if request.method == "GET":
        # Frontend polling for pending commands
        if pending_commands:
            cmd = pending_commands.pop(0)
            return web.json_response({"command": cmd})
        return web.json_response({"command": None})

    elif request.method == "POST":
        # MCP server sending a command or frontend returning result
        try:
            data = await request.json()

            if "result" in data:
                # Frontend returning command result
                cmd_id = data.get("command_id")
                command_results[cmd_id] = data.get("result")
                return web.json_response({"status": "ok"})

            # MCP server sending a new command
            import uuid
            cmd_id = str(uuid.uuid4())
            cmd = {
                "id": cmd_id,
                "action": data.get("action"),
                "params": data.get("params", {})
            }
            pending_commands.append(cmd)

            # Wait for result (with timeout)
            import time
            start = time.time()
            while cmd_id not in command_results and time.time() - start < 5:
                await asyncio.sleep(0.1)

            if cmd_id in command_results:
                result = command_results.pop(cmd_id)
                return web.json_response(result)
            else:
                return web.json_response({"error": "Timeout waiting for frontend to execute command"}, status=504)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.json_response({"error": str(e)}, status=500)


async def run_node_handler(request):
    """Run the workflow up to a specific node."""
    try:
        data = await request.json()
        node_id = data.get("node_id")

        if not node_id:
            return web.json_response({"error": "node_id is required"}, status=400)

        # Get the current workflow API format
        if not current_workflow.get("workflow_api"):
            return web.json_response({"error": "No workflow available. Make sure ComfyUI is open in browser."}, status=400)

        workflow_api = current_workflow["workflow_api"]

        # The workflow_api has 'output' and 'workflow' keys
        prompt = workflow_api.get("output", workflow_api)

        # Convert node_id to string for comparison
        node_id_str = str(node_id)

        if node_id_str not in prompt:
            return web.json_response({"error": f"Node {node_id} not found in workflow"}, status=400)

        # Queue the prompt via ComfyUI's prompt queue
        from execution import PromptQueue
        from server import PromptServer
        import uuid

        prompt_id = str(uuid.uuid4())

        # Queue using the prompt_queue.put method
        PromptServer.instance.prompt_queue.put(
            (0, prompt_id, prompt, {"client_id": "claude-code"}, [node_id_str])
        )

        return web.json_response({
            "status": "queued",
            "prompt_id": prompt_id,
            "node_id": node_id_str
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return web.json_response({"error": str(e)}, status=500)


async def websocket_handler(request):
    """Handle WebSocket connections for terminal sessions."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_id = id(ws)
    terminal = WebSocketTerminal()
    terminal_sessions[session_id] = terminal
    terminal_started = False
    initial_rows = 24
    initial_cols = 80

    print(f"[Claude Code] WebSocket connected: {session_id}")
    log_memory("ws connect")

    # Get command from query params, or auto-detect
    command = request.query.get("cmd", None)
    if command is None:
        command = get_claude_command()
        print(f"[Claude Code] Auto-detected command: {command}")

    # Check if claude is installed, auto-install if needed
    if not is_claude_installed():
        print("[Claude Code] Claude CLI not found, attempting auto-install...")
        await ws.send_str("o\x1b[33mClaude Code CLI not found. Attempting to install...\x1b[0m\r\n")

        # Run installation in a thread to not block
        loop = asyncio.get_event_loop()
        success, message = await loop.run_in_executor(None, install_claude_code)

        if success:
            await ws.send_str("o\x1b[32m" + message + "\x1b[0m\r\n\r\n")
        else:
            await ws.send_str("o\x1b[31m" + message + "\x1b[0m\r\n")
            await ws.send_str("o\r\nTo install manually, run:\r\n")
            await ws.send_str("o\x1b[36m  npm install -g @anthropic-ai/claude-code\x1b[0m\r\n")
            await ws.close()
            del terminal_sessions[session_id]
            return ws

    async def read_pty():
        """Read from PTY and send to WebSocket."""
        loop = asyncio.get_event_loop()
        fd = terminal.fd
        read_event = asyncio.Event()
        pending_data = []

        def on_readable():
            """Called by event loop when fd has data."""
            try:
                data = terminal.read_nonblock()
                if data:
                    pending_data.append(data)
                    read_event.set()
            except Exception as e:
                print(f"[Claude Code] Read callback error: {e}")

        loop.add_reader(fd, on_readable)

        try:
            while terminal.running and not ws.closed:
                await read_event.wait()
                read_event.clear()
                # Send all pending data
                while pending_data:
                    data = pending_data.pop(0)
                    await ws.send_str("o" + data)
        except Exception as e:
            print(f"[Claude Code] Read error: {e}")
        finally:
            try:
                loop.remove_reader(fd)
            except:
                pass

    read_task = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    msg_type = data.get("type")

                    if msg_type == "i":
                        # Fast path for input
                        terminal.write(data.get("d", ""))
                    elif msg_type == "input":
                        # Legacy format
                        terminal.write(data.get("data", ""))
                    elif msg_type == "resize":
                        rows = data.get("rows", 24)
                        cols = data.get("cols", 80)

                        if not terminal_started:
                            # First resize - now start the terminal with correct size
                            initial_rows = rows
                            initial_cols = cols
                            terminal.spawn(command)
                            terminal.resize(rows, cols)
                            terminal_started = True
                            # Start reading task
                            read_task = asyncio.create_task(read_pty())
                            print(f"[Claude Code] Terminal started with size {cols}x{rows}")
                        else:
                            terminal.resize(rows, cols)
                except json.JSONDecodeError:
                    pass
            elif msg.type == web.WSMsgType.ERROR:
                print(f"[Claude Code] WebSocket error: {ws.exception()}")
                break
    finally:
        terminal.running = False
        if read_task:
            read_task.cancel()
        terminal.close()
        del terminal_sessions[session_id]
        print(f"[Claude Code] WebSocket disconnected: {session_id}")
        log_memory("ws disconnect")

    return ws


async def mcp_status_handler(request):
    """Check if the MCP server is available (lightweight check)."""
    try:
        plugin_dir = os.path.dirname(os.path.abspath(__file__))
        mcp_server_path = os.path.join(plugin_dir, "mcp_server.py")

        # Just check if the file exists and is readable - no subprocess
        if os.path.isfile(mcp_server_path):
            return web.json_response({
                "connected": True,
                "tools": 15  # Known tool count
            })
        else:
            return web.json_response({
                "connected": False,
                "error": "MCP server file not found"
            })

    except Exception as e:
        return web.json_response({
            "connected": False,
            "error": str(e)
        })


def setup_routes(app):
    """Set up the WebSocket and API routes."""
    app.router.add_get("/ws/claude-terminal", websocket_handler)
    app.router.add_get("/claude-code/workflow", workflow_handler)
    app.router.add_post("/claude-code/workflow", workflow_handler)
    app.router.add_post("/claude-code/run-node", run_node_handler)
    app.router.add_get("/claude-code/graph-command", graph_command_handler)
    app.router.add_post("/claude-code/graph-command", graph_command_handler)
    app.router.add_get("/claude-code/mcp-status", mcp_status_handler)
    app.router.add_get("/claude-code/memory", memory_stats_handler)
    print("[Claude Code] Terminal WebSocket endpoint registered at /ws/claude-terminal")
    print("[Claude Code] Workflow API endpoint registered at /claude-code/workflow")
    print("[Claude Code] Run node endpoint registered at /claude-code/run-node")
    print("[Claude Code] Graph command endpoint registered at /claude-code/graph-command")
    print("[Claude Code] MCP status endpoint registered at /claude-code/mcp-status")
    print("[Claude Code] Memory stats endpoint registered at /claude-code/memory")


def write_comfyui_url():
    """Write the ComfyUI server URL to a file for the MCP server to read."""
    import os
    from pathlib import Path

    plugin_dir = os.path.dirname(os.path.abspath(__file__))
    url_file = os.path.join(plugin_dir, ".comfyui_url")

    # Get the server address from PromptServer
    try:
        from server import PromptServer
        address = PromptServer.instance.address
        port = PromptServer.instance.port
        url = f"http://{address}:{port}"
        with open(url_file, "w") as f:
            f.write(url)
        print(f"[Claude Code] ComfyUI URL written to {url_file}: {url}")
    except Exception as e:
        # Fallback to default
        with open(url_file, "w") as f:
            f.write("http://127.0.0.1:8188")
        print(f"[Claude Code] Using default ComfyUI URL")


def setup_mcp_config():
    """Set up MCP server configuration for Claude Code using claude mcp add."""
    import os
    import subprocess
    import shutil

    # Get the directory where this plugin is installed
    plugin_dir = os.path.dirname(os.path.abspath(__file__))
    mcp_server_path = os.path.join(plugin_dir, "mcp_server.py")

    # Find the Python executable - use the same one running this code
    python_path = sys.executable

    # Check if claude is available
    claude_path = shutil.which("claude")
    if not claude_path:
        print("[Claude Code] 'claude' command not found - MCP server not configured")
        return

    # Check if MCP server is already configured
    try:
        result = subprocess.run(
            [claude_path, "mcp", "get", "comfyui"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("[Claude Code] MCP server 'comfyui' already configured")
            return
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Add MCP server using claude mcp add
    # Use full paths for both python and mcp_server.py
    try:
        result = subprocess.run(
            [claude_path, "mcp", "add", "comfyui", python_path, mcp_server_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            print(f"[Claude Code] MCP server added: {python_path} {mcp_server_path}")
        else:
            print(f"[Claude Code] Failed to add MCP server: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("[Claude Code] Timeout adding MCP server")
    except FileNotFoundError:
        print("[Claude Code] 'claude' command not found - MCP server not configured")


# Hook into ComfyUI's server setup
try:
    from server import PromptServer

    # Register our WebSocket route
    setup_routes(PromptServer.instance.app)

    # Write ComfyUI URL for MCP server
    write_comfyui_url()

    # Set up MCP configuration
    setup_mcp_config()

    # Log initial memory usage
    mem_mb = get_memory_mb()
    print(f"[Claude Code] Plugin loaded successfully (Memory: {mem_mb:.1f}MB)")
except Exception as e:
    print(f"[Claude Code] Failed to register routes: {e}")
