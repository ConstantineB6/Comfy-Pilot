import { app } from "../../scripts/app.js";

// Store reference to floating window globally for context menu access
let floatingWindow = null;
let terminal = null;
let fitAddon = null;
let websocket = null;
let claudeRunning = false;

app.registerExtension({
    name: "comfy.claude-code",

    async setup() {
        console.log("Claude Code extension loading...");

        // Load xterm.js
        await loadXtermDependencies();

        // Create floating window
        floatingWindow = createFloatingWindow();
        document.body.appendChild(floatingWindow);

        // Make it draggable
        makeDraggable(floatingWindow, floatingWindow.querySelector(".claude-header"));

        // Make it resizable from all edges
        makeResizable(floatingWindow);

        // Add toggle button to ComfyUI menu
        addMenuButton(floatingWindow);

        // Add context menu option
        addContextMenuOption();

        // Start workflow sync
        startWorkflowSync();

        console.log("Claude Code extension loaded");
    },
});

async function loadXtermDependencies() {
    // Load xterm.js CSS
    const xtermCss = document.createElement("link");
    xtermCss.rel = "stylesheet";
    xtermCss.href = "https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.css";
    document.head.appendChild(xtermCss);

    // Load xterm.js
    await loadScript("https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js");

    // Load xterm-addon-fit
    await loadScript("https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js");

    // Load xterm-addon-canvas for rendering (faster than DOM, more stable than WebGL)
    await loadScript("https://cdn.jsdelivr.net/npm/xterm-addon-canvas@0.5.0/lib/xterm-addon-canvas.min.js");

    // Load xterm-addon-unicode11 for proper unicode character width handling
    await loadScript("https://cdn.jsdelivr.net/npm/xterm-addon-unicode11@0.6.0/lib/xterm-addon-unicode11.min.js");
}

function loadScript(src) {
    return new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = src;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
    });
}


function createFloatingWindow() {
    const container = document.createElement("div");
    container.id = "claude-code-window";

    container.innerHTML = `
        <div class="claude-resize-edge claude-resize-n"></div>
        <div class="claude-resize-edge claude-resize-s"></div>
        <div class="claude-resize-edge claude-resize-e"></div>
        <div class="claude-resize-edge claude-resize-w"></div>
        <div class="claude-resize-corner claude-resize-nw"></div>
        <div class="claude-resize-corner claude-resize-ne"></div>
        <div class="claude-resize-corner claude-resize-sw"></div>
        <div class="claude-resize-corner claude-resize-se"></div>
        <div class="claude-header">
            <div class="claude-title-area">
                <span class="claude-title">Claude Code</span>
                <div class="claude-mcp-status" title="MCP Server Status">
                    <span class="mcp-indicator"></span>
                    <span class="mcp-label">MCP</span>
                </div>
            </div>
            <div class="claude-controls">
                <button class="claude-btn claude-reload" title="Reload Terminal">↻</button>
                <button class="claude-btn claude-minimize" title="Minimize">−</button>
                <button class="claude-btn claude-close" title="Close">×</button>
            </div>
        </div>
        <div class="claude-content">
            <div class="claude-terminal" id="claude-terminal"></div>
        </div>
    `;

    // Apply styles
    const style = document.createElement("style");
    style.textContent = `
        #claude-code-window {
            position: fixed;
            top: 100px;
            right: 20px;
            width: 950px;
            height: 600px;
            background-color: #0d0d0d;
            border: 1px solid #333;
            border-radius: 8px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.6);
            z-index: 10000;
            color: #e0e0e0;
            will-change: transform, left, top;
            contain: layout style;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            font-size: 13px;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        .claude-header {
            cursor: move;
            background: #1a1a1a;
            padding: 8px 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #333;
            user-select: none;
            flex-shrink: 0;
        }

        .claude-title-area {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .claude-title {
            font-weight: 600;
            font-size: 13px;
            color: #aaa;
        }

        .claude-mcp-status {
            display: flex;
            align-items: center;
            gap: 5px;
            padding: 3px 8px;
            border-radius: 4px;
            background: rgba(255, 255, 255, 0.05);
            cursor: pointer;
            transition: background 0.15s;
        }

        .claude-mcp-status:hover {
            background: rgba(255, 255, 255, 0.1);
        }

        .mcp-indicator {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #666;
            transition: background 0.3s;
        }

        .mcp-indicator.connected {
            background: #4ade80;
            box-shadow: 0 0 6px rgba(74, 222, 128, 0.5);
        }

        .mcp-indicator.disconnected {
            background: #f87171;
        }

        .mcp-indicator.checking {
            background: #fbbf24;
            animation: pulse 1s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .mcp-label {
            font-size: 11px;
            color: #888;
            font-weight: 500;
        }

        .claude-controls {
            display: flex;
            gap: 4px;
        }

        .claude-btn {
            background: transparent;
            border: none;
            color: #666;
            width: 24px;
            height: 24px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            line-height: 1;
            transition: all 0.15s;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .claude-btn:hover {
            background: rgba(255, 255, 255, 0.1);
            color: #fff;
        }

        .claude-close:hover {
            background: #e74c3c;
            color: #fff;
        }

        .claude-reload:hover {
            background: #27ae60;
            color: #fff;
        }

        .claude-minimize:hover {
            background: #f39c12;
            color: #fff;
        }

        /* Minimized state */
        #claude-code-window.minimized {
            height: auto !important;
            min-height: 0 !important;
        }

        #claude-code-window.minimized.collapsed-width {
            width: auto !important;
            min-width: 0 !important;
        }

        #claude-code-window.minimized .claude-content {
            display: none;
        }

        #claude-code-window.minimized .claude-resize-edge,
        #claude-code-window.minimized .claude-resize-corner {
            display: none;
        }

        #claude-code-window.minimized .claude-header {
            padding: 8px 10px;
        }

        #claude-code-window.minimized .mcp-label {
            display: none;
        }

        .claude-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            position: relative;
        }

        .claude-terminal {
            flex: 1;
            padding: 4px;
            overflow: hidden;
        }

        .claude-terminal .xterm {
            height: 100%;
        }

        .claude-terminal .xterm-viewport {
            overflow-y: auto !important;
        }

        /* Resize edges */
        .claude-resize-edge {
            position: absolute;
            z-index: 10;
        }

        .claude-resize-n {
            top: 0;
            left: 8px;
            right: 8px;
            height: 6px;
            cursor: ns-resize;
        }

        .claude-resize-s {
            bottom: 0;
            left: 8px;
            right: 8px;
            height: 6px;
            cursor: ns-resize;
        }

        .claude-resize-e {
            right: 0;
            top: 8px;
            bottom: 8px;
            width: 6px;
            cursor: ew-resize;
        }

        .claude-resize-w {
            left: 0;
            top: 8px;
            bottom: 8px;
            width: 6px;
            cursor: ew-resize;
        }

        /* Resize corners */
        .claude-resize-corner {
            position: absolute;
            width: 12px;
            height: 12px;
            z-index: 11;
        }

        .claude-resize-nw {
            top: 0;
            left: 0;
            cursor: nwse-resize;
        }

        .claude-resize-ne {
            top: 0;
            right: 0;
            cursor: nesw-resize;
        }

        .claude-resize-sw {
            bottom: 0;
            left: 0;
            cursor: nesw-resize;
        }

        .claude-resize-se {
            bottom: 0;
            right: 0;
            cursor: nwse-resize;
        }

        #claude-menu-btn {
            background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
            border: none;
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 13px;
            font-weight: 500;
            margin-left: 8px;
        }

        #claude-menu-btn:hover {
            background: linear-gradient(135deg, #7c7ff2 0%, #6366f1 100%);
        }
    `;
    document.head.appendChild(style);

    // Event listeners
    setTimeout(() => {
        // Close button
        container.querySelector(".claude-close").addEventListener("click", () => {
            container.style.display = "none";
        });

        // Reload button - fully reloads terminal
        container.querySelector(".claude-reload").addEventListener("click", () => {
            reloadTerminal();
        });

        // Minimize button - toggle minimized state
        const minimizeBtn = container.querySelector(".claude-minimize");
        let savedWidth = null;
        let savedRight = null;
        minimizeBtn.addEventListener("click", () => {
            const isMinimized = container.classList.toggle("minimized");
            minimizeBtn.textContent = isMinimized ? "+" : "−";
            minimizeBtn.title = isMinimized ? "Expand" : "Minimize";

            if (isMinimized) {
                // Save current width and right edge position
                savedWidth = container.offsetWidth;
                const rect = container.getBoundingClientRect();
                savedRight = window.innerWidth - rect.right;

                // Switch to right-anchored positioning
                container.style.left = "auto";
                container.style.right = savedRight + "px";
                container.classList.add("collapsed-width");
            } else {
                // Remove collapsed class and restore saved width
                container.classList.remove("collapsed-width");
                if (savedWidth) {
                    container.style.width = savedWidth + "px";
                }
                // Refit terminal when expanding
                if (fitAddon && terminal) {
                    setTimeout(() => {
                        window.fitTerminalPreserveScroll();
                    }, 50);
                }
            }
        });

        // MCP status indicator - click to check status
        container.querySelector(".claude-mcp-status").addEventListener("click", () => {
            checkMcpStatus();
        });

        // Initialize terminal
        initTerminal(container.querySelector("#claude-terminal"));

        // Start MCP status checking
        checkMcpStatus();
        // MCP status only checked on click, no polling
    }, 0);

    return container;
}

function initTerminal(terminalContainer) {
    // Create xterm.js terminal
    terminal = new Terminal({
        cursorBlink: true,
        cursorStyle: "block",
        fontSize: 13,
        fontFamily: '"SF Mono", "Monaco", "Inconsolata", "Fira Code", "Consolas", "Courier New", monospace',
        theme: {
            background: "#0d0d0d",
            foreground: "#e0e0e0",
            cursor: "#4ade80",
            cursorAccent: "#0d0d0d",
            selectionBackground: "rgba(255, 255, 255, 0.2)",
            black: "#000000",
            red: "#f87171",
            green: "#4ade80",
            yellow: "#fbbf24",
            blue: "#60a5fa",
            magenta: "#c084fc",
            cyan: "#22d3ee",
            white: "#e0e0e0",
            brightBlack: "#666666",
            brightRed: "#fca5a5",
            brightGreen: "#86efac",
            brightYellow: "#fcd34d",
            brightBlue: "#93c5fd",
            brightMagenta: "#d8b4fe",
            brightCyan: "#67e8f9",
            brightWhite: "#ffffff",
        },
        allowProposedApi: true,
        scrollback: 1000,
        smoothScrollDuration: 0,
        fastScrollModifier: "none",
        scrollOnUserInput: true,  // Only scroll when user types, not on focus
    });

    // Load fit addon
    fitAddon = new FitAddon.FitAddon();
    terminal.loadAddon(fitAddon);

    // Helper to fit terminal while preserving scroll position
    // (fitAddon.fit() can reset scroll position)
    window.fitTerminalPreserveScroll = function() {
        if (!fitAddon || !terminal) return;

        // Check if user is scrolled to bottom (following output)
        const buffer = terminal.buffer.active;
        const isAtBottom = buffer.viewportY >= buffer.baseY;

        // Get current scroll position
        const viewport = document.querySelector(".xterm-viewport");
        const scrollTop = viewport ? viewport.scrollTop : 0;

        // Do the fit
        fitAddon.fit();

        // Restore scroll position only if user wasn't at bottom
        // (if at bottom, let it stay at bottom to follow new output)
        if (!isAtBottom && viewport) {
            viewport.scrollTop = scrollTop;
        }
    };

    // Load unicode11 addon for proper character width handling (box drawing, CJK, etc.)
    try {
        const unicode11Addon = new Unicode11Addon.Unicode11Addon();
        terminal.loadAddon(unicode11Addon);
        terminal.unicode.activeVersion = "11";
        console.log("Unicode11 addon loaded");
    } catch (e) {
        console.log("Unicode11 addon not available:", e.message);
    }

    // Use Canvas renderer (faster than DOM, more stable than WebGL)
    try {
        terminal.loadAddon(new CanvasAddon.CanvasAddon());
        console.log("Using Canvas renderer");
    } catch (e) {
        console.log("Canvas addon not available, using DOM renderer:", e.message);
    }

    // Open terminal in container
    terminal.open(terminalContainer);

    // Fit to container after a short delay
    setTimeout(() => {
        fitAddon.fit();
    }, 100);

    // Handle terminal input - use minimal protocol for speed
    terminal.onData((data) => {
        if (websocket && websocket.readyState === WebSocket.OPEN) {
            // Fast path: 'i' prefix + data (no JSON overhead)
            websocket.send(JSON.stringify({ type: "i", d: data }));
        }
    });

    // Handle special keyboard shortcuts for macOS-style editing
    terminal.attachCustomKeyEventHandler((event) => {
        if (event.type !== "keydown") return true;

        const isMac = navigator.platform.toUpperCase().indexOf("MAC") >= 0;
        const modKey = isMac ? event.metaKey : event.ctrlKey;
        const altKey = event.altKey;

        // Helper to send escape sequence
        const send = (data) => {
            if (websocket && websocket.readyState === WebSocket.OPEN) {
                websocket.send(JSON.stringify({ type: "i", d: data }));
            }
        };

        // Shift+Enter: insert a literal newline (for multi-line input in Claude)
        if (event.key === "Enter" && event.shiftKey) {
            send("\n");
            return false;
        }

        // Option/Alt + Left Arrow: move word left (send ESC-b)
        if (altKey && event.key === "ArrowLeft") {
            send("\x1bb");
            return false;
        }

        // Option/Alt + Right Arrow: move word right (send ESC-f)
        if (altKey && event.key === "ArrowRight") {
            send("\x1bf");
        return false;
        }

        // Cmd/Ctrl + Left Arrow: move to beginning of line (send Ctrl-A)
        if (modKey && event.key === "ArrowLeft") {
            send("\x01");
            return false;
        }

        // Cmd/Ctrl + Right Arrow: move to end of line (send Ctrl-E)
        if (modKey && event.key === "ArrowRight") {
            send("\x05");
            return false;
        }

        // Option/Alt + Backspace: delete word backward (send ESC-DEL or Ctrl-W)
        if (altKey && event.key === "Backspace") {
            send("\x17"); // Ctrl-W (unix-word-rubout)
            return false;
        }

        // Cmd/Ctrl + Backspace: delete to beginning of line (send Ctrl-U)
        if (modKey && event.key === "Backspace") {
            send("\x15"); // Ctrl-U (kill line backward)
            return false;
        }

        // Option/Alt + Delete: delete word forward (send ESC-d)
        if (altKey && event.key === "Delete") {
            send("\x1bd");
            return false;
        }

        // Cmd/Ctrl + Delete: delete to end of line (send Ctrl-K)
        if (modKey && event.key === "Delete") {
            send("\x0b"); // Ctrl-K (kill line forward)
            return false;
        }

        return true; // Allow all other keys
    });

    // Handle terminal resize
    terminal.onResize(({ rows, cols }) => {
        if (websocket && websocket.readyState === WebSocket.OPEN) {
            websocket.send(JSON.stringify({ type: "resize", rows, cols }));
        }
    });

    // Preserve scroll position on blur (clicking outside terminal)
    // xterm's internal textarea can cause scroll resets on blur
    setTimeout(() => {
        const viewport = terminalContainer.querySelector(".xterm-viewport");
        const textarea = terminal.textarea;
        if (viewport && textarea) {
            let savedScrollTop = null;

            textarea.addEventListener("blur", () => {
                savedScrollTop = viewport.scrollTop;
                // Restore after any internal xterm processing
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        if (savedScrollTop !== null && viewport.scrollTop !== savedScrollTop) {
                            viewport.scrollTop = savedScrollTop;
                        }
                    });
                });
            });
        }
    }, 200);

    // Connect to WebSocket
    connectWebSocket();
}

function reloadTerminal() {
    // Close existing connection
    if (websocket) {
        websocket.close();
    }

    // Clear terminal
    if (terminal) {
        terminal.clear();
        terminal.writeln("\x1b[1;34mReloading terminal...\x1b[0m\n");
    }

    // Reconnect
    setTimeout(() => {
        connectWebSocket();
    }, 100);
}

function connectWebSocket() {
    // Close existing connection
    if (websocket) {
        websocket.close();
    }

    // Determine WebSocket URL
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws/claude-terminal`;

    console.log(`[Claude Code] Connecting to ${wsUrl}`);

    try {
        websocket = new WebSocket(wsUrl);

        websocket.onopen = () => {
            console.log("[Claude Code] WebSocket connected");

            // Clear terminal first
            terminal.clear();

            // Send initial size immediately
            if (terminal && fitAddon) {
                fitAddon.fit();
                const sendSize = () => {
                    const dims = fitAddon.proposeDimensions();
                    if (dims && websocket && websocket.readyState === WebSocket.OPEN) {
                        websocket.send(JSON.stringify({ type: "resize", rows: dims.rows, cols: dims.cols }));
                    }
                };

                // Send size immediately
                sendSize();

                // Send resize multiple times after claude has started to force proper rendering
                // Claude needs SIGWINCH to redraw properly
                setTimeout(sendSize, 300);
                setTimeout(sendSize, 800);
                setTimeout(sendSize, 1500);
            }
        };

        websocket.onmessage = (event) => {
            const data = event.data;
            // Fast path: 'o' prefix means raw output (no JSON)
            if (data[0] === 'o') {
                const output = data.slice(1);
                // Filter out focus tracking sequences that can cause issues
                const filteredData = output.replace(/\x1b\[\[?[IO]/g, "");
                if (filteredData) {
                    terminal.write(filteredData);
                }
                return;
            }
            // Legacy JSON format
            try {
                const msg = JSON.parse(data);
                if (msg.type === "output" && msg.data) {
                    const filteredData = msg.data.replace(/\x1b\[\[?[IO]/g, "");
                    if (filteredData) {
                        terminal.write(filteredData);
                    }
                }
            } catch (e) {
                // Unknown format, ignore
            }
        };

        websocket.onclose = (event) => {
            console.log("[Claude Code] WebSocket closed:", event.code, event.reason);
            terminal.writeln("\n\x1b[1;31mTerminal disconnected.\x1b[0m");
            terminal.writeln("Click ↻ to reload.\n");
        };

        websocket.onerror = (error) => {
            console.error("[Claude Code] WebSocket error:", error);
        };
    } catch (e) {
        console.error("[Claude Code] Failed to create WebSocket:", e);
        terminal.writeln(`\x1b[1;31mFailed to connect: ${e.message}\x1b[0m\n`);
    }
}

function makeDraggable(element, handle) {
    let pos1 = 0,
        pos2 = 0,
        pos3 = 0,
        pos4 = 0;

    handle.onmousedown = dragMouseDown;

    function dragMouseDown(e) {
        // Don't drag if clicking on buttons
        if (e.target.closest(".claude-btn")) return;

        e.preventDefault();
        pos3 = e.clientX;
        pos4 = e.clientY;
        document.onmouseup = closeDragElement;
        document.onmousemove = elementDrag;
    }

    function elementDrag(e) {
        e.preventDefault();
        pos1 = pos3 - e.clientX;
        pos2 = pos4 - e.clientY;
        pos3 = e.clientX;
        pos4 = e.clientY;

        const newTop = element.offsetTop - pos2;
        const newLeft = element.offsetLeft - pos1;

        // Keep within viewport bounds
        const maxTop = window.innerHeight - element.offsetHeight;
        const maxLeft = window.innerWidth - element.offsetWidth;

        element.style.top = Math.max(0, Math.min(newTop, maxTop)) + "px";
        element.style.left = Math.max(0, Math.min(newLeft, maxLeft)) + "px";
        element.style.right = "auto";
    }

    function closeDragElement() {
        document.onmouseup = null;
        document.onmousemove = null;
    }
}

function makeResizable(element) {
    const minWidth = 400;
    const minHeight = 300;
    let resizeTimeout = null;

    // Handle all resize elements
    const resizeElements = element.querySelectorAll(".claude-resize-edge, .claude-resize-corner");

    resizeElements.forEach((resizer) => {
        resizer.addEventListener("mousedown", (e) => {
            e.preventDefault();
            e.stopPropagation();

            const startX = e.clientX;
            const startY = e.clientY;
            const startWidth = element.offsetWidth;
            const startHeight = element.offsetHeight;
            const startLeft = element.offsetLeft;
            const startTop = element.offsetTop;

            const isLeft = resizer.classList.contains("claude-resize-w") ||
                           resizer.classList.contains("claude-resize-nw") ||
                           resizer.classList.contains("claude-resize-sw");
            const isTop = resizer.classList.contains("claude-resize-n") ||
                          resizer.classList.contains("claude-resize-nw") ||
                          resizer.classList.contains("claude-resize-ne");
            const isRight = resizer.classList.contains("claude-resize-e") ||
                            resizer.classList.contains("claude-resize-ne") ||
                            resizer.classList.contains("claude-resize-se");
            const isBottom = resizer.classList.contains("claude-resize-s") ||
                             resizer.classList.contains("claude-resize-sw") ||
                             resizer.classList.contains("claude-resize-se");

            function resize(e) {
                const dx = e.clientX - startX;
                const dy = e.clientY - startY;

                if (isRight) {
                    const newWidth = Math.max(minWidth, startWidth + dx);
                    element.style.width = newWidth + "px";
                }

                if (isBottom) {
                    const newHeight = Math.max(minHeight, startHeight + dy);
                    element.style.height = newHeight + "px";
                }

                if (isLeft) {
                    const newWidth = Math.max(minWidth, startWidth - dx);
                    if (newWidth > minWidth) {
                        element.style.width = newWidth + "px";
                        element.style.left = (startLeft + dx) + "px";
                        element.style.right = "auto";
                    }
                }

                if (isTop) {
                    const newHeight = Math.max(minHeight, startHeight - dy);
                    if (newHeight > minHeight) {
                        element.style.height = newHeight + "px";
                        element.style.top = (startTop + dy) + "px";
                    }
                }

                // Debounce terminal fit
                if (resizeTimeout) {
                    clearTimeout(resizeTimeout);
                }
                resizeTimeout = setTimeout(() => {
                    if (fitAddon) {
                        window.fitTerminalPreserveScroll();
                    }
                }, 16);
            }

            function stopResize() {
                document.removeEventListener("mousemove", resize);
                document.removeEventListener("mouseup", stopResize);

                // Final fit after resize
                if (fitAddon) {
                    setTimeout(() => {
                        window.fitTerminalPreserveScroll();
                    }, 50);
                }
            }

            document.addEventListener("mousemove", resize);
            document.addEventListener("mouseup", stopResize);
        });
    });
}

function addMenuButton(floatingWindow) {
    // Wait for ComfyUI menu to load
    const checkMenu = setInterval(() => {
        const menu = document.querySelector(".comfy-menu") || document.querySelector(".comfyui-menu");
        if (menu) {
            clearInterval(checkMenu);

            const btn = document.createElement("button");
            btn.id = "claude-menu-btn";
            btn.textContent = "Claude Code";
            btn.addEventListener("click", () => {
                if (floatingWindow.style.display === "none") {
                    floatingWindow.style.display = "flex";
                    if (fitAddon) {
                        setTimeout(() => window.fitTerminalPreserveScroll(), 100);
                    }
                } else {
                    floatingWindow.style.display = "none";
                }
            });

            menu.appendChild(btn);
        }
    }, 500);

    // Stop checking after 10 seconds
    setTimeout(() => clearInterval(checkMenu), 10000);
}

function addContextMenuOption() {
    // Hook into LiteGraph's context menu
    const originalGetCanvasMenuOptions = LGraphCanvas.prototype.getCanvasMenuOptions;

    LGraphCanvas.prototype.getCanvasMenuOptions = function (...args) {
        const options = originalGetCanvasMenuOptions.apply(this, args);

        // Add separator and Claude Code option
        options.push(null); // separator
        options.push({
            content: "Open Claude Code",
            callback: () => {
                if (floatingWindow) {
                    floatingWindow.style.display = "flex";

                    // Fit terminal and focus
                    if (fitAddon) {
                        setTimeout(() => {
                            window.fitTerminalPreserveScroll();
                            if (terminal) terminal.focus();
                        }, 100);
                    }
                }
            },
        });

        return options;
    };
}

// Handle window resize - debounced and only if size actually changed
let lastTerminalSize = { cols: 0, rows: 0 };
let windowResizeTimeout = null;

window.addEventListener("resize", () => {
    if (fitAddon && floatingWindow && floatingWindow.style.display !== "none") {
        // Debounce resize events
        if (windowResizeTimeout) {
            clearTimeout(windowResizeTimeout);
        }
        windowResizeTimeout = setTimeout(() => {
            const dims = fitAddon.proposeDimensions();
            if (dims && (dims.cols !== lastTerminalSize.cols || dims.rows !== lastTerminalSize.rows)) {
                lastTerminalSize = { cols: dims.cols, rows: dims.rows };
                window.fitTerminalPreserveScroll();
            }
        }, 100);
    }
});


// MCP status checking
async function checkMcpStatus() {
    const indicator = document.querySelector(".mcp-indicator");
    const label = document.querySelector(".mcp-label");
    if (!indicator) return;

    // Set to checking state
    indicator.className = "mcp-indicator checking";
    label.textContent = "MCP...";

    try {
        // Try to reach the MCP server via our backend endpoint
        const response = await fetch("/claude-code/mcp-status", {
            method: "GET",
            signal: AbortSignal.timeout(3000)
        });
        const data = await response.json();

        if (data.connected) {
            indicator.className = "mcp-indicator connected";
            label.textContent = "MCP";
            indicator.parentElement.title = `MCP Connected - ${data.tools || 0} tools available`;
        } else {
            indicator.className = "mcp-indicator disconnected";
            label.textContent = "MCP";
            indicator.parentElement.title = `MCP Disconnected - ${data.error || "Not running"}`;
        }
    } catch (e) {
        indicator.className = "mcp-indicator disconnected";
        label.textContent = "MCP";
        indicator.parentElement.title = "MCP Status Unknown - Could not check";
    }
}

// Workflow sync - send current workflow to backend periodically
function startWorkflowSync() {
    // Sync immediately and then every 2 seconds
    syncWorkflow();
    setInterval(syncWorkflow, 2000);

    // Also poll for graph commands
    pollGraphCommands();
    setInterval(pollGraphCommands, 200);
}

// Track if workflow has changed to avoid unnecessary syncs
let lastWorkflowHash = null;

async function syncWorkflow() {
    try {
        if (!app.graph) return;

        // Get the workflow in ComfyUI's format
        const workflow = app.graph.serialize();

        // Simple hash to detect changes - avoid syncing if nothing changed
        const workflowStr = JSON.stringify(workflow);
        const hash = workflowStr.length + "_" + (workflowStr.charCodeAt(100) || 0);
        if (hash === lastWorkflowHash) return;
        lastWorkflowHash = hash;

        // Send to backend (without graphToPrompt which can cause UI flicker)
        await fetch("/claude-code/workflow", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                workflow: workflow,
                workflow_api: null,  // Only fetch on demand to avoid flicker
                timestamp: Date.now(),
            }),
        });
    } catch (e) {
        // Silently fail - don't spam console
    }
}

// Get workflow API format on demand (called before running nodes)
async function getWorkflowApi() {
    try {
        if (!app.graph) return null;
        return await app.graphToPrompt();
    } catch (e) {
        return null;
    }
}

async function pollGraphCommands() {
    try {
        const response = await fetch("/claude-code/graph-command");
        const data = await response.json();

        if (data.command) {
            const result = await executeGraphCommand(data.command);

            // Send result back
            await fetch("/claude-code/graph-command", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    command_id: data.command.id,
                    result: result
                })
            });
        }
    } catch (e) {
        // Silently fail
    }
}

async function executeGraphCommand(command) {
    const { action, params } = command;

    try {
        if (!app.graph) {
            return { error: "Graph not available" };
        }

        switch (action) {
            case "get_workflow_api": {
                // Fetch workflow API format on demand (for run_node)
                try {
                    const workflowApi = await app.graphToPrompt();
                    return { workflow_api: workflowApi };
                } catch (e) {
                    return { error: `Failed to get workflow API: ${e.message}` };
                }
            }

            case "queue_prompt": {
                // Queue prompt from frontend so it uses the browser's client_id
                // This ensures preview images show up in the UI
                try {
                    await app.queuePrompt(0, 1);  // queue at front, 1 batch
                    return { status: "queued" };
                } catch (e) {
                    return { error: `Failed to queue prompt: ${e.message}` };
                }
            }

            case "create_node": {
                const node = LiteGraph.createNode(params.type);
                if (!node) {
                    return { error: `Failed to create node of type: ${params.type}` };
                }
                node.pos = [params.pos_x || 100, params.pos_y || 100];
                if (params.title) {
                    node.title = params.title;
                }
                app.graph.add(node);
                app.graph.setDirtyCanvas(true, true);
                return {
                    status: "created",
                    node_id: node.id,
                    type: params.type,
                    title: node.title,
                    pos: node.pos,
                    size: node.size
                };
            }

            case "delete_node": {
                const nodeId = parseInt(params.node_id);
                const node = app.graph.getNodeById(nodeId);
                if (!node) {
                    return { error: `Node ${params.node_id} not found` };
                }
                app.graph.remove(node);
                app.graph.setDirtyCanvas(true, true);
                return {
                    status: "deleted",
                    node_id: params.node_id
                };
            }

            case "set_node_property": {
                const nodeId = parseInt(params.node_id);
                const node = app.graph.getNodeById(nodeId);
                if (!node) {
                    return { error: `Node ${params.node_id} not found` };
                }

                // Try to find the widget by name
                let found = false;
                if (node.widgets) {
                    for (const widget of node.widgets) {
                        if (widget.name === params.property_name) {
                            widget.value = params.value;
                            if (widget.callback) {
                                widget.callback(params.value, app.canvas, node, [0, 0], null);
                            }
                            found = true;
                            break;
                        }
                    }
                }

                if (!found) {
                    // Try setting as a direct property
                    if (params.property_name in node) {
                        node[params.property_name] = params.value;
                        found = true;
                    } else if (node.properties && params.property_name in node.properties) {
                        node.properties[params.property_name] = params.value;
                        found = true;
                    }
                }

                if (!found) {
                    return { error: `Property '${params.property_name}' not found on node ${params.node_id}` };
                }

                app.graph.setDirtyCanvas(true, true);
                return {
                    status: "updated",
                    node_id: params.node_id,
                    property: params.property_name,
                    value: params.value
                };
            }

            case "connect_nodes": {
                const fromNodeId = parseInt(params.from_node_id);
                const toNodeId = parseInt(params.to_node_id);
                const fromNode = app.graph.getNodeById(fromNodeId);
                const toNode = app.graph.getNodeById(toNodeId);

                if (!fromNode) {
                    return { error: `Source node ${params.from_node_id} not found` };
                }
                if (!toNode) {
                    return { error: `Target node ${params.to_node_id} not found` };
                }

                const link = fromNode.connect(params.from_slot, toNode, params.to_slot);
                app.graph.setDirtyCanvas(true, true);

                return {
                    status: "connected",
                    from_node: params.from_node_id,
                    from_slot: params.from_slot,
                    to_node: params.to_node_id,
                    to_slot: params.to_slot,
                    link_id: link ? link.id : null
                };
            }

            case "disconnect_nodes": {
                const fromNodeId = parseInt(params.from_node_id);
                const toNodeId = parseInt(params.to_node_id);
                const fromNode = app.graph.getNodeById(fromNodeId);
                const toNode = app.graph.getNodeById(toNodeId);

                if (!fromNode) {
                    return { error: `Source node ${params.from_node_id} not found` };
                }
                if (!toNode) {
                    return { error: `Target node ${params.to_node_id} not found` };
                }

                // Find and remove the link
                if (toNode.inputs && toNode.inputs[params.to_slot]) {
                    const linkId = toNode.inputs[params.to_slot].link;
                    if (linkId !== null) {
                        app.graph.removeLink(linkId);
                    }
                }

                app.graph.setDirtyCanvas(true, true);
                return {
                    status: "disconnected",
                    from_node: params.from_node_id,
                    from_slot: params.from_slot,
                    to_node: params.to_node_id,
                    to_slot: params.to_slot
                };
            }

            case "move_node": {
                const nodeId = parseInt(params.node_id);
                const node = app.graph.getNodeById(nodeId);

                if (!node) {
                    return { error: `Node ${params.node_id} not found` };
                }

                let newX, newY;

                if (params.relative_to && params.direction) {
                    // Relative positioning
                    const refNodeId = parseInt(params.relative_to);
                    const refNode = app.graph.getNodeById(refNodeId);

                    if (!refNode) {
                        return { error: `Reference node ${params.relative_to} not found` };
                    }

                    const gap = params.gap || 30;
                    const refPos = refNode.pos;
                    const refSize = refNode.size || [200, 100];
                    const nodeSize = node.size || [200, 100];

                    switch (params.direction) {
                        case "right":
                            newX = refPos[0] + refSize[0] + gap;
                            newY = refPos[1];
                            break;
                        case "left":
                            newX = refPos[0] - nodeSize[0] - gap;
                            newY = refPos[1];
                            break;
                        case "below":
                            newX = refPos[0];
                            newY = refPos[1] + refSize[1] + gap;
                            break;
                        case "above":
                            newX = refPos[0];
                            newY = refPos[1] - nodeSize[1] - gap;
                            break;
                        default:
                            return { error: `Unknown direction: ${params.direction}` };
                    }
                } else if (params.x !== null && params.x !== undefined &&
                           params.y !== null && params.y !== undefined) {
                    // Absolute positioning
                    newX = params.x;
                    newY = params.y;
                } else if (params.width || params.height) {
                    // Resize only, no move - keep current position
                    newX = node.pos[0];
                    newY = node.pos[1];
                } else {
                    return { error: "Must provide (x, y), (relative_to, direction), or (width, height)" };
                }

                node.pos = [newX, newY];

                // Apply resize if specified
                if (params.width || params.height) {
                    const currentSize = node.size || [200, 100];
                    node.size = [
                        params.width || currentSize[0],
                        params.height || currentSize[1]
                    ];
                }

                app.graph.setDirtyCanvas(true, true);

                return {
                    status: "moved",
                    node_id: params.node_id,
                    pos: node.pos,
                    size: node.size
                };
            }

            default:
                return { error: `Unknown action: ${action}` };
        }
    } catch (e) {
        return { error: e.message || String(e) };
    }
}
