import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

DESCRIPTION = "Call a tool of an MCP server (servers from mcp_servers.json, their tools are listed in the system message). arguments: JSON object as text. Slow calls return PENDING and their result arrives later via the mcp channel."

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = [ROOT / "mcp_servers.json", ROOT / "channels" / ".runtime" / "acp" / "mcp_servers.json"] #the second one is written by the ACP channel from the editor's session
RUNTIME = ROOT / "channels" / ".runtime" / "mcp"
REQUESTS = RUNTIME / "requests"
PENDING = RUNTIME / "pending" #claim token: whoever removes pending/<id> decides where the result goes
LATE = RUNTIME / "late"
RESPONSES = RUNTIME / "responses"
INBOX = RUNTIME / "inbox" #read by channels/mcp.py
TMP = RUNTIME / "tmp"
PID = RUNTIME / "pid"
TOOLS = RUNTIME / "tools.json" #read by transformations/mcp.py
WAIT = 3 #seconds the tool waits before returning PENDING (must stay below DYNAMIC_TIMEOUT)
START_TIMEOUT = 120
CALL_TIMEOUT = 600

def write_atomic(path, text):
    tmp = TMP / uuid.uuid4().hex
    tmp.write_text(text)
    os.replace(tmp, path)

def take(path):
    text = path.read_text()
    path.unlink()
    return text

def ensure_daemon():
    for directory in (REQUESTS, PENDING, LATE, RESPONSES, INBOX, TMP):
        directory.mkdir(parents=True, exist_ok=True)
    try:
        os.kill(int(PID.read_text()), 0)
        return
    except Exception:
        pass
    process = subprocess.Popen([sys.executable, __file__, "--daemon"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    PID.write_text(str(process.pid))

def run(server, tool, arguments):
    try:
        arguments = json.loads(arguments) if arguments.strip() else {}
    except ValueError as error:
        return f"arguments must be a JSON object: {error}"
    if not isinstance(arguments, dict):
        return "arguments must be a JSON object"
    ensure_daemon()
    rid = uuid.uuid4().hex
    (PENDING / rid).write_text("")
    write_atomic(REQUESTS / rid, json.dumps({"server": server, "tool": tool, "arguments": arguments}))
    deadline = time.time() + WAIT
    while time.time() < deadline:
        if (RESPONSES / rid).exists():
            return take(RESPONSES / rid)
        time.sleep(0.05)
    try:
        os.rename(PENDING / rid, LATE / rid)
        return f"PENDING: {server}.{tool} is still running, its result will arrive via the mcp channel tagged [{rid[:8]}]"
    except FileNotFoundError: #the daemon claimed it in the meantime
        deadline = time.time() + 1
        while time.time() < deadline and not (RESPONSES / rid).exists():
            time.sleep(0.05)
        return take(RESPONSES / rid)

# --------------------------------------------------------------------
# Daemon: keeps the MCP server processes (stdio, JSON-RPC) alive
# --------------------------------------------------------------------
SERVERS = {}

class Server:
    def __init__(self, name, config):
        env = dict(os.environ)
        env.update({str(k): str(v) for k, v in config.get("env", {}).items()})
        self.name = name
        self.status = "starting"
        self.tools = []
        self.next_id = 0
        self.waiting = {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.process = subprocess.Popen([config["command"]] + list(config.get("args", [])), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, env=env, cwd=str(ROOT), text=True, bufsize=1)
        threading.Thread(target=self.read, daemon=True).start()
        threading.Thread(target=self.start, daemon=True).start()

    def write(self, message):
        with self.lock:
            self.process.stdin.write(json.dumps(message) + "\n")
            self.process.stdin.flush()

    def call(self, method, params, timeout):
        done = threading.Event()
        box = {}
        def callback(message):
            box.update(message)
            done.set()
        with self.lock:
            self.next_id += 1
            request_id = self.next_id
            self.waiting[request_id] = callback
        self.write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        if not done.wait(timeout):
            self.waiting.pop(request_id, None)
            raise TimeoutError(f"{method} took longer than {timeout}s")
        if "error" in box:
            raise RuntimeError(box["error"].get("message", str(box["error"])))
        return box.get("result", {})

    def read(self):
        for line in self.process.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if "method" not in message and "id" in message: #response
                callback = self.waiting.pop(message["id"], None)
                if callback:
                    callback(message)
            elif "id" in message: #request from the server
                if message["method"] == "ping":
                    self.write({"jsonrpc": "2.0", "id": message["id"], "result": {}})
                else:
                    self.write({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": "Method not found"}})
        self.status = "exited"
        for callback in list(self.waiting.values()):
            callback({"error": {"message": "MCP server exited"}})
        self.waiting.clear()
        publish_tools()

    def start(self):
        try:
            self.call("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "iter", "version": "1.0"}}, START_TIMEOUT)
            self.write({"jsonrpc": "2.0", "method": "notifications/initialized"})
            tools, cursor = [], None
            while True:
                result = self.call("tools/list", {"cursor": cursor} if cursor else {}, START_TIMEOUT)
                tools += result.get("tools", [])
                cursor = result.get("nextCursor")
                if not cursor:
                    break
            self.tools = tools
            self.status = "ready"
        except Exception as error:
            if self.status == "starting":
                self.status = f"failed: {type(error).__name__}: {error}"
        self.ready.set()
        publish_tools()

def publish_tools():
    write_atomic(TOOLS, json.dumps({name: {"status": server.status, "tools": server.tools} for name, server in SERVERS.items()}, ensure_ascii=False))

def format_result(result):
    parts = [item["text"] if item.get("type") == "text" else json.dumps(item, ensure_ascii=False) for item in result.get("content", [])]
    if not parts and "structuredContent" in result:
        parts = [json.dumps(result["structuredContent"], ensure_ascii=False)]
    return ("[MCP TOOL ERROR] " if result.get("isError") else "") + "\n".join(parts)

def handle(rid, request):
    server = SERVERS.get(request["server"])
    try:
        if server is None:
            text = f"Unknown MCP server: {request['server']} (known: {', '.join(SERVERS) or 'none'})"
        else:
            server.ready.wait(START_TIMEOUT)
            if server.status != "ready":
                text = f"MCP server {server.name} is not ready: {server.status}"
            else:
                text = format_result(server.call("tools/call", {"name": request["tool"], "arguments": request["arguments"]}, CALL_TIMEOUT))
    except Exception as error:
        text = f"MCP error: {type(error).__name__}: {error}"
    try:
        os.unlink(PENDING / rid) #the tool is still waiting
        write_atomic(RESPONSES / rid, text)
    except FileNotFoundError: #the tool returned PENDING
        (LATE / rid).unlink(missing_ok=True)
        write_atomic(INBOX / str(time.time_ns()), f"[{rid[:8]}] {request['server']}.{request['tool']} returned: {text}")

def read_config():
    servers = {}
    for path in CONFIGS:
        try:
            servers.update(json.loads(path.read_text()).get("mcpServers", {}))
        except Exception:
            pass
    return servers

def daemon():
    for directory in (REQUESTS, PENDING, LATE, RESPONSES, INBOX, TMP):
        directory.mkdir(parents=True, exist_ok=True)
    PID.write_text(str(os.getpid()))
    config = None
    while True:
        new_config = read_config()
        if new_config != config: #(re)start all servers when the configuration changed
            for server in SERVERS.values():
                server.process.kill()
            SERVERS.clear()
            for name, server_config in new_config.items():
                try:
                    SERVERS[name] = Server(name, server_config)
                except Exception as error:
                    pass
            config = new_config
            publish_tools()
        for path in sorted(REQUESTS.glob("*")):
            try:
                request = json.loads(take(path))
            except Exception:
                continue
            threading.Thread(target=handle, args=(path.name, request), daemon=True).start()
        time.sleep(0.05)

if __name__ == "__main__" and "--daemon" in sys.argv:
    daemon()
