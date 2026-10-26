import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

# ACP (Agent Client Protocol) channel: an editor (e.g. Zed) uses Iter as its agent.
# The editor starts "python3 channels/acp.py --stdio" (the bridge), which speaks ACP JSON-RPC on stdio,
# starts Iter if it is not running yet, and exchanges messages with this channel's receive/send via files.
# receive() also reports the end of a turn (nop) and the tool calls of the turn by reading experience.json.

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "channels" / ".runtime" / "acp"
INBOX = RUNTIME / "inbox"
OUTBOX = RUNTIME / "outbox"
EVENTS = RUNTIME / "events" #turn events for the bridge, written by receive()
DELIVERED = RUNTIME / "delivered" #the prompt handed to Iter by receive(), while the turn is open
REPORTED = RUNTIME / "reported" #tool call ids already shown in the editor
TMP = RUNTIME / "tmp"
PID = RUNTIME / "pid"
PROMPT_OPEN = RUNTIME / "prompt_open" #exists while the editor waits for the end of a turn
MCP_SERVERS = RUNTIME / "mcp_servers.json" #MCP servers of the editor session, used by tools/mcp.py
ITER_LOG = RUNTIME / "iter.log"
ITER_PID = RUNTIME / "iter_pid"
ITER_START = os.environ.get("ITER_START", "1") == "1" #0 if Iter is started separately
ITER_COMMAND = os.environ.get("ITER_COMMAND", "sh " + str(ROOT.parent / "PeTTa" / "run.sh") + " iter.metta --silent")

def write_atomic(directory, text):
    TMP.mkdir(parents=True, exist_ok=True)
    tmp = TMP / str(time.time_ns())
    tmp.write_text(text)
    os.replace(tmp, directory / tmp.name)

def bridge_alive():
    try:
        os.kill(int(PID.read_text()), 0)
        return True
    except Exception:
        return False

def receive():
    if not INBOX.is_dir():
        return ""
    report_turn()
    messages = []
    for path in sorted(INBOX.glob("*")):
        try:
            messages.append(path.read_text())
            path.unlink()
        except FileNotFoundError:
            pass
    if messages and PROMPT_OPEN.exists():
        DELIVERED.write_text(json.dumps({"prompt": messages[-1], "time": time.strftime("%Y-%m-%d %H:%M:%S")}))
    return "\n".join(messages)

def tool_result(experience, call_id):
    for message in experience:
        if message.get("role") == "tool" and message.get("tool_call_id") == call_id:
            return message.get("content")
    return None

def report_turn(): #tool calls and the end of the current turn, from the experience after the delivered prompt
    if not (PROMPT_OPEN.exists() and DELIVERED.exists()):
        return
    try:
        experience = json.loads((ROOT / "experience.json").read_text())
    except Exception:
        return
    delivered = json.loads(DELIVERED.read_text())
    prompt = "[acp] " + delivered["prompt"]
    starts = [i for i, message in enumerate(experience) if message.get("role") == "user" and prompt in str(message.get("content"))
              and str(message.get("content"))[5:24] >= delivered["time"]] #"Step YYYY-mm-dd HH:MM:SS: ..." not older than the delivery
    if not starts:
        return
    reported = set(REPORTED.read_text().split()) if REPORTED.exists() else set()
    ended = False
    for message in experience[starts[-1] + 1:]:
        for call in message.get("tool_calls", []) if message.get("role") == "assistant" else []:
            name = call["function"]["name"]
            if name == "nop":
                ended = True
            elif name != "send" and call["id"] not in reported:
                result = tool_result(experience, call["id"])
                if result is None:
                    continue
                reported.add(call["id"])
                write_atomic(EVENTS, json.dumps({"type": "tool_call", "update": {
                    "sessionUpdate": "tool_call", "toolCallId": call["id"], "title": f"{name} {call['function']['arguments']}"[:200],
                    "kind": "execute" if name in ("shell", "python", "metta") else "other", "status": "completed",
                    "rawInput": call["function"]["arguments"], "content": [{"type": "content", "content": {"type": "text", "text": result}}]}}))
    REPORTED.write_text("\n".join(reported))
    if ended:
        DELIVERED.unlink(missing_ok=True)
        write_atomic(EVENTS, json.dumps({"type": "end_turn"}))

def send(content):
    if not bridge_alive():
        raise RuntimeError("no ACP client (editor) connected")
    write_atomic(OUTBOX, str(content))

# --------------------------------------------------------------------
# Bridge (started by the editor)
# --------------------------------------------------------------------
LOCK = threading.Lock()
STATE = {"session": None, "prompt_id": None}

def write(message):
    with LOCK:
        sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
        sys.stdout.flush()

def respond(request_id, result):
    write({"jsonrpc": "2.0", "id": request_id, "result": result})

def update(update):
    if STATE["session"]:
        write({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": STATE["session"], "update": update}})

def end_turn(stop_reason):
    with LOCK:
        prompt_id, STATE["prompt_id"] = STATE["prompt_id"], None
        PROMPT_OPEN.unlink(missing_ok=True)
        DELIVERED.unlink(missing_ok=True)
    if prompt_id is not None:
        respond(prompt_id, {"stopReason": stop_reason})

def prompt_text(blocks):
    parts = []
    for block in blocks:
        if block.get("type") == "text":
            parts.append(block["text"])
        elif block.get("type") == "resource_link":
            parts.append(f"[resource: {block.get('uri')}]")
        elif block.get("type") == "resource":
            resource = block.get("resource", {})
            parts.append(f"[resource {resource.get('uri')}]:\n{resource.get('text', '[binary]')}")
        else:
            parts.append(f"[{block.get('type')} content omitted]")
    return "\n".join(parts)

def start_iter(): #unless Iter was already started by this channel and is still running
    try:
        os.kill(int(ITER_PID.read_text()), 0)
        return
    except Exception:
        pass
    process = subprocess.Popen(ITER_COMMAND, shell=True, cwd=str(ROOT), stdin=subprocess.DEVNULL, stdout=open(ITER_LOG, "a"),
                               stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
    ITER_PID.write_text(str(process.pid))

def set_mcp_servers(servers):
    config = {}
    for server in servers:
        if "command" in server: #stdio servers only
            config[server["name"]] = {"command": server["command"], "args": server.get("args", []),
                                      "env": {item["name"]: item["value"] for item in server.get("env", [])}}
    MCP_SERVERS.write_text(json.dumps({"mcpServers": config}, ensure_ascii=False))

def handle(message):
    method, request_id, params = message.get("method"), message.get("id"), message.get("params", {})
    if method is None:
        return #a response to a request we never send
    if method == "initialize":
        respond(request_id, {"protocolVersion": 1,
                             "agentCapabilities": {"loadSession": False, "promptCapabilities": {"image": False, "audio": False, "embeddedContext": True}},
                             "authMethods": [], "agentInfo": {"name": "iter", "version": "1.0"}})
    elif method == "authenticate":
        respond(request_id, {})
    elif method == "session/new":
        end_turn("cancelled")
        STATE["session"] = "iter-" + str(time.time_ns())
        set_mcp_servers(params.get("mcpServers", []))
        if ITER_START:
            start_iter()
        respond(request_id, {"sessionId": STATE["session"]})
    elif method == "session/prompt":
        end_turn("cancelled")
        with LOCK:
            STATE["prompt_id"] = request_id
            DELIVERED.unlink(missing_ok=True)
            PROMPT_OPEN.write_text(str(request_id))
        if ITER_START:
            start_iter()
        write_atomic(INBOX, prompt_text(params.get("prompt", [])))
    elif method == "session/cancel":
        write_atomic(INBOX, "[THE USER CANCELLED THE CURRENT TURN. STOP WORKING ON IT.]")
        end_turn("cancelled")
    elif request_id is not None:
        write({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Method not found: {method}"}})

def read_stdin():
    for line in sys.stdin:
        try:
            handle(json.loads(line))
        except ValueError:
            pass
    os._exit(0) #editor closed the connection

def bridge():
    for directory in (INBOX, OUTBOX, EVENTS, TMP):
        directory.mkdir(parents=True, exist_ok=True)
    PID.write_text(str(os.getpid()))
    PROMPT_OPEN.unlink(missing_ok=True)
    DELIVERED.unlink(missing_ok=True)
    for path in list(OUTBOX.glob("*")) + list(EVENTS.glob("*")): #left over from an earlier editor connection
        path.unlink()
    threading.Thread(target=read_stdin, daemon=True).start()
    while True:
        for path in sorted(OUTBOX.glob("*")):
            update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": path.read_text() + "\n"}})
            path.unlink()
        for path in sorted(EVENTS.glob("*")):
            event = json.loads(path.read_text())
            path.unlink()
            if event["type"] == "end_turn":
                end_turn("end_turn")
            elif event["type"] == "tool_call":
                update(event["update"])
        time.sleep(0.05)

if __name__ == "__main__" and "--stdio" in sys.argv:
    bridge()
