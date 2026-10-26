import importlib.util
from pathlib import Path

# Results of MCP tool calls that took longer than the mcp tool waits (see tools/mcp.py).

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("_mcp_tool", ROOT / "tools" / "mcp.py")
mcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp)

def receive():
    messages = []
    for path in sorted(mcp.INBOX.glob("*")) if mcp.INBOX.is_dir() else []:
        try:
            messages.append(path.read_text())
            path.unlink()
        except FileNotFoundError:
            pass
    return "\n".join(messages)
