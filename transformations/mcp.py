import importlib.util
import json
from pathlib import Path

DESCRIPTION = "Lists the tools of the MCP servers (mcp_servers.json and the ACP editor session) in the system message, for use with the mcp tool."

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("_mcp_tool", ROOT / "tools" / "mcp.py")
mcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp)
MAX_SCHEMA_CHARS = 300

def transform(messages, tools):
    if not mcp.read_config():
        return messages, tools
    mcp.ensure_daemon()
    try:
        servers = json.loads(mcp.TOOLS.read_text())
    except Exception:
        servers = {}
    lines = ["MCP servers (call their tools with the mcp tool: server, tool, arguments as JSON object text):"]
    for name, server in servers.items():
        lines.append(f"{name} [{server['status']}]")
        for tool in server["tools"]:
            schema = json.dumps(tool.get("inputSchema", {}).get("properties", {}), ensure_ascii=False)
            if len(schema) > MAX_SCHEMA_CHARS:
                schema = schema[:MAX_SCHEMA_CHARS] + " [TRUNCATED]"
            lines.append(f"  {tool['name']}: {tool.get('description', '').strip()} arguments: {schema}")
    if not servers:
        lines.append("[STARTING]")
    messages[0]["content"] += "\n\n" + "\n".join(lines)
    return messages, tools
