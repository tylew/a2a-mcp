# a2a-mcp

A light MCP server that lets an MCP client — Claude Code, Claude Desktop, Cursor, Codex — talk to other
agents over the [A2A (Agent-to-Agent) protocol](https://a2a-protocol.org).

- **One file, standard library only.** No dependencies, no install step: `python3 a2a_mcp.py`.
- **A2A v1.0**, with fallback to v0.3 method names and message shapes for older peers.
- **Long tasks don't break.** Replies are streamed and the stream is held open in the background, so a tool
  call can return "still working" and the model checks back later with `a2a_get_task` — the remote task
  isn't dropped when the tool call returns. Streaming keepalives also carry tasks past proxy idle limits
  (verified: a 2-minute task through a Cloudflare Tunnel, whose limit is 100 s).
- **Works behind auth layers.** Per-agent headers (bearer tokens, Cloudflare Access service tokens, …) with
  secrets pulled from environment variables or files, never stored in the client config.

## Tools

| Tool | What it does |
|---|---|
| `a2a_list_agents` | Configured agents, plus tasks sent this session |
| `a2a_discover` | Fetch an agent's Agent Card: description, skills, streaming support |
| `a2a_send` | Send a message/task and return the reply — or a task id if it's still running |
| `a2a_get_task` | Wait for / check on a task |
| `a2a_cancel_task` | Ask the agent to cancel a task |

Pass `context_id` from a reply to `a2a_send` to continue the same conversation. If an agent answers
`input-required`, reply with the same `context_id` and `task_id`.

## Setup

### 1. Get the server

```bash
git clone https://github.com/tylew/a2a-mcp ~/src/a2a-mcp
```

(or `pipx install git+https://github.com/tylew/a2a-mcp` / `uvx --from git+https://github.com/tylew/a2a-mcp a2a-mcp`
if you prefer an `a2a-mcp` command on your PATH).

### 2. Describe your agents

`~/.config/a2a-mcp/agents.json` (or point `A2A_MCP_CONFIG` at another file, or put the same JSON in the
`A2A_MCP_AGENTS` environment variable):

```json
{
  "agents": {
    "hermes": {
      "url": "https://a2a.example.com",
      "description": "My Hermes agent on the home server",
      "headers": {
        "Authorization": "Bearer ${file:~/.config/a2a-mcp/hermes.token}",
        "CF-Access-Client-Id": "${CF_ACCESS_CLIENT_ID}",
        "CF-Access-Client-Secret": "${CF_ACCESS_CLIENT_SECRET}"
      },
      "timeout": 30
    }
  }
}
```

| Field | |
|---|---|
| `url` | Base URL. The Agent Card is read from `/.well-known/agent-card.json` (falls back to `agent.json`). |
| `headers` | Sent on every request. `${VAR}` reads an environment variable, `${file:PATH}` reads a file (trimmed). A missing variable or file is an error, never an empty header. |
| `description` | Shown to the model by `a2a_list_agents`. |
| `timeout` | Seconds for non-streaming requests (default 30). |
| `rpc_url` | Optional: override the JSON-RPC endpoint instead of taking it from the card. |

The file is re-read on every call, so edits apply without restarting your client. Keep secret files
`chmod 600`.

### 3. Register with your client

**Claude Code**

```bash
claude mcp add --scope user a2a -- python3 ~/src/a2a-mcp/a2a_mcp.py
```

**Claude Desktop / Cursor / others** (`mcpServers` JSON):

```json
{
  "mcpServers": {
    "a2a": { "command": "python3", "args": ["/absolute/path/to/a2a_mcp.py"] }
  }
}
```

Then ask: *"use a2a to ask hermes what's on my calendar tomorrow"*.

## Example: Hermes Agent

[Hermes Agent](https://github.com/NousResearch/hermes-agent) ships an A2A platform. Enable it in
`config.yaml`:

```yaml
platforms:
  a2a:
    enabled: true
    extra:
      port: 9900
```

and give this client its own credential in Hermes' `.env`:

```bash
A2A_PEER_TOKENS=claude:<long random token>
A2A_TRUSTED_PEERS=claude
A2A_PUBLIC_URL=https://a2a.example.com   # if it sits behind a proxy/tunnel
```

Hermes keeps listening on `127.0.0.1` only; expose it with an SSH tunnel, Tailscale, or a Cloudflare
Tunnel + Access application. Messages arrive in Hermes' live agent (same memory and tools as its other
platforms), passed through its prompt-injection filters as untrusted peer input.

## Security notes

- **Credentials stay with their origin.** Headers are only sent to the configured `url`'s origin. If an
  Agent Card advertises an endpoint on a *different* origin it is ignored, so a card can't redirect your
  tokens elsewhere. Ad-hoc URLs passed directly to a tool never get configured credentials.
- **Replies are untrusted input.** The server tells the model so, but treat another agent's output the
  way you'd treat a web page.
- **Think about what the remote agent can do.** Sending a task to an agent with shell or account access
  is equivalent to giving the model calling this tool that access, through the remote agent's own
  approval rules.
- Tasks held open in the background live as long as the MCP server process (your client session).

## Development

```bash
python3 -m unittest discover -s tests -v
```

The tests run the real stdio server against mock v1.0 (streaming) and v0.3 (blocking) peers.
Set `A2A_MCP_DEBUG=1` for logs on stderr.

## License

MIT
