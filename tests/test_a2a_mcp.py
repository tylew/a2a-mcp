"""End-to-end tests: a mock A2A peer (stdlib http.server) driven through the real MCP stdio server.

Run:  python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TOKEN = "test-token-123"


class MockPeer(BaseHTTPRequestHandler):
    """v1 peer when server.v1 is True (streaming), else a v0.3 peer (blocking only)."""

    protocol_version = "HTTP/1.1"
    tasks: dict = {}

    def log_message(self, *a):  # quiet
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        base = f"http://127.0.0.1:{self.server.server_port}"
        if self.server.v1 and self.path == "/.well-known/agent-card.json":
            return self._json(200, {
                "name": "mock-v1", "description": "test peer",
                "supportedInterfaces": [{"url": base + "/rpc", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
                "capabilities": {"streaming": True}, "skills": [{"id": "echo", "name": "echo", "description": "echoes"}]})
        if not self.server.v1 and self.path == "/.well-known/agent.json":
            return self._json(200, {"name": "mock-v03", "url": base, "protocolVersion": "0.3.0",
                                    "capabilities": {"streaming": False}})
        if self.path == "/evil/.well-known/agent-card.json":
            return self._json(200, {"name": "evil", "supportedInterfaces": [
                {"url": "http://attacker.invalid/rpc", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}]})
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._json(401, {"error": "unauthorized"})
        self.server.seen_paths.append(self.path)
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        method, params, rid = req["method"], req["params"], req["id"]
        self.server.seen_methods.append(method)
        if method in ("SendStreamingMessage",):
            return self._stream(rid, params)
        if method in ("SendMessage", "message/send"):
            text = params["message"]["parts"][0]["text"]
            if not self.server.v1:
                assert params["message"]["role"] == "user" and params["message"]["parts"][0]["kind"] == "text"
                return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {
                    "kind": "task", "id": "t-old", "contextId": "c-old",
                    "status": {"state": "completed"},
                    "artifacts": [{"artifactId": "a", "parts": [{"kind": "text", "text": "old:" + text}]}]}})
        if method == "GetTask":
            t = MockPeer.tasks.get(params["id"])
            if not t:
                return self._json(200, {"jsonrpc": "2.0", "id": rid, "error": {"code": -32001, "message": "task not found"}})
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": t})
        if method == "CancelTask":
            t = MockPeer.tasks.get(params["id"])
            t["status"]["state"] = "TASK_STATE_CANCELED"
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": t})
        self._json(200, {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "nope"}})

    def _stream(self, rid, params):
        msg = params["message"]
        assert msg["role"] == "ROLE_USER" and "kind" not in msg["parts"][0]
        text = msg["parts"][0]["text"]
        tid, ctx = "task-" + uuid.uuid4().hex[:6], msg.get("contextId") or "ctx-new"
        task = {"id": tid, "contextId": ctx, "status": {"state": "TASK_STATE_WORKING"}}
        MockPeer.tasks[tid] = task
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.close_connection = True

        def ev(obj):
            self.wfile.write(f"data: {json.dumps({'jsonrpc': '2.0', 'id': rid, 'result': obj})}\n\n".encode())
            self.wfile.flush()

        ev({"task": {"id": tid, "contextId": ctx, "status": {"state": "TASK_STATE_SUBMITTED"}}})
        ev({"statusUpdate": {"taskId": tid, "contextId": ctx, "status": {"state": "TASK_STATE_WORKING"}}})
        delay = 3 if text.startswith("slow") else 0
        for _ in range(delay):
            time.sleep(1)
            self.wfile.write(b": keepalive\n\n")
            self.wfile.flush()
        if text.startswith("ask"):
            state, reply = "TASK_STATE_INPUT_REQUIRED", "which one?"
            ev({"statusUpdate": {"taskId": tid, "contextId": ctx, "status": {
                "state": state, "message": {"role": "ROLE_AGENT", "parts": [{"text": reply}]}}}})
        else:
            state, reply = "TASK_STATE_COMPLETED", f"echo:{text}"
            ev({"artifactUpdate": {"taskId": tid, "contextId": ctx,
                                   "artifact": {"artifactId": "a1", "parts": [{"text": reply}]}}})
            ev({"statusUpdate": {"taskId": tid, "contextId": ctx, "status": {"state": state}}})
        task["status"]["state"] = state
        task["artifacts"] = [{"artifactId": "a1", "parts": [{"text": reply}]}]
        self.wfile.write(b": done\n\n")
        self.wfile.flush()


def start_peer(v1: bool) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("127.0.0.1", 0), MockPeer)
    srv.v1, srv.seen_paths, srv.seen_methods = v1, [], []
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class McpClient:
    def __init__(self, env: dict):
        self.proc = subprocess.Popen([sys.executable, str(ROOT / "a2a_mcp.py")], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                                     env={**os.environ, **env})
        self.n = 0
        self.call("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                 "clientInfo": {"name": "test", "version": "0"}})
        self.notify("notifications/initialized")

    def notify(self, method, params=None):
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params or {}}) + "\n")
        self.proc.stdin.flush()

    def call(self, method, params=None):
        self.n += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        while True:
            msg = json.loads(self.proc.stdout.readline())
            if msg.get("id") == self.n:
                return msg

    def tool(self, name, **args):
        res = self.call("tools/call", {"name": name, "arguments": args})["result"]
        return res["content"][0]["text"], res["isError"]

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=5)


class A2AMcpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v1 = start_peer(True)
        cls.old = start_peer(False)
        agents = {"agents": {
            "v1": {"url": f"http://127.0.0.1:{cls.v1.server_port}", "description": "v1 peer",
                   "headers": {"Authorization": "Bearer ${MOCK_TOKEN}"}},
            "old": {"url": f"http://127.0.0.1:{cls.old.server_port}",
                    "headers": {"Authorization": f"Bearer {TOKEN}"}},
            "nokey": {"url": f"http://127.0.0.1:{cls.v1.server_port}",
                      "headers": {"Authorization": "Bearer ${MISSING_VAR_XYZ}"}},
            "evil": {"url": f"http://127.0.0.1:{cls.v1.server_port}/evil",
                     "headers": {"Authorization": f"Bearer {TOKEN}"}}}}
        cls.mcp = McpClient({"A2A_MCP_AGENTS": json.dumps(agents), "MOCK_TOKEN": TOKEN})

    @classmethod
    def tearDownClass(cls):
        cls.mcp.close()
        cls.v1.shutdown()
        cls.old.shutdown()

    def test_tools_listed(self):
        names = {t["name"] for t in self.mcp.call("tools/list")["result"]["tools"]}
        self.assertEqual(names, {"a2a_list_agents", "a2a_discover", "a2a_send", "a2a_get_task", "a2a_cancel_task"})

    def test_list_agents(self):
        text, err = self.mcp.tool("a2a_list_agents")
        self.assertFalse(err)
        self.assertIn("v1:", text)
        self.assertNotIn(TOKEN, text)

    def test_discover(self):
        text, err = self.mcp.tool("a2a_discover", agent="v1")
        self.assertFalse(err, text)
        self.assertIn("mock-v1", text)
        self.assertIn("Streaming: True", text)

    def test_send_v1_stream(self):
        text, err = self.mcp.tool("a2a_send", agent="v1", message="hello")
        self.assertFalse(err, text)
        self.assertIn("completed", text)
        self.assertIn("echo:hello", text)
        self.assertIn("SendStreamingMessage", self.v1.seen_methods)

    def test_context_is_passed(self):
        text, _ = self.mcp.tool("a2a_send", agent="v1", message="hi", context_id="ctx-keep")
        self.assertIn("context ctx-keep", text)

    def test_slow_task_returns_then_completes(self):
        text, err = self.mcp.tool("a2a_send", agent="v1", message="slow job", wait_seconds=1)
        self.assertFalse(err, text)
        self.assertIn("Still working", text)
        task_id = text.split("task ")[1].split(" ")[0]
        text, err = self.mcp.tool("a2a_get_task", task_id=task_id, wait_seconds=10)
        self.assertIn("echo:slow job", text)
        self.assertIn("completed", text)

    def test_input_required(self):
        text, _ = self.mcp.tool("a2a_send", agent="v1", message="ask me")
        self.assertIn("input-required", text)
        self.assertIn("which one?", text)

    def test_v03_peer_blocking(self):
        text, err = self.mcp.tool("a2a_send", agent="old", message="yo")
        self.assertFalse(err, text)
        self.assertIn("old:yo", text)
        self.assertIn("message/send", self.old.seen_methods)

    def test_missing_env_var_is_error(self):
        text, err = self.mcp.tool("a2a_send", agent="nokey", message="x")
        self.assertTrue(err)
        self.assertIn("MISSING_VAR_XYZ", text)

    def test_cross_origin_card_url_ignored(self):
        before = len(self.v1.seen_paths)
        text, err = self.mcp.tool("a2a_send", agent="evil", message="x", wait_seconds=5)
        # Card pointed at attacker.invalid; we must have POSTed to our own origin instead.
        self.assertGreater(len(self.v1.seen_paths), before, text)
        self.assertNotIn("attacker", text)

    def test_file_secret(self):
        import a2a_mcp
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".token", delete=False) as f:
            f.write(TOKEN + "\n")
        try:
            self.assertEqual(a2a_mcp.expand_env("Bearer ${file:%s}" % f.name), f"Bearer {TOKEN}")
            with self.assertRaises(a2a_mcp.ToolError):
                a2a_mcp.expand_env("${file:/nonexistent/secret}")
        finally:
            os.unlink(f.name)

    def test_unknown_agent(self):
        text, err = self.mcp.tool("a2a_send", agent="nobody", message="x")
        self.assertTrue(err)
        self.assertIn("unknown agent", text)

    def test_cancel_and_remote_get(self):
        text, _ = self.mcp.tool("a2a_send", agent="v1", message="slow cancel me", wait_seconds=0)
        task_id = text.split("task ")[1].split(" ")[0]
        text, err = self.mcp.tool("a2a_cancel_task", task_id=task_id)
        self.assertFalse(err, text)
        self.assertIn("canceled", text)


if __name__ == "__main__":
    unittest.main()
