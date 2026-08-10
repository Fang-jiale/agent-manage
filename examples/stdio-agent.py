#!/usr/bin/env python3
"""
Stdio Agent 示例

AgentClient 通过 stdin 发送 JSON-RPC 请求/通知，本脚本通过 stdout 返回
JSON-RPC 响应与 stream.chunk 通知（每行一条 JSON）。
适合把已有命令行 Agent 接入 AgentClient 的 StdioAdapter。

运行方式：
    node src/client.ts -adapter stdio -local-agent "python3 /path/to/stdio-agent.py"
"""

import sys
import json
import time


def emit(msg: dict):
    print(json.dumps(msg, ensure_ascii=False), flush=True)


def respond(req_id, result):
    emit({"jsonrpc": "2.0", "id": req_id, "result": result})


def send_chunk(chunk: dict):
    emit({"jsonrpc": "2.0", "method": "stream.chunk", "params": chunk})


def text(s: str):
    return [{"type": "text", "text": s}]


def handle_initialize(req: dict):
    respond(req.get("id"), {
        "protocolVersion": "1.0.0",
        "capabilities": {"chat": {}, "streaming": {}, "confirmations": {}},
        "serverInfo": {"name": "stdio-agent-example", "version": "1.0.0"},
    })


def send_register():
    emit({
        "jsonrpc": "2.0",
        "method": "lifecycle.register",
        "params": {
            "agent_id": "stdio-example",
            "capabilities": [
                {"type": "chat", "name": "general", "description": "通用对话能力"},
            ],
        },
    })


def handle_chat(params: dict):
    session_id = params.get("session_id", "")
    task_id = params.get("task_id", "")
    content = params.get("content", "")

    reply = f"收到指令：{content}"
    for i, ch in enumerate(reply):
        send_chunk({
            "task_id": task_id,
            "session_id": session_id,
            "type": "text",
            "content": text(ch),
            "percentage": (i + 1) / len(reply) * 100,
            "done": False,
        })
        time.sleep(0.02)

    # 模拟需要确认
    send_chunk({
        "task_id": task_id,
        "session_id": session_id,
        "type": "confirm_required",
        "confirm_id": f"c-{task_id}",
        "content": text(f"确认执行 '{content}' 吗？"),
    })


def handle_respond(params: dict):
    session_id = params.get("session_id", "")
    task_id = params.get("task_id", "")
    response = params.get("response", "")

    send_chunk({
        "task_id": task_id,
        "session_id": session_id,
        "type": "text",
        "content": text(f"已收到确认：{response}"),
    })
    send_chunk({
        "task_id": task_id,
        "session_id": session_id,
        "type": "text",
        "content": text("继续执行..."),
        "done": True,
    })


def main():
    registered = False
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        method = msg.get("method", "")
        params = msg.get("params") or {}

        if method == "lifecycle.initialize":
            handle_initialize(msg)
        elif method == "lifecycle.initialized":
            if not registered:
                registered = True
                send_register()
        elif method == "task.create":
            handle_chat(params)
        elif method == "task.respond":
            handle_respond(params)


if __name__ == "__main__":
    main()
