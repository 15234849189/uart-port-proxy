# -*- coding: utf-8 -*-
"""精简 MCP 服务端：Streamable HTTP 上的 JSON-RPC 2.0。

不依赖官方 SDK（其要求 Python 3.10+），实现 MCP 客户端所需的最小方法集：
initialize / notifications/initialized / ping / tools/list / tools/call，
兼容 Trae、Claude 等标准 MCP 客户端（HTTP 连接方式）。
"""
import json
import time

from flask import jsonify, request

from . import __version__
from .session import VALID_FLOWS, VALID_MODES, VALID_NEWLINES

SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
LATEST_VERSION = SUPPORTED_VERSIONS[0]

TOOLS = [
    {
        "name": "uart_list_ports",
        "description": "列出本机可用串口（COMx / ttyUSBx）及设备描述",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "uart_open",
        "description": "打开串口，返回 port_id。同一设备重复打开返回既有会话。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port": {"type": "string", "description": "串口名，如 COM3 或 /dev/ttyUSB0"},
                "baudrate": {"type": "integer", "description": "波特率，默认 115200"},
                "bytesize": {"type": "integer", "enum": [7, 8], "description": "数据位，默认 8"},
                "parity": {"type": "string", "enum": ["N", "E", "O", "M", "S"], "description": "校验，默认 N"},
                "stopbits": {"type": "number", "enum": [1, 1.5, 2], "description": "停止位，默认 1"},
                "flow": {"type": "string", "enum": ["none", "rtscts", "xonxoff"], "description": "流控，默认 none"},
                "dtr": {"type": "boolean", "description": "打开时 DTR 初始电平"},
                "rts": {"type": "boolean", "description": "打开时 RTS 初始电平"},
                "auto_reconnect": {"type": "boolean", "description": "断线自动重连，默认 true"},
            },
            "required": ["port"],
        },
    },
    {
        "name": "uart_close",
        "description": "关闭串口会话",
        "inputSchema": {"type": "object",
                        "properties": {"port_id": {"type": "string"}},
                        "required": ["port_id"]},
    },
    {
        "name": "uart_list_active",
        "description": "列出当前已打开的串口会话及统计",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "uart_status",
        "description": "查询单个会话状态（连接状态/波特率/收发字节数/自动化任务状态）",
        "inputSchema": {"type": "object",
                        "properties": {"port_id": {"type": "string"}},
                        "required": ["port_id"]},
    },
    {
        "name": "uart_send",
        "description": "向串口发送数据。mode=text 按会话编码发送；mode=hex 接受 'AA 01' / '0xAA' 等写法。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port_id": {"type": "string"},
                "data": {"type": "string", "description": "要发送的内容"},
                "mode": {"type": "string", "enum": ["text", "hex"]},
                "newline": {"type": "string", "enum": ["none", "cr", "lf", "crlf"]},
            },
            "required": ["port_id", "data"],
        },
    },
    {
        "name": "uart_read",
        "description": "读取串口新收到的数据（内部游标续读）。阻塞至 timeout_ms 或有数据。返回 text/hex 双视图。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port_id": {"type": "string"},
                "timeout_ms": {"type": "integer", "description": "等待时间，默认 300"},
                "max_bytes": {"type": "integer", "description": "单次最多字节，默认 4096"},
            },
            "required": ["port_id"],
        },
    },
    {
        "name": "uart_readline",
        "description": "读取一行（按分隔符，默认换行符）。适合 AT 命令等按行协议。无完整行时返回 found=false。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port_id": {"type": "string"},
                "timeout_ms": {"type": "integer", "description": "默认 1000"},
                "delimiter": {"type": "string", "description": "默认 '\\n'"},
            },
            "required": ["port_id"],
        },
    },
    {
        "name": "uart_run_sequence",
        "description": "按序发送一组指令（阻塞执行完成）。steps=[{data, mode, newline, delay_ms}]，loops 为循环次数，0=无限（此时请改用后台方式，勿阻塞调用）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port_id": {"type": "string"},
                "steps": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "string"},
                            "mode": {"type": "string", "enum": ["text", "hex"]},
                            "newline": {"type": "string", "enum": ["none", "cr", "lf", "crlf"]},
                            "delay_ms": {"type": "integer"},
                        },
                        "required": ["data"],
                    },
                },
                "loops": {"type": "integer", "description": "默认 1；0 表示无限"},
            },
            "required": ["port_id", "steps"],
        },
    },
    {
        "name": "uart_run_script",
        "description": "后台运行 scripts/ 目录下的 Python 脚本（API: send/recv/recv_line/sleep/log/status）。用 uart_script_log 查看输出。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "port_id": {"type": "string"},
                "name": {"type": "string", "description": "脚本文件名，如 example_at.py"},
            },
            "required": ["port_id", "name"],
        },
    },
    {
        "name": "uart_script_log",
        "description": "查询某串口的自动化任务状态与脚本/序列日志",
        "inputSchema": {"type": "object",
                        "properties": {"port_id": {"type": "string"}},
                        "required": ["port_id"]},
    },
    {
        "name": "uart_stop",
        "description": "停止该串口上正在运行的自动化任务（序列或脚本）",
        "inputSchema": {"type": "object",
                        "properties": {"port_id": {"type": "string"}},
                        "required": ["port_id"]},
    },
]


def register_mcp(app, manager, engine):
    def _rpc_error(code, message, msg_id):
        return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}

    def _rpc_result(result, msg_id):
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def _get_session(args):
        session = manager.get(str(args.get("port_id", "")))
        if session is None:
            raise ValueError("串口会话不存在（port_id 无效），可先调用 uart_list_active")
        return session

    # ---------- 工具实现 ----------
    def tool_list_ports(args):
        return {"ports": manager.scan()}

    def tool_open(args):
        port = args.get("port")
        if not port:
            raise ValueError("缺少 port")
        cfg = {"port": str(port)}
        for k in ("baudrate", "bytesize", "stopbits"):
            if args.get(k) is not None:
                cfg[k] = args[k]
        for k in ("parity", "flow", "dtr", "rts", "auto_reconnect"):
            if args.get(k) is not None:
                cfg[k] = args[k]
        if cfg.get("flow") is not None and cfg["flow"] not in VALID_FLOWS:
            raise ValueError("flow 必须是 none/rtscts/xonxoff")
        session, existed = manager.open_port(cfg)
        # 等待连接建立（或首报错误），避免 AI 立刻读写失败
        deadline = time.time() + 3.0
        while time.time() < deadline and session.state == "connecting":
            time.sleep(0.05)
        info = session.status_payload()
        info["existed"] = existed
        return info

    def tool_close(args):
        if not manager.close_port(str(args.get("port_id", ""))):
            raise ValueError("会话不存在")
        return {"ok": True}

    def tool_list_active(args):
        return {"sessions": manager.status_all()}

    def tool_status(args):
        session = _get_session(args)
        info = session.status_payload()
        info["automation"] = engine.get_state(session.port_id)
        return info

    def tool_send(args):
        session = _get_session(args)
        mode = args.get("mode", "text")
        newline = args.get("newline", "none")
        if mode not in VALID_MODES or newline not in VALID_NEWLINES:
            raise ValueError("mode/newline 非法")
        payload = manager.send_data(session, args.get("data", ""), mode, newline)
        return {"bytes_sent": len(payload)}

    def tool_read(args):
        session = _get_session(args)
        timeout_ms = int(args.get("timeout_ms", 300))
        max_bytes = int(args.get("max_bytes", 4096))
        data, cursor = session.read_new(timeout_ms / 1000.0, max_bytes)
        text = None
        try:
            text = data.decode(session.cfg.get("encoding", "utf-8"))
        except Exception:
            try:
                text = data.decode("gbk")
            except Exception:
                text = data.decode("latin-1", errors="replace")
        return {"bytes": len(data), "text": text,
                "hex": data.hex(), "cursor": cursor}

    def tool_readline(args):
        session = _get_session(args)
        timeout_ms = int(args.get("timeout_ms", 1000))
        delimiter = str(args.get("delimiter", "\n")).encode("latin-1") or b"\n"
        line, _cursor, found = session.read_line(session.mcp_cursor, timeout_ms / 1000.0, delimiter)
        if found:
            session.mcp_cursor = _cursor
        try:
            text = line.decode(session.cfg.get("encoding", "utf-8"))
        except Exception:
            text = line.decode("latin-1", errors="replace")
        return {"found": found, "text": text, "hex": line.hex()}

    def tool_run_sequence(args):
        session = _get_session(args)
        steps = args.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError("steps 不能为空")
        cleaned = []
        for s in steps:
            if not isinstance(s, dict) or "data" not in s:
                raise ValueError("步骤缺少 data")
            mode = s.get("mode", "text")
            newline = s.get("newline", "none")
            if mode not in VALID_MODES or newline not in VALID_NEWLINES:
                raise ValueError("步骤 mode/newline 非法")
            cleaned.append({"data": str(s["data"]), "mode": mode, "newline": newline,
                            "delay_ms": int(s.get("delay_ms", 0))})
        loops = int(args.get("loops", 1))
        handle = engine.run_sequence_sync(session, "mcp-sequence", cleaned, loops)
        return {"state": handle.state, "error": handle.error, "logs": handle.get_logs()}

    def tool_run_script(args):
        session = _get_session(args)
        import os
        name = str(args.get("name", ""))
        if not name.endswith(".py"):
            name += ".py"
        fp = os.path.join(engine.scripts_dir, name)
        if not os.path.isfile(fp):
            raise ValueError("脚本不存在: %s" % name)
        with open(fp, "r", encoding="utf-8", errors="replace") as f:
            code = f.read()
        engine.start_script(session, name, code)
        return {"started": True, "name": name,
                "hint": "用 uart_script_log 查询进度，用 uart_stop 停止"}

    def tool_script_log(args):
        port_id = str(args.get("port_id", ""))
        if manager.get(port_id) is None:
            raise ValueError("会话不存在")
        return engine.get_state(port_id)

    def tool_stop(args):
        port_id = str(args.get("port_id", ""))
        if manager.get(port_id) is None:
            raise ValueError("会话不存在")
        return {"stopped": engine.stop(port_id)}

    TOOL_IMPLS = {
        "uart_list_ports": tool_list_ports,
        "uart_open": tool_open,
        "uart_close": tool_close,
        "uart_list_active": tool_list_active,
        "uart_status": tool_status,
        "uart_send": tool_send,
        "uart_read": tool_read,
        "uart_readline": tool_readline,
        "uart_run_sequence": tool_run_sequence,
        "uart_run_script": tool_run_script,
        "uart_script_log": tool_script_log,
        "uart_stop": tool_stop,
    }

    # ---------- JSON-RPC 分发 ----------
    def handle_message(msg):
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            return _rpc_error(-32600, "无效的 JSON-RPC 请求", None)
        method = msg.get("method")
        msg_id = msg.get("id")

        if method == "initialize":
            client_ver = msg.get("params", {}).get("protocolVersion")
            ver = client_ver if client_ver in SUPPORTED_VERSIONS else LATEST_VERSION
            return _rpc_result({
                "protocolVersion": ver,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "uart-proxy", "version": __version__},
            }, msg_id)
        if method == "notifications/initialized":
            return None
        if method == "ping":
            return _rpc_result({}, msg_id)
        if method == "tools/list":
            return _rpc_result({"tools": TOOLS}, msg_id)
        if method == "prompts/list":
            return _rpc_result({"prompts": []}, msg_id)
        if method == "resources/list":
            return _rpc_result({"resources": []}, msg_id)
        if method == "tools/call":
            params = msg.get("params", {}) or {}
            name = params.get("name")
            impl = TOOL_IMPLS.get(name)
            if impl is None:
                return _rpc_error(-32602, "未知工具: %s" % name, msg_id)
            try:
                result = impl(params.get("arguments", {}) or {})
                return _rpc_result({
                    "content": [{"type": "text",
                                 "text": json.dumps(result, ensure_ascii=False, indent=1)}],
                    "isError": False,
                }, msg_id)
            except Exception as e:
                return _rpc_result({
                    "content": [{"type": "text", "text": "错误: %s" % e}],
                    "isError": True,
                }, msg_id)
        if msg_id is None:
            return None  # 未知通知，忽略
        return _rpc_error(-32601, "方法不存在: %s" % method, msg_id)

    @app.route("/mcp", methods=["POST", "GET", "DELETE"])
    def mcp_endpoint():
        if request.method == "DELETE":
            return "", 200
        if request.method == "GET":
            return jsonify({"error": "SSE stream not supported, use POST"}), 405
        payload = request.get_json(force=True, silent=True)
        if payload is None:
            return jsonify(_rpc_error(-32700, "JSON 解析失败", None)), 400

        resp = None
        status = 200
        if isinstance(payload, list):
            results = [r for r in (handle_message(m) for m in payload) if r is not None]
            if results:
                resp = jsonify(results)
            else:
                resp, status = ("", 202)
        else:
            result = handle_message(payload)
            if result is None:
                resp, status = ("", 202)
            else:
                resp = jsonify(result)
        out = app.make_response((resp if resp is not None else "", status))
        out.headers["Mcp-Session-Id"] = "stateless"
        out.headers["MCP-Protocol-Version"] = LATEST_VERSION
        return out
