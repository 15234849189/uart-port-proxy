# -*- coding: utf-8 -*-
"""Web 层：Flask REST + WebSocket 实时通道 + 访问令牌。"""
import json
import os
import queue
import re
import threading

from flask import Flask, abort, jsonify, request, send_file, send_from_directory
from flask_sock import Sock

from . import __version__
from .session import (VALID_ENCODINGS, VALID_FLOWS, VALID_MODES,
                      VALID_NEWLINES, sanitize_name)

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
SAFE_SCRIPT = re.compile(r"^[A-Za-z0-9_\-]+\.py$")
SAFE_RECORD = re.compile(r"^[A-Za-z0-9_.\-]+\.log$")


class WsClient(object):
    """一个浏览器连接：收线程在 flask-sock 路由里，发线程走独立队列。"""

    def __init__(self, ws):
        self.ws = ws
        self.q = queue.Queue(maxsize=20000)
        self.closed = False
        self.sender = threading.Thread(target=self._sender, daemon=True)

    def enqueue(self, msg):
        if self.closed:
            return
        try:
            self.q.put_nowait(msg)
        except queue.Full:
            pass  # 慢客户端直接丢帧，避免阻塞串口线程

    def _sender(self):
        while not self.closed:
            try:
                msg = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self.ws.send(json.dumps(msg, ensure_ascii=False))
            except Exception:
                self.closed = True
                try:
                    self.ws.close()
                except Exception:
                    pass


def create_app(manager, config, engine, scripts_dir, logs_dir, token):
    app = Flask(__name__, static_folder=None)
    sock = Sock(app)
    token = token or ""

    # ---------- 鉴权 ----------
    def authorized():
        if not token:
            return True
        t = request.args.get("token") or request.headers.get("X-Token") or ""
        if not t:
            auth = request.headers.get("Authorization") or ""
            if auth.startswith("Bearer "):
                t = auth[7:]
        return t == token

    @app.before_request
    def _auth_gate():
        p = request.path
        if p.startswith("/api") or p.startswith("/mcp") or p == "/ws":
            if not authorized():
                return jsonify({"error": "unauthorized"}), 401
        return None

    # ---------- 静态页 ----------
    @app.route("/")
    def index():
        return send_from_directory(WEB_DIR, "index.html")

    @app.route("/<path:fname>")
    def static_files(fname):
        return send_from_directory(WEB_DIR, fname)

    # ---------- 串口 ----------
    @app.route("/api/ports", methods=["GET"])
    def api_ports():
        busy = {}
        for s in manager.status_all():
            if s["state"] != "closed":
                busy[s["cfg"]["port"]] = s["port_id"]
        items = []
        for p in manager.scan():
            p["open"] = p["port"] in busy
            p["port_id"] = busy.get(p["port"])
            p["saved_cfg"] = config.get_device_cfg(p["port"])
            items.append(p)
        return jsonify({"ports": items})

    @app.route("/api/ports/open", methods=["POST"])
    def api_open():
        body = request.get_json(force=True, silent=True) or {}
        try:
            session, existed = manager.open_port(body)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"port_id": session.port_id, "existed": existed})

    @app.route("/api/ports/<pid>/close", methods=["POST"])
    def api_close(pid):
        if manager.close_port(pid):
            return jsonify({"ok": True})
        abort(404)

    def _get_session(pid):
        s = manager.get(pid)
        if s is None:
            abort(404)
        return s

    @app.route("/api/ports/<pid>/dtr", methods=["POST"])
    def api_dtr(pid):
        s = _get_session(pid)
        v = bool((request.get_json(force=True, silent=True) or {}).get("value"))
        try:
            s.set_dtr(v)
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 409
        return jsonify({"ok": True, "dtr": v})

    @app.route("/api/ports/<pid>/rts", methods=["POST"])
    def api_rts(pid):
        s = _get_session(pid)
        v = bool((request.get_json(force=True, silent=True) or {}).get("value"))
        try:
            s.set_rts(v)
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 409
        return jsonify({"ok": True, "rts": v})

    @app.route("/api/ports/<pid>/encoding", methods=["POST"])
    def api_encoding(pid):
        s = _get_session(pid)
        v = (request.get_json(force=True, silent=True) or {}).get("encoding")
        try:
            s.set_encoding(v)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True, "encoding": v})

    @app.route("/api/ports/<pid>/record", methods=["POST"])
    def api_record(pid):
        s = _get_session(pid)
        enable = bool((request.get_json(force=True, silent=True) or {}).get("enable"))
        try:
            if enable:
                s.start_record()
            else:
                s.stop_record()
        except (IOError, OSError) as e:
            return jsonify({"error": "记录文件创建失败: %s" % e}), 500
        return jsonify({"ok": True, "recording": s.recording})

    # ---------- 记录文件 ----------
    @app.route("/api/records", methods=["GET"])
    def api_records():
        items = []
        if os.path.isdir(logs_dir):
            for n in os.listdir(logs_dir):
                if not n.endswith(".log"):
                    continue
                fp = os.path.join(logs_dir, n)
                try:
                    st = os.stat(fp)
                    items.append({"name": n, "size": st.st_size, "mtime": int(st.st_mtime * 1000)})
                except OSError:
                    pass
        items.sort(key=lambda x: -x["mtime"])
        return jsonify({"records": items, "dir": logs_dir})

    @app.route("/api/records/<name>", methods=["GET"])
    def api_record_download(name):
        if not SAFE_RECORD.match(name):
            abort(400)
        fp = os.path.join(logs_dir, name)
        if not os.path.isfile(fp):
            abort(404)
        return send_file(fp, as_attachment=True, attachment_filename=name)

    # ---------- 指令库 ----------
    @app.route("/api/commands", methods=["GET", "POST"])
    def api_commands():
        if request.method == "GET":
            return jsonify({"commands": config.get_commands()})
        body = request.get_json(force=True, silent=True) or {}
        name = str(body.get("name", "")).strip()
        data = str(body.get("data", ""))
        mode = body.get("mode", "text")
        newline = body.get("newline", "none")
        if not name or data == "":
            return jsonify({"error": "名称与内容不能为空"}), 400
        if mode not in VALID_MODES or newline not in VALID_NEWLINES:
            return jsonify({"error": "mode/newline 非法"}), 400
        return jsonify({"command": config.add_command(name, data, mode, newline)})

    @app.route("/api/commands/<cmd_id>", methods=["PUT", "DELETE"])
    def api_command_item(cmd_id):
        if request.method == "DELETE":
            if config.delete_command(cmd_id):
                return jsonify({"ok": True})
            abort(404)
        body = request.get_json(force=True, silent=True) or {}
        item = config.update_command(cmd_id, body)
        if item is None:
            abort(404)
        return jsonify({"command": item})

    # ---------- 指令序列 ----------
    @app.route("/api/sequences", methods=["GET", "POST"])
    def api_sequences():
        if request.method == "GET":
            return jsonify({"sequences": config.get_sequences()})
        body = request.get_json(force=True, silent=True) or {}
        name = str(body.get("name", "")).strip()
        steps = body.get("steps") or []
        if not name or not isinstance(steps, list) or not steps:
            return jsonify({"error": "名称与步骤不能为空"}), 400
        cleaned = _clean_steps(steps)
        if cleaned is None:
            return jsonify({"error": "步骤格式非法"}), 400
        loops = body.get("loops", 1)
        return jsonify({"sequence": config.add_sequence(name, cleaned, loops)})

    @app.route("/api/sequences/<seq_id>", methods=["PUT", "DELETE"])
    def api_sequence_item(seq_id):
        if request.method == "DELETE":
            if config.delete_sequence(seq_id):
                return jsonify({"ok": True})
            abort(404)
        body = request.get_json(force=True, silent=True) or {}
        if "steps" in body:
            cleaned = _clean_steps(body["steps"])
            if cleaned is None:
                return jsonify({"error": "步骤格式非法"}), 400
            body["steps"] = cleaned
        item = config.update_sequence(seq_id, body)
        if item is None:
            abort(404)
        return jsonify({"sequence": item})

    @app.route("/api/sequences/<seq_id>/start", methods=["POST"])
    def api_sequence_start(seq_id):
        body = request.get_json(force=True, silent=True) or {}
        session = manager.get(body.get("port_id", ""))
        if session is None:
            return jsonify({"error": "串口会话不存在"}), 404
        seq = None
        for s in config.get_sequences():
            if s["id"] == seq_id:
                seq = s
                break
        if seq is None:
            abort(404)
        try:
            engine.start_sequence(session, seq["name"], seq["steps"], seq.get("loops", 1))
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 409
        return jsonify({"ok": True})

    @app.route("/api/automation/stop", methods=["POST"])
    def api_automation_stop():
        body = request.get_json(force=True, silent=True) or {}
        ok = engine.stop(body.get("port_id", ""))
        return jsonify({"ok": ok})

    # ---------- 脚本 ----------
    @app.route("/api/scripts", methods=["GET", "POST"])
    def api_scripts():
        if request.method == "GET":
            items = []
            if os.path.isdir(scripts_dir):
                for n in sorted(os.listdir(scripts_dir)):
                    if not n.endswith(".py"):
                        continue
                    fp = os.path.join(scripts_dir, n)
                    try:
                        st = os.stat(fp)
                        items.append({"name": n, "size": st.st_size, "mtime": int(st.st_mtime * 1000)})
                    except OSError:
                        pass
            return jsonify({"scripts": items, "dir": scripts_dir})
        body = request.get_json(force=True, silent=True) or {}
        name = str(body.get("name", "")).strip()
        if not name.endswith(".py"):
            name += ".py"
        if not SAFE_SCRIPT.match(name):
            return jsonify({"error": "脚本名仅允许字母数字下划线"}), 400
        os.makedirs(scripts_dir, exist_ok=True)
        fp = os.path.join(scripts_dir, name)
        if os.path.exists(fp):
            return jsonify({"error": "脚本已存在"}), 409
        with open(fp, "w", encoding="utf-8") as f:
            f.write(str(body.get("content", "# 新脚本\n")))
        return jsonify({"ok": True, "name": name})

    @app.route("/api/scripts/<name>", methods=["GET", "PUT", "DELETE"])
    def api_script_item(name):
        if not SAFE_SCRIPT.match(name):
            abort(400)
        fp = os.path.join(scripts_dir, name)
        if request.method == "GET":
            if not os.path.isfile(fp):
                abort(404)
            with open(fp, "r", encoding="utf-8", errors="replace") as f:
                return jsonify({"name": name, "content": f.read()})
        if request.method == "PUT":
            if not os.path.isfile(fp):
                abort(404)
            content = str((request.get_json(force=True, silent=True) or {}).get("content", ""))
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            return jsonify({"ok": True})
        if os.path.isfile(fp):
            os.remove(fp)
            return jsonify({"ok": True})
        abort(404)

    @app.route("/api/scripts/<name>/run", methods=["POST"])
    def api_script_run(name):
        if not SAFE_SCRIPT.match(name):
            abort(400)
        fp = os.path.join(scripts_dir, name)
        if not os.path.isfile(fp):
            abort(404)
        body = request.get_json(force=True, silent=True) or {}
        session = manager.get(body.get("port_id", ""))
        if session is None:
            return jsonify({"error": "串口会话不存在"}), 404
        with open(fp, "r", encoding="utf-8", errors="replace") as f:
            code = f.read()
        try:
            engine.start_script(session, name, code)
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 409
        return jsonify({"ok": True})

    @app.route("/api/automation/state/<pid>", methods=["GET"])
    def api_automation_state(pid):
        return jsonify(engine.get_state(pid))

    # ---------- 偏好 / 总状态 ----------
    @app.route("/api/prefs", methods=["PUT"])
    def api_prefs():
        body = request.get_json(force=True, silent=True) or {}
        return jsonify({"prefs": config.update_prefs(body)})

    @app.route("/api/state", methods=["GET"])
    def api_state():
        return jsonify({
            "version": __version__,
            "sessions": manager.status_all(),
            "commands": config.get_commands(),
            "sequences": config.get_sequences(),
            "prefs": config.get_prefs(),
            "scripts_dir": scripts_dir,
            "logs_dir": logs_dir,
        })

    # ---------- WebSocket ----------
    @sock.route("/ws")
    def ws_route(ws):
        client = WsClient(ws)
        manager.add_client(client)
        client.sender.start()
        client.enqueue({
            "t": "hello",
            "version": __version__,
            "sessions": manager.status_all(),
            "commands": config.get_commands(),
            "sequences": config.get_sequences(),
            "prefs": config.get_prefs(),
        })
        try:
            while True:
                raw = ws.receive()
                if raw is None:
                    break
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                t = msg.get("t")
                if t == "ping":
                    client.enqueue({"t": "pong"})
                elif t == "send":
                    session = manager.get(msg.get("port_id", ""))
                    if session is None:
                        client.enqueue({"t": "error", "message": "串口会话不存在"})
                        continue
                    try:
                        manager.send_data(session, msg.get("data", ""),
                                          msg.get("mode", "text"),
                                          msg.get("newline", "none"))
                    except (ValueError, RuntimeError) as e:
                        client.enqueue({"t": "error", "port_id": session.port_id,
                                        "message": str(e)})
        finally:
            manager.remove_client(client)
            client.closed = True
            try:
                ws.close()
            except Exception:
                pass

    # ---------- MCP ----------
    from .mcp_server import register_mcp
    register_mcp(app, manager, engine)

    return app, sock


def _clean_steps(steps):
    """校验/规整序列步骤：[{data,mode,newline,delay_ms}]。"""
    out = []
    for s in steps:
        if not isinstance(s, dict):
            return None
        data = str(s.get("data", ""))
        mode = s.get("mode", "text")
        newline = s.get("newline", "none")
        if mode not in VALID_MODES or newline not in VALID_NEWLINES:
            return None
        try:
            delay_ms = int(s.get("delay_ms", 0))
        except (TypeError, ValueError):
            return None
        if delay_ms < 0 or delay_ms > 3600000:
            return None
        out.append({"data": data, "mode": mode, "newline": newline, "delay_ms": delay_ms})
    return out
