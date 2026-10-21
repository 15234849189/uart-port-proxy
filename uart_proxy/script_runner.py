# -*- coding: utf-8 -*-
"""自动化引擎：指令序列（按序+延时+循环）与 Python 脚本（send/recv/sleep/log API）。

每个串口同一时间只允许一个自动化任务。脚本运行在专用线程，通过 stop_evt
协作式停止：sleep/recv 均可中断；纯 CPU 死循环无法中断（脚本应避免）。
"""
import threading
import time
import traceback


class AutomationStopped(Exception):
    """脚本被手动停止。"""


class AutomationHandle(object):
    def __init__(self, kind, name):
        self.kind = kind          # "seq" / "script"
        self.name = name
        self.stop_evt = threading.Event()
        self.log_lines = []       # 最多保留 500 行
        self.log_lock = threading.Lock()
        self.state = "running"    # running/done/stopped/error
        self.error = None

    def add_log(self, line):
        with self.log_lock:
            self.log_lines.append(line)
            if len(self.log_lines) > 500:
                del self.log_lines[:-500]

    def get_logs(self):
        with self.log_lock:
            return list(self.log_lines)


class AutomationEngine(object):
    def __init__(self, manager, scripts_dir):
        self.manager = manager
        self.scripts_dir = scripts_dir
        self.lock = threading.RLock()
        self.handles = {}   # port_id -> AutomationHandle

    # ---------- 公共 ----------
    def _acquire(self, port_id, kind, name):
        with self.lock:
            old = self.handles.get(port_id)
            if old is not None and old.state == "running":
                raise RuntimeError("该串口已有自动化任务在运行（%s），请先停止" % old.name)
            handle = AutomationHandle(kind, name)
            self.handles[port_id] = handle
            return handle

    def _finish(self, port_id, handle):
        with self.lock:
            if self.handles.get(port_id) is handle:
                pass  # 保留 handle 供日志查询，状态已非 running
        self._broadcast_state(port_id, handle)

    def stop(self, port_id):
        with self.lock:
            handle = self.handles.get(port_id)
        if handle is not None and handle.state == "running":
            handle.stop_evt.set()
            return True
        return False

    def get_state(self, port_id):
        with self.lock:
            handle = self.handles.get(port_id)
        if handle is None:
            return {"state": "idle"}
        return {"state": handle.state, "kind": handle.kind, "name": handle.name,
                "error": handle.error, "logs": handle.get_logs()}

    def _broadcast_state(self, port_id, handle):
        self.manager.broadcast({
            "t": "automation",
            "port_id": port_id,
            "kind": handle.kind,
            "name": handle.name,
            "state": handle.state,
            "error": handle.error,
        })

    def _emit_log(self, port_id, handle, line):
        handle.add_log(line)
        self.manager.broadcast({
            "t": "script_log",
            "port_id": port_id,
            "name": handle.name,
            "line": str(line),
        })

    # ---------- 指令序列 ----------
    def start_sequence(self, session, seq_name, steps, loops):
        handle = self._acquire(session.port_id, "seq", seq_name)

        def target():
            self._sequence_core(session, handle, steps, loops)

        th = threading.Thread(target=target, name="seq-%s" % session.port_id[:8], daemon=True)
        handle.state = "running"
        self._broadcast_state(session.port_id, handle)
        th.start()
        return handle

    def run_sequence_sync(self, session, seq_name, steps, loops):
        """阻塞执行（MCP 使用），返回 handle。"""
        handle = self._acquire(session.port_id, "seq", seq_name)
        handle.state = "running"
        self._sequence_core(session, handle, steps, loops)
        return handle

    def _sequence_core(self, session, handle, steps, loops):
        port_id = session.port_id
        try:
            if not isinstance(steps, list) or not steps:
                raise ValueError("序列步骤为空")
            loops = int(loops) if loops is not None else 1
            infinite = loops <= 0
            loop_no = 0
            while infinite or loop_no < loops:
                if handle.stop_evt.is_set():
                    handle.state = "stopped"
                    break
                for si, step in enumerate(steps):
                    if handle.stop_evt.is_set():
                        handle.state = "stopped"
                        break
                    self.manager.send_data(
                        session,
                        step.get("data", ""),
                        step.get("mode", "text"),
                        step.get("newline", "none"),
                    )
                    delay_ms = int(step.get("delay_ms", 0))
                    if delay_ms > 0:
                        # 分片睡眠，保证可停止
                        end = time.time() + delay_ms / 1000.0
                        while time.time() < end:
                            if handle.stop_evt.is_set():
                                break
                            handle.stop_evt.wait(min(0.1, end - time.time()))
                    self._emit_log(port_id, handle,
                                   "[loop %d step %d] 已发送" % (loop_no + 1, si + 1))
                if handle.state == "stopped":
                    break
                loop_no += 1
            if handle.state == "running":
                handle.state = "done"
        except Exception as e:
            handle.state = "error"
            handle.error = str(e)
            self._emit_log(port_id, handle, "错误: %s" % e)
        finally:
            self._finish(port_id, handle)

    # ---------- Python 脚本 ----------
    def start_script(self, session, script_name, code):
        handle = self._acquire(session.port_id, "script", script_name)

        def target():
            self._script_core(session, handle, script_name, code)

        th = threading.Thread(target=target, name="script-%s" % session.port_id[:8], daemon=True)
        handle.state = "running"
        self._broadcast_state(session.port_id, handle)
        th.start()
        return handle

    def _script_core(self, session, handle, script_name, code):
        port_id = session.port_id
        api = _ScriptApi(self.manager, session, handle, self)
        env = {
            "__name__": "__uart_script__",
            "send": api.send,
            "send_hex": api.send_hex,
            "recv": api.recv,
            "recv_line": api.recv_line,
            "sleep": api.sleep,
            "log": api.log,
            "print": api.print_,
            "status": api.status,
        }
        try:
            exec(compile(code, "<uart-script:%s>" % script_name, "exec"), env)
            if handle.state == "running":
                handle.state = "done"
        except AutomationStopped:
            handle.state = "stopped"
        except Exception:
            handle.state = "error"
            handle.error = traceback.format_exc(limit=3).strip()
            self._emit_log(port_id, handle, "脚本异常:\n%s" % handle.error)
        finally:
            self._finish(port_id, handle)


class _ScriptApi(object):
    """注入脚本执行环境的 API 集合。"""

    def __init__(self, manager, session, handle, engine):
        self.manager = manager
        self.session = session
        self.handle = handle
        self.engine = engine
        self.rx_cursor = session.rx_index
        self.line_buf = b""

    def _check(self):
        if self.handle.stop_evt.is_set():
            raise AutomationStopped()
        if self.session.state not in ("open", "reconnecting", "connecting"):
            raise RuntimeError("串口已关闭")

    # -- 发送 --
    def send(self, data, mode="text", newline="none"):
        self._check()
        return self.manager.send_data(self.session, data, mode, newline)

    def send_hex(self, data):
        return self.send(data, mode="hex", newline="none")

    # -- 接收 --
    def recv(self, timeout=1.0, max_bytes=4096):
        """等待数据；收到首批数据后再等 50ms 汇总，返回 {"text","hex"} 或 None。"""
        self._check()
        deadline = time.time() + float(timeout)
        buf = b""
        while time.time() < deadline:
            self._check()
            chunk, self.rx_cursor = self.session.read_from(self.rx_cursor, 0.05, max_bytes)
            if chunk:
                buf += chunk
                # 首包到达后再小幅汇聚
                grace_end = time.time() + 0.05
                while time.time() < grace_end and len(buf) < max_bytes:
                    chunk, self.rx_cursor = self.session.read_from(self.rx_cursor, 0.01, max_bytes)
                    if chunk:
                        buf += chunk
                    else:
                        break
                return {"text": self._decode(buf), "hex": buf.hex()}
        return None

    def recv_line(self, timeout=1.0, delimiter="\n"):
        """按分隔符读一行（跨包缓冲），返回 {"text","hex"} 或 None。"""
        self._check()
        delim = delimiter.encode("latin-1") if isinstance(delimiter, str) else bytes(delimiter)
        deadline = time.time() + float(timeout)
        while time.time() < deadline:
            self._check()
            pos = self.line_buf.find(delim)
            if pos >= 0:
                line = self.line_buf[:pos]
                self.line_buf = self.line_buf[pos + len(delim):]
                if delim == b"\n" and line.endswith(b"\r"):
                    line = line[:-1]
                return {"text": self._decode(line), "hex": line.hex()}
            chunk, self.rx_cursor = self.session.read_from(self.rx_cursor, 0.05, 8192)
            if chunk:
                self.line_buf += chunk
                if len(self.line_buf) > 1024 * 1024:
                    self.line_buf = self.line_buf[-64 * 1024:]
        return None

    @staticmethod
    def _decode(b):
        enc = "utf-8"
        try:
            return b.decode(enc)
        except Exception:
            try:
                return b.decode("gbk")
            except Exception:
                return b.decode("latin-1", errors="replace")

    # -- 其他 --
    def sleep(self, sec):
        """可中断睡眠。"""
        if self.handle.stop_evt.wait(float(sec)):
            raise AutomationStopped()

    def log(self, msg):
        self.engine._emit_log(self.session.port_id, self.handle, msg)

    def print_(self, *args, **kwargs):
        sep = kwargs.get("sep", " ")
        self.log(sep.join(str(a) for a in args))

    def status(self):
        return self.session.status_payload()
