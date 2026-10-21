# -*- coding: utf-8 -*-
"""串口会话：单个串口连接的收发线程、环形缓冲、统计、断线重连、记录落盘。"""
import base64
import collections
import os
import re
import threading
import time

import serial
from serial.serialutil import SerialException

PARITY_MAP = {
    "N": serial.PARITY_NONE,
    "E": serial.PARITY_EVEN,
    "O": serial.PARITY_ODD,
    "M": serial.PARITY_MARK,
    "S": serial.PARITY_SPACE,
}

VALID_MODES = ("text", "hex")
VALID_NEWLINES = ("none", "cr", "lf", "crlf")
VALID_FLOWS = ("none", "rtscts", "xonxoff")
VALID_ENCODINGS = ("utf-8", "gbk", "big5", "latin-1")

NEWLINE_MAP = {"none": "", "cr": "\r", "lf": "\n", "crlf": "\r\n"}


def now_ms():
    return int(time.time() * 1000)


def parse_hex(text):
    """"AA BB" / "0xAA,0x01" / "aabb" -> bytes；非法时抛 ValueError。"""
    t = re.sub(r"[\s,;:]", "", str(text))
    while True:
        t2 = re.sub(r"0[xX]", "", t)
        if t2 == t:
            break
        t = t2
    if t == "" or len(t) % 2 != 0 or re.search(r"[^0-9a-fA-F]", t):
        raise ValueError("HEX 格式错误：%s" % text)
    return bytes.fromhex(t)


def build_payload(data, mode="text", newline="none", encoding="utf-8"):
    """按发送模式与行尾符组装最终字节。"""
    if mode not in VALID_MODES:
        raise ValueError("mode 必须是 text/hex")
    if newline not in VALID_NEWLINES:
        raise ValueError("newline 必须是 none/cr/lf/crlf")
    if mode == "hex":
        payload = parse_hex(data)
    else:
        payload = str(data).encode(encoding, errors="replace")
    nl = NEWLINE_MAP[newline]
    if nl:
        payload = payload + nl.encode("ascii")
    return payload


def sanitize_name(s):
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", str(s))


class PortSession(object):
    """一个串口连接的全部状态与后台线程。"""

    def __init__(self, port_id, cfg, manager, logs_dir):
        self.port_id = port_id
        self.cfg = dict(cfg)
        self.manager = manager
        self.logs_dir = logs_dir

        self.ser = None
        self.state = "connecting"   # connecting/open/reconnecting/closed/error
        self.error = None
        self.rx_bytes = 0
        self.tx_bytes = 0
        self.connected_since = None
        self.recording = False

        self.stop_evt = threading.Event()
        self.thread = None
        self.record_fp = None
        self.record_lock = threading.Lock()

        # RX 环形缓冲：deque[(idx, ts, bytes)]，供脚本/MCP 按游标读取
        self.rx_cond = threading.Condition()
        self.rx_ring = collections.deque(maxlen=4096)
        self.rx_index = 0
        self.mcp_cursor = 0
        self._last_stats_ms = 0

    # ---------- 生命周期 ----------
    def start(self):
        self.stop_evt.clear()
        self.thread = threading.Thread(
            target=self._run,
            name="uart-%s" % self.port_id[:8],
            daemon=True,
        )
        self.thread.start()

    def close(self):
        self.stop_evt.set()
        self._close_serial()
        if self.thread is not None:
            self.thread.join(timeout=3.0)
        self.stop_record()
        self.state = "closed"
        self.broadcast_status()

    def _close_serial(self):
        ser = self.ser
        self.ser = None
        if ser is not None:
            try:
                ser.close()
            except Exception:
                pass

    def _open_serial(self):
        cfg = self.cfg
        flow = cfg.get("flow", "none")
        ser = serial.Serial(
            port=cfg["port"],
            baudrate=int(cfg.get("baudrate", 115200)),
            bytesize=int(cfg.get("bytesize", 8)),
            parity=PARITY_MAP.get(cfg.get("parity", "N"), serial.PARITY_NONE),
            stopbits=float(cfg.get("stopbits", 1)),
            xonxoff=(flow == "xonxoff"),
            rtscts=(flow == "rtscts"),
            timeout=0.05,          # 读超时：保证线程响应度
            write_timeout=2.0,
        )
        if "dtr" in cfg and cfg["dtr"] is not None:
            ser.dtr = bool(cfg["dtr"])
        if "rts" in cfg and cfg["rts"] is not None:
            ser.rts = bool(cfg["rts"])
        return ser

    # ---------- 后台线程 ----------
    def _run(self):
        auto_reconnect = bool(self.cfg.get("auto_reconnect", True))
        while not self.stop_evt.is_set():
            if self.ser is None:
                try:
                    self.ser = self._open_serial()
                except Exception as e:
                    self.error = str(e)
                    if not auto_reconnect:
                        self.state = "error"
                        self.broadcast_status()
                        return
                    self.state = "reconnecting"
                    self.broadcast_status()
                    if self.stop_evt.wait(2.0):
                        return
                    continue
                self.state = "open"
                self.error = None
                self.connected_since = now_ms()
                self._last_stats_ms = 0
                self.broadcast_status()

            try:
                data = self.ser.read(4096)
                if data:
                    self._on_rx(data)
                ts = now_ms()
                if ts - self._last_stats_ms >= 500:
                    self._last_stats_ms = ts
                    self.broadcast_stats()
            except (SerialException, OSError, ValueError) as e:
                self._close_serial()
                self.error = str(e)
                if not auto_reconnect:
                    self.state = "error"
                    self.broadcast_status()
                    return
                self.state = "reconnecting"
                self.connected_since = None
                self.broadcast_status()
                if self.stop_evt.wait(2.0):
                    return

    # ---------- 数据接收 ----------
    def _on_rx(self, data):
        ts = now_ms()
        self.rx_bytes += len(data)
        with self.rx_cond:
            self.rx_ring.append((self.rx_index, ts, data))
            self.rx_index += 1
            self.rx_cond.notify_all()
        if self.recording:
            self._record_line("RX", data, ts)
        self.manager.broadcast_data(self, "rx", data, ts)

    # ---------- 发送 ----------
    def write(self, payload):
        if self.ser is None or self.state != "open":
            raise RuntimeError("串口未连接")
        self.ser.write(payload)
        ts = now_ms()
        self.tx_bytes += len(payload)
        if self.recording:
            self._record_line("TX", payload, ts)
        self.manager.broadcast_data(self, "tx", payload, ts)

    # ---------- DTR/RTS ----------
    def set_dtr(self, value):
        if self.ser is None:
            raise RuntimeError("串口未连接")
        self.ser.dtr = bool(value)
        self.cfg["dtr"] = bool(value)
        self.broadcast_status()

    def set_rts(self, value):
        if self.ser is None:
            raise RuntimeError("串口未连接")
        self.ser.rts = bool(value)
        self.cfg["rts"] = bool(value)
        self.broadcast_status()

    def set_encoding(self, encoding):
        if encoding not in VALID_ENCODINGS:
            raise ValueError("不支持的编码")
        self.cfg["encoding"] = encoding
        self.manager.config.remember_device(self.cfg["port"], self.cfg)

    # ---------- RX 游标读取（脚本 / MCP） ----------
    def read_from(self, cursor, timeout=0.3, max_bytes=4096):
        """读取游标之后的数据，返回 (data, new_cursor)。阻塞至超时。"""
        deadline = time.time() + timeout
        result = b""
        with self.rx_cond:
            while True:
                got = False
                for entry in self.rx_ring:
                    idx, _ts, chunk = entry
                    if idx < cursor:
                        continue
                    if len(result) >= max_bytes:
                        break
                    result += chunk
                    cursor = idx + 1
                    got = True
                if got or len(result) >= max_bytes:
                    break
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self.rx_cond.wait(min(remaining, 0.2))
        return result, cursor

    def read_new(self, timeout=0.3, max_bytes=4096):
        """MCP 读取：消费 mcp_cursor 之后的新数据。"""
        data, cursor = self.read_from(self.mcp_cursor, timeout, max_bytes)
        self.mcp_cursor = cursor
        return data, cursor

    def read_line(self, cursor, timeout=1.0, delimiter=b"\n"):
        """从 cursor 起按分隔符读一行，返回 (line_bytes, new_cursor, found)。"""
        deadline = time.time() + timeout
        buf = b""
        while time.time() < deadline:
            chunk, cursor = self.read_from(cursor, 0.1, 8192)
            if chunk:
                buf += chunk
                pos = buf.find(delimiter)
                if pos >= 0:
                    line = buf[:pos]
                    rest_cursor = cursor - len(buf) + pos + len(delimiter)
                    if delimiter == b"\n" and line.endswith(b"\r"):
                        line = line[:-1]
                    return line, rest_cursor, True
        return buf, cursor, False

    # ---------- 记录落盘 ----------
    def start_record(self):
        with self.record_lock:
            if self.recording:
                return
            os.makedirs(self.logs_dir, exist_ok=True)
            name = "%s_%s.log" % (sanitize_name(self.cfg["port"]),
                                  time.strftime("%Y%m%d_%H%M%S"))
            self.record_fp = open(os.path.join(self.logs_dir, name),
                                  "a", encoding="utf-8")
            self.record_fp.write("# uart-proxy record %s %s\n"
                                 % (self.cfg["port"], time.strftime("%Y-%m-%d %H:%M:%S")))
            self.recording = True
            self.broadcast_status()

    def stop_record(self):
        with self.record_lock:
            if not self.recording:
                return
            self.recording = False
            try:
                self.record_fp.close()
            except Exception:
                pass
            self.record_fp = None
            self.broadcast_status()

    def _record_line(self, direction, payload, ts):
        try:
            ms = ts % 1000
            stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000.0))
            self.record_fp.write("[%s.%03d] %s %d %s\n"
                                 % (stamp, ms, direction, len(payload), payload.hex()))
            self.record_fp.flush()
        except Exception:
            pass

    # ---------- 状态广播 ----------
    def broadcast_status(self):
        self.manager.broadcast({
            "t": "status",
            "port_id": self.port_id,
            "state": self.state,
            "cfg": self.public_cfg(),
            "error": self.error,
            "rx_bytes": self.rx_bytes,
            "tx_bytes": self.tx_bytes,
            "connected_since": self.connected_since,
            "recording": self.recording,
        })

    def broadcast_stats(self):
        self.manager.broadcast({
            "t": "stats",
            "port_id": self.port_id,
            "rx_bytes": self.rx_bytes,
            "tx_bytes": self.tx_bytes,
            "connected_since": self.connected_since,
        })

    def public_cfg(self):
        keys = ("port", "baudrate", "bytesize", "parity", "stopbits",
                "flow", "encoding", "dtr", "rts", "auto_reconnect")
        return {k: self.cfg.get(k) for k in keys}

    def status_payload(self):
        return {
            "port_id": self.port_id,
            "state": self.state,
            "cfg": self.public_cfg(),
            "error": self.error,
            "rx_bytes": self.rx_bytes,
            "tx_bytes": self.tx_bytes,
            "connected_since": self.connected_since,
            "recording": self.recording,
        }
