# -*- coding: utf-8 -*-
"""多串口管理器：会话表、客户端广播、发送统一入口。"""
import base64
import threading
import uuid

from .session import PortSession, build_payload, now_ms


class PortManager(object):
    def __init__(self, config, logs_dir):
        self.config = config
        self.logs_dir = logs_dir
        self.lock = threading.RLock()
        self.sessions = {}      # port_id -> PortSession
        self.by_device = {}     # 设备名 -> PortSession
        self.clients = []       # [WsClient]

    # ---------- 扫描 ----------
    def scan(self):
        from .ports import list_serial_ports
        return list_serial_ports()

    # ---------- 会话 ----------
    def open_port(self, cfg):
        """打开串口，返回 (session, existed)。同一设备重复打开时返回既有会话。"""
        with self.lock:
            device = cfg.get("port")
            if not device:
                raise ValueError("缺少串口名")
            existing = self.by_device.get(device)
            if existing is not None and existing.state not in ("closed", "error"):
                return existing, True

            merged = dict(self.config.get_device_cfg(device))
            for k, v in cfg.items():
                if v is not None:
                    merged[k] = v
            merged["port"] = device
            merged.setdefault("baudrate", 115200)
            merged.setdefault("bytesize", 8)
            merged.setdefault("parity", "N")
            merged.setdefault("stopbits", 1)
            merged.setdefault("flow", "none")
            merged.setdefault("encoding", "utf-8")
            merged.setdefault("auto_reconnect", True)

            port_id = uuid.uuid4().hex
            session = PortSession(port_id, merged, self, self.logs_dir)
            self.sessions[port_id] = session
            self.by_device[device] = session
            self.config.remember_device(device, merged)
            session.start()
            return session, False

    def close_port(self, port_id):
        with self.lock:
            session = self.sessions.pop(port_id, None)
            if session is None:
                return False
            for dev, s in list(self.by_device.items()):
                if s.port_id == port_id:
                    del self.by_device[dev]
        session.close()
        self.broadcast({"t": "status", "port_id": port_id, "state": "closed"})
        return True

    def get(self, port_id):
        with self.lock:
            return self.sessions.get(port_id)

    def status_all(self):
        with self.lock:
            return [s.status_payload() for s in self.sessions.values()]

    def shutdown(self):
        with self.lock:
            sessions = list(self.sessions.values())
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass

    # ---------- 客户端 ----------
    def add_client(self, client):
        with self.lock:
            self.clients.append(client)

    def remove_client(self, client):
        with self.lock:
            if client in self.clients:
                self.clients.remove(client)

    def broadcast(self, msg):
        with self.lock:
            clients = list(self.clients)
        for c in clients:
            c.enqueue(msg)

    def broadcast_data(self, session, direction, payload, ts):
        self.broadcast({
            "t": "data",
            "port_id": session.port_id,
            "dir": direction,
            "b64": base64.b64encode(payload).decode("ascii"),
            "ts": ts if ts is not None else now_ms(),
        })

    # ---------- 发送统一入口（WS / MCP / 脚本共用） ----------
    def send_data(self, session, data, mode="text", newline="none", encoding=None):
        enc = encoding or session.cfg.get("encoding", "utf-8")
        payload = build_payload(data, mode, newline, enc)
        session.write(payload)
        return payload
