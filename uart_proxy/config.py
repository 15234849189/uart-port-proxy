# -*- coding: utf-8 -*-
"""配置持久化：JSON 文件，线程安全，原子写入。"""
import json
import os
import threading
import uuid

# 记忆到设备名下的串口参数键
DEVICE_CFG_KEYS = ["baudrate", "bytesize", "parity", "stopbits", "flow",
                   "encoding", "dtr", "rts", "auto_reconnect"]


class Config(object):
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = {
            "device_cfgs": {},   # 设备名 -> 上次串口参数
            "commands": [],      # [{id,name,data,mode,newline}]
            "sequences": [],     # [{id,name,steps:[{data,mode,newline,delay_ms}],loops}]
            "prefs": {
                "display_mode": "text",
                "show_ts": True,
                "encoding": "utf-8",
                "auto_reconnect": True,
            },
        }
        self._load()

    # ---------- 文件 ----------
    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except (IOError, OSError, ValueError):
            pass
        self.data.setdefault("device_cfgs", {})
        self.data.setdefault("commands", [])
        self.data.setdefault("sequences", [])
        prefs = self.data.setdefault("prefs", {})
        prefs.setdefault("display_mode", "text")
        prefs.setdefault("show_ts", True)
        prefs.setdefault("encoding", "utf-8")
        prefs.setdefault("auto_reconnect", True)

    def save(self):
        with self.lock:
            d = os.path.dirname(self.path)
            if d and not os.path.isdir(d):
                try:
                    os.makedirs(d)
                except OSError:
                    pass
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            try:
                os.replace(tmp, self.path)
            except OSError:
                if os.path.exists(tmp):
                    os.remove(tmp)

    # ---------- 设备参数记忆 ----------
    def get_device_cfg(self, device):
        with self.lock:
            return dict(self.data["device_cfgs"].get(device, {}))

    def remember_device(self, device, cfg):
        with self.lock:
            saved = {}
            for k in DEVICE_CFG_KEYS:
                if k in cfg and cfg[k] is not None:
                    saved[k] = cfg[k]
            self.data["device_cfgs"][device] = saved
            self.save()

    # ---------- 指令库 ----------
    def get_commands(self):
        with self.lock:
            return json.loads(json.dumps(self.data["commands"]))

    def add_command(self, name, data, mode, newline):
        with self.lock:
            item = {"id": uuid.uuid4().hex, "name": name, "data": data,
                    "mode": mode, "newline": newline}
            self.data["commands"].append(item)
            self.save()
            return item

    def update_command(self, cmd_id, fields):
        with self.lock:
            for c in self.data["commands"]:
                if c["id"] == cmd_id:
                    for k in ("name", "data", "mode", "newline"):
                        if k in fields:
                            c[k] = fields[k]
                    self.save()
                    return dict(c)
        return None

    def delete_command(self, cmd_id):
        with self.lock:
            before = len(self.data["commands"])
            self.data["commands"] = [c for c in self.data["commands"] if c["id"] != cmd_id]
            changed = len(self.data["commands"]) != before
            if changed:
                self.save()
            return changed

    # ---------- 指令序列 ----------
    def get_sequences(self):
        with self.lock:
            return json.loads(json.dumps(self.data["sequences"]))

    def add_sequence(self, name, steps, loops):
        with self.lock:
            item = {"id": uuid.uuid4().hex, "name": name,
                    "steps": steps, "loops": int(loops)}
            self.data["sequences"].append(item)
            self.save()
            return item

    def update_sequence(self, seq_id, fields):
        with self.lock:
            for s in self.data["sequences"]:
                if s["id"] == seq_id:
                    if "name" in fields:
                        s["name"] = fields["name"]
                    if "steps" in fields:
                        s["steps"] = fields["steps"]
                    if "loops" in fields:
                        s["loops"] = int(fields["loops"])
                    self.save()
                    return json.loads(json.dumps(s))
        return None

    def delete_sequence(self, seq_id):
        with self.lock:
            before = len(self.data["sequences"])
            self.data["sequences"] = [s for s in self.data["sequences"] if s["id"] != seq_id]
            changed = len(self.data["sequences"]) != before
            if changed:
                self.save()
            return changed

    # ---------- 界面偏好 ----------
    def get_prefs(self):
        with self.lock:
            return dict(self.data["prefs"])

    def update_prefs(self, fields):
        with self.lock:
            for k, v in fields.items():
                if k in ("display_mode", "show_ts", "encoding", "auto_reconnect"):
                    self.data["prefs"][k] = v
            self.save()
            return dict(self.data["prefs"])
