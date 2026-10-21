# -*- coding: utf-8 -*-
"""串口扫描。"""
import serial.tools.list_ports


def list_serial_ports():
    """返回 [{port, desc, hwid}]，跨平台（COMx / ttyUSBx / ttyACMx）。"""
    out = []
    for p in serial.tools.list_ports.comports():
        out.append({
            "port": p.device,
            "desc": p.description or "",
            "hwid": p.hwid or "",
        })
    return out
