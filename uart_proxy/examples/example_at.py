# -*- coding: utf-8 -*-
# uart-proxy 脚本示例：AT 握手
# 可用 API：send/send_hex/recv/recv_line/sleep/log/print/status

send("AT", newline="crlf")
r = recv(timeout=2)
if r is None:
    log("超时未收到数据")
else:
    log("收到: %r" % r["text"])
    if "OK" in r["text"]:
        send("AT+GMR", newline="crlf")
        while True:
            line = recv_line(timeout=1)
            if line is None:
                break
            log(line["text"])
    else:
        log("未收到 OK，请检查设备")
