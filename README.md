# uart-proxy

跨平台串口代理工具：将本机串口代理为 **Web 终端 + MCP 服务**，兼容 Python 3.7+（Windows / Linux）。

- **Web 终端**：浏览器中打开/关闭串口、收发数据，支持十六进制模式与实时日志
- **MCP 服务**：内置 MCP 端点（`/mcp`），可供 AI 客户端直接操作串口
- **自动化脚本**：在 `scripts/` 目录放置 Python 脚本，实现 AT 握手等自动收发序列
- **会话记录**：所有串口数据自动记录到 `logs/` 目录，便于回溯分析

## 安装

```bash
pip install .
# 或开发模式
pip install -e .
```

依赖：flask、flask-sock、simple-websocket、pyserial（见 `requirements.txt`）。

## 使用

```bash
# 默认监听 127.0.0.1:8321
uart-proxy

# 开放局域网访问并启用令牌
uart-proxy --host 0.0.0.0 --token your-secret

# 自定义端口与目录
uart-proxy --port 9000 --scripts-dir ./scripts --logs-dir ./logs
```

启动后：

| 入口 | 地址 |
| --- | --- |
| Web 终端 | `http://127.0.0.1:8321/` |
| MCP 端点 | `http://127.0.0.1:8321/mcp` |

- **配置文件**：默认 `~/.uart_proxy/config.json`（可用 `--config` 指定）
- **脚本目录**：默认 `./scripts`（不存在则回退 `~/.uart_proxy/scripts`）
- **记录目录**：默认 `./logs`（不存在则回退 `~/.uart_proxy/logs`）
- 首次启动时若脚本目录为空，会自动释放内置示例脚本 `example_at.py`

## MCP 工具

MCP 客户端连接 `/mcp` 端点后可使用以下工具：

| 工具 | 说明 |
| --- | --- |
| `uart_list_ports` | 列出本机可用串口 |
| `uart_open` / `uart_close` | 打开 / 关闭串口 |
| `uart_list_active` | 列出已打开的串口会话 |
| `uart_status` | 查询会话状态 |
| `uart_send` | 发送文本或十六进制数据 |
| `uart_read` / `uart_readline` | 读取数据 / 按行读取 |
| `uart_run_sequence` | 运行收发序列 |
| `uart_run_script` | 运行 `scripts/` 下的自动化脚本 |
| `uart_script_log` | 获取脚本执行日志 |
| `uart_stop` | 停止正在运行的脚本 |

## 自动化脚本示例

脚本可用的 API：`send` / `send_hex` / `recv` / `recv_line` / `sleep` / `log` / `print` / `status`。

```python
# AT 握手示例（scripts/example_at.py）
send("AT", newline="crlf")
r = recv(timeout=2)
if r is None:
    log("超时未收到数据")
elif "OK" in r["text"]:
    send("AT+GMR", newline="crlf")
    while True:
        line = recv_line(timeout=1)
        if line is None:
            break
        log(line["text"])
```

## 项目结构

```
uart_proxy/
├── cli.py           # 命令行入口
├── config.py        # 配置管理
├── ports.py         # 串口扫描
├── manager.py       # 端口会话管理
├── session.py       # 串口会话
├── script_runner.py # 自动化脚本引擎
├── webapp.py        # Flask Web 应用
├── mcp_server.py    # MCP 服务端
└── web/             # Web 前端（HTML/JS/CSS）
```

## License

MIT
