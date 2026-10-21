# -*- coding: utf-8 -*-
"""命令行入口：解析参数、组装各模块并启动 Web 服务。"""
import argparse
import os
import sys

from . import __version__


def _default_data_dirs():
    """数据目录约定：项目内 ./scripts ./logs 存在则优先，否则用 ~/.uart_proxy。"""
    home_dir = os.path.join(os.path.expanduser("~"), ".uart_proxy")
    cwd = os.getcwd()
    scripts = os.path.join(cwd, "scripts")
    logs = os.path.join(cwd, "logs")
    if not os.path.isdir(scripts):
        scripts = os.path.join(home_dir, "scripts")
    if not os.path.isdir(logs):
        logs = os.path.join(home_dir, "logs")
    return scripts, logs


def build_parser():
    p = argparse.ArgumentParser(
        prog="uart-proxy",
        description="串口代理工具：将本机串口代理为 Web 终端 + MCP 服务（Win/Linux）",
    )
    p.add_argument("--host", default="127.0.0.1", help="监听地址（默认 127.0.0.1，开放局域网用 0.0.0.0）")
    p.add_argument("--port", type=int, default=8321, help="HTTP 端口（默认 8321）")
    p.add_argument("--token", default="", help="访问令牌；设置后 Web/MCP 均需携带")
    p.add_argument("--config", default=None, help="配置文件路径（默认 ~/.uart_proxy/config.json）")
    p.add_argument("--scripts-dir", default=None, help="Python 脚本目录（默认 ./scripts 或 ~/.uart_proxy/scripts）")
    p.add_argument("--logs-dir", default=None, help="记录文件目录（默认 ./logs 或 ~/.uart_proxy/logs）")
    p.add_argument("--version", action="version", version="uart-proxy " + __version__)
    return p


def _seed_example_script(scripts_dir):
    """脚本目录为空时，释放内置示例脚本（wheel 安装后开箱即用）。"""
    try:
        if os.listdir(scripts_dir):
            return
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "examples", "example_at.py")
        if os.path.isfile(src):
            import shutil
            shutil.copy(src, os.path.join(scripts_dir, "example_at.py"))
    except OSError:
        pass


def main(argv=None):
    args = build_parser().parse_args(argv)

    home_dir = os.path.join(os.path.expanduser("~"), ".uart_proxy")
    config_path = args.config or os.path.join(home_dir, "config.json")
    scripts_dir, logs_dir = _default_data_dirs()
    if args.scripts_dir:
        scripts_dir = args.scripts_dir
    if args.logs_dir:
        logs_dir = args.logs_dir
    os.makedirs(scripts_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    _seed_example_script(scripts_dir)

    # 延迟导入，保证 --version 等参数快速响应
    from .config import Config
    from .manager import PortManager
    from .script_runner import AutomationEngine
    from .webapp import create_app

    config = Config(config_path)
    manager = PortManager(config, logs_dir)
    engine = AutomationEngine(manager, scripts_dir)
    app, sock = create_app(manager, config, engine, scripts_dir, logs_dir, args.token)

    banner = [
        "",
        "==============================================",
        "  uart-proxy v%s" % __version__,
        "==============================================",
        "  Web 终端 : http://%s:%d/" % (args.host, args.port),
        "  MCP 端点 : http://%s:%d/mcp" % (args.host, args.port),
        "  配置文件 : %s" % config_path,
        "  脚本目录 : %s" % scripts_dir,
        "  记录目录 : %s" % logs_dir,
    ]
    if args.token:
        banner.append("  访问令牌 : 已启用（请求需携带 token）")
    if args.host == "0.0.0.0":
        banner.append("  警告     : 服务已对局域网开放，建议配合 --token 使用")
    banner.append("  按 Ctrl+C 退出")
    banner.append("")
    for line in banner:
        try:
            print(line)
        except Exception:
            # Windows 控制台编码异常时降级输出
            print(line.encode("ascii", "replace").decode("ascii"))
    sys.stdout.flush()

    try:
        # threaded=True：WS 长连接与 REST 并发
        app.run(host=args.host, port=args.port, threaded=True, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()


if __name__ == "__main__":
    main()
