#!/usr/bin/env python3
"""Tiny static server for the TVBox maintainer output dir (tvbox.json + report.txt).

Extras:
  GET /run  -> trigger a maintenance run in the background (one at a time);
               returns a small status page. Handy for manual runs from a
               phone/TV browser on the LAN.
"""
import functools
import http.server
import os
import socketserver
import subprocess
import sys
import threading

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "out")
PORT = 9977

_run_lock = threading.Lock()
_run_active = False


def _run_maintainer():
    global _run_active
    try:
        with open(os.path.join(BASE, "run.log"), "a", encoding="utf-8") as log:
            subprocess.run(
                [sys.executable, os.path.join(BASE, "tvbox_maintain.py")],
                stdout=log, stderr=subprocess.STDOUT, check=False,
            )
    finally:
        with _run_lock:
            _run_active = False


def _page(text):
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>TVBox 维护</title></head>"
            "<body style='font-family:sans-serif;font-size:18px;line-height:1.7;padding:24px'>"
            + text +
            "<p><a href='/report.txt'>查看最新报告</a> · <a href='/tvbox.json'>查看当前配置</a></p>"
            "</body></html>").encode("utf-8")


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, fmt, *args):  # keep the log quiet
        pass

    def do_GET(self):
        if self.path.split("?", 1)[0] == "/run":
            global _run_active
            with _run_lock:
                if _run_active:
                    body = _page("⏳ <b>正在运行中</b>，请稍等一两分钟再看报告。")
                else:
                    _run_active = True
                    threading.Thread(target=_run_maintainer, daemon=True).start()
                    body = _page("▶️ <b>已开始运行</b>：正在收集、验活、生成新配置，"
                                 "大约 1–3 分钟。完成后刷新报告即可看到结果。")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    handler = functools.partial(Handler, directory=OUT)
    with Server(("0.0.0.0", PORT), handler) as httpd:
        httpd.serve_forever()
