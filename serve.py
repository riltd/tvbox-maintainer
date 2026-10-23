#!/usr/bin/env python3
"""Tiny static server for the TVBox maintainer output dir (tvbox.json + report.txt)."""
import functools
import http.server
import os
import socketserver

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "out")
PORT = 9977


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, fmt, *args):  # keep the log quiet
        pass


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    handler = functools.partial(Handler, directory=OUT)
    with Server(("0.0.0.0", PORT), handler) as httpd:
        httpd.serve_forever()
