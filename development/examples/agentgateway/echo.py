# Answers every POST with the request headers it received, values shortened,
# so run.sh can show what agentgateway forwards to a backend.
import http.server
import json


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        headers = {k.lower(): v if len(v) <= 24 else v[:24] + "..." for k, v in self.headers.items()}
        body = json.dumps(headers, sort_keys=True).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
