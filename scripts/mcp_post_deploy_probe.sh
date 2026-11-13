#!/usr/bin/env bash
# Blocking post-deploy MCP probe (DVP-TSK-L92).
# Usage: mcp_post_deploy_probe.sh <connect_url> [expected_sign_in_origin]
#        mcp_post_deploy_probe.sh --self-test
# Retries every 30s for up to 3 minutes (no polling under 30s).
set -u



probe_once() {
  local url="$1" expect="${2:-}"
  python3 - "$url" "$expect" <<'PY'
import sys, json, urllib.request, urllib.error, urllib.parse
url, expect = sys.argv[1], sys.argv[2]
class NoRedir(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k): return None
op = urllib.request.build_opener(NoRedir)
def call(u, method="GET", data=None):
    req = urllib.request.Request(u, method=method, data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"})
    try:
        r = op.open(req, timeout=20)
    except urllib.error.HTTPError as e:
        r = e
    except Exception as e:
        return None, {}, str(e)
    return r.status if hasattr(r, "status") else r.code, {k.lower(): v for k, v in r.headers.items()}, r.read().decode("utf-8", "replace")
p = urllib.parse.urlparse(url)
origin = f"{p.scheme}://{p.netloc}"
meta = f"{origin}/.well-known/oauth-protected-resource{p.path}"
st, h, body = call(meta)
if st != 200: print(f"FAIL metadata: GET {meta} -> {st} {body[:120] if st is None else ''}"); sys.exit(1)
try: d = json.loads(body)
except Exception: print(f"FAIL metadata: not JSON at {meta}"); sys.exit(1)
if d.get("resource") != url: print(f"FAIL metadata: resource={d.get('resource')!r} != {url!r}"); sys.exit(1)
if expect:
    a = (d.get("authorization_servers") or [None])[0]
    if a != expect: print(f"FAIL metadata: authorization_servers[0]={a!r} != {expect!r}"); sys.exit(1)
st, h, body = call(url, "POST", b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}')
if st != 401: print(f"FAIL mcp: POST {url} without token -> {st} (want 401)"); sys.exit(1)
wa = h.get("www-authenticate")
src = "www-authenticate"
if wa is None:
    wa = h.get("x-amzn-remapped-www-authenticate")
    src = "x-amzn-remapped-www-authenticate"
    if wa is not None and any(k in h for k in ("x-amz-cf-id", "x-amz-cf-pop")):
        print("FAIL mcp: only x-amzn-remapped header present behind CloudFront"); sys.exit(1)
if not wa or "resource_metadata=" not in wa: print(f"FAIL mcp: www-authenticate missing resource_metadata= ({wa!r})"); sys.exit(1)
if "x-io-kit-mcp" not in h: print("FAIL mcp: response lacks x-io-kit-mcp header"); sys.exit(1)
print(f"OK metadata resource matches; 401 challenge via {src}; x-io-kit-mcp={h['x-io-kit-mcp']}")
PY
}

run() {
  local url="$1" expect="${2:-}" start now out rc INTERVAL="${PROBE_INTERVAL:-30}" DEADLINE="${PROBE_DEADLINE:-180}"
  start=$(date +%s)
  while :; do
    out=$(probe_once "$url" "$expect"); rc=$?
    echo "[mcp-probe] $out"
    [ $rc -eq 0 ] && return 0
    now=$(date +%s)
    if [ $((now - start + INTERVAL)) -gt "$DEADLINE" ]; then
      echo "[mcp-probe] giving up after $((now - start))s: $out" >&2
      return 1
    fi
    sleep "$INTERVAL"
  done
}

self_test() {
  local d port pid rc=0
  d=$(mktemp -d); export no_proxy=127.0.0.1 NO_PROXY=127.0.0.1
  cat > "$d/srv.py" <<'PY'
import http.server, json, sys
MODE = sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        base = f"http://127.0.0.1:{self.server.server_port}"
        if self.path == "/.well-known/oauth-protected-resource/mcp":
            b = json.dumps({"resource": base + "/mcp", "authorization_servers": [base]}).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
        else: self.send_response(404); self.end_headers()
    def do_POST(self):
        if MODE == "bad": self.send_response(500); self.end_headers(); return
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Bearer resource_metadata="x"')
        self.send_header("x-io-kit-mcp", "1"); self.end_headers()
s = http.server.HTTPServer(("127.0.0.1", 0), H)
open(sys.argv[1], "w").write(str(s.server_port))
s.serve_forever()
PY
  for mode in good bad; do
    python3 "$d/srv.py" "$d/port" $mode & pid=$!
    for _ in $(seq 50); do [ -s "$d/port" ] && break; sleep 0.1; done
    port=$(cat "$d/port"); rm -f "$d/port"
    PROBE_INTERVAL=1 PROBE_DEADLINE=2 run "http://127.0.0.1:$port/mcp" "http://127.0.0.1:$port" >/dev/null 2>&1
    r=$?
    kill $pid 2>/dev/null; wait $pid 2>/dev/null
    if [ "$mode" = good ] && [ $r -ne 0 ]; then echo "self-test FAIL: good fixture rejected"; rc=1; fi
    if [ "$mode" = bad ] && [ $r -eq 0 ]; then echo "self-test FAIL: bad fixture accepted"; rc=1; fi
  done
  rm -rf "$d"
  [ $rc -eq 0 ] && echo "self-test OK"
  return $rc
}

case "${1:-}" in
  --self-test) self_test; exit $? ;;
  "") echo "usage: $0 <connect_url> [expected_sign_in_origin] | --self-test" >&2; exit 2 ;;
  *) run "$1" "${2:-}"; exit $? ;;
esac
