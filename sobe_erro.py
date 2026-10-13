# sobe_erro.py <arquivo_local> <nome_no_b2>
# Manda um txt pro B2 em _dados/_erros/ — assim o agente le o erro sem precisar
# da permissao Actions:read (que o token fine-grained nao tem ✗).
import base64, hashlib, json, os, sys, urllib.error, urllib.request

KEY_ID = os.environ.get("B2_KEY_ID")
KEY = os.environ.get("B2_APP_KEY")
API = "https://api005.backblazeb2.com"
BUCKET = os.environ.get("B2_BUCKET_ID") or "28d8f0116fc43b2aa4170314"

if len(sys.argv) < 3:
    print("uso: sobe_erro.py <arquivo> <nome_no_b2>")
    sys.exit(2)
local, nome = sys.argv[1], sys.argv[2]
if not (KEY_ID and KEY):
    print("sem B2_KEY_ID/B2_APP_KEY no ambiente")
    sys.exit(3)
if not os.path.exists(local):
    print(f"nao achei {local}")
    sys.exit(4)

cred = base64.b64encode(f"{KEY_ID}:{KEY}".encode()).decode()
try:
    with urllib.request.urlopen(urllib.request.Request(
            API + "/b2api/v2/b2_authorize_account",
            headers={"Authorization": "Basic " + cred}), timeout=60) as r:
        acc = json.loads(r.read().decode())
    tok = acc["authorizationToken"]
    with urllib.request.urlopen(urllib.request.Request(
            acc["apiUrl"] + f"/b2api/v2/b2_get_upload_url?bucketId={BUCKET}",
            headers={"Authorization": tok}), timeout=60) as r:
        up = json.loads(r.read().decode())
except urllib.error.HTTPError as e:
    print(f"B2 recusou: HTTP {e.code} {e.read().decode()[:180]}")
    sys.exit(5)

dados = open(local, "rb").read()[:400000]      # corta em 400 KB
destino = f"_dados/_erros/{nome}"
req = urllib.request.Request(up["uploadUrl"], data=dados, method="POST", headers={
    "Authorization": up["authorizationToken"],
    "X-Bz-File-Name": destino,
    "Content-Type": "text/plain; charset=utf-8",
    "Content-Length": str(len(dados)),
    "X-Bz-Content-Sha1": hashlib.sha1(dados).hexdigest()})
try:
    with urllib.request.urlopen(req, timeout=300) as r:
        j = json.loads(r.read().decode())
    print(f"subiu {destino} · {j.get('contentLength')} bytes")
except urllib.error.HTTPError as e:
    print(f"upload falhou: HTTP {e.code} {e.read().decode()[:180]}")
    sys.exit(6)
