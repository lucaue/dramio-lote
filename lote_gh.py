#!/usr/bin/env python3
"""Versao GitHub do lote: baixa os dados do B2, processa SEU shard, sobe os videos.
Uso: python3 lote_gh.py <shard> <total>
"""
import base64, concurrent.futures as cf, hashlib, json, os, re, shutil, subprocess, sys, threading, time
import unicodedata, urllib.parse, urllib.request, urllib.error

BK = os.environ.get("B2_KEY_ID", "0058801f4ba47340000000002")
BA = os.environ.get("B2_APP_KEY", "K005Q21vHqghnK5o2PoY305sqVuB/S4")
BUCKET = "dramio-videos"
B2 = "https://api.backblazeb2.com/b2api/v2"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0"
W = "/tmp/lote"
NOME = {"pt": "Português", "es": "Español", "fr": "Français", "de": "Deutsch",
        "th": "ไทย", "id": "Bahasa Indonesia", "en": "English"}
basic = base64.b64encode(f"{BK}:{BA}".encode()).decode()


def b2(url, body=None, token=None, ba=False):
    h = {"Accept": "application/json"}
    h["Authorization"] = f"Basic {basic}" if ba else (token or "")
    d = json.dumps(body).encode() if body is not None else None
    if d:
        h["Content-Type"] = "application/json"
    with urllib.request.urlopen(urllib.request.Request(url, data=d, method="POST" if d else "GET", headers=h), timeout=180) as r:
        return json.loads(r.read().decode() or "{}")


a = b2(f"{B2}/b2_authorize_account", ba=True)
api, tok, acc, dlurl = a["apiUrl"], a["authorizationToken"], a["accountId"], a["downloadUrl"]
# A CHAVE RESTRITA NAO TEM listBuckets — uso o id do bucket direto (e o proprio auth informa)
bid = (a.get("allowed") or {}).get("bucketId") or os.environ.get("B2_BUCKET_ID") or "28d8f0116fc43b2aa4170314"
print(f"bucket id: {bid} (sem listBuckets ✓)", flush=True)
_pool, _lk = [], threading.Lock()
_MODELO = None


def nova():
    u = b2(f"{api}/b2api/v2/b2_get_upload_url", {"bucketId": bid}, tok)
    return u["uploadUrl"], u["authorizationToken"]


def sobe(nome, dados, ctype="application/octet-stream"):
    with _lk:
        url, tk = _pool.pop() if _pool else nova()
    try:
        r = urllib.request.Request(url, data=dados, method="POST", headers={
            "Authorization": tk, "X-Bz-File-Name": nome, "Content-Type": ctype,
            "X-Bz-Content-Sha1": hashlib.sha1(dados).hexdigest(), "Content-Length": str(len(dados))})
        with urllib.request.urlopen(r, timeout=600) as resp:
            json.loads(resp.read().decode())
        with _lk:
            _pool.append((url, tk))
        return True
    except Exception:
        return False


def baixa(u, timeout=90, ref="https://www.reelshort.com/"):
    for i in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA, "Referer": ref}), timeout=timeout) as r:
                return r.read()
        except Exception:
            if i == 2:
                return None
            time.sleep(1 + i)
    return None


def baixa_auth(u, timeout=600):
    """baixa do B2 com o token do authorize (serve para leitura ✓)."""
    for i in range(3):
        try:
            r = urllib.request.Request(u, headers={"Authorization": tok, "User-Agent": UA})
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                return resp.read()
        except Exception as e:
            if i == 2:
                print(f"      baixa_auth falhou: {str(e)[:80]}", flush=True)
                return None
            time.sleep(2 + i)
    return None


def baixa_hls(m3u8):
    man = baixa(m3u8)
    if not man:
        return None, {}
    base = m3u8.rsplit("/", 1)[0]
    segs = [l.strip() for l in man.decode("utf-8", "replace").splitlines() if l.strip() and not l.startswith("#")]
    out = {}
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        def um(s):
            return (s.split("?")[0].rsplit("/", 1)[-1], baixa(s if s.startswith("http") else f"{base}/{s}", 60))
        for n, d in ex.map(um, segs):
            if d:
                out[n] = d
    return man, out


def slug_de(t):
    s = unicodedata.normalize("NFKD", str(t or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:70]


DADOS = {}


def pega_dados():
    """Le os dados do PROPRIO REPOSITORIO (o runner ja tem o checkout).

    ⚠️ NAO baixar do B2. Com 100 shards, 100 x ~70 MB = ~7 GB num dia e estoura a
    cota diaria de download do Backblaze — foi o que aconteceu em 13/10: TODA
    leitura de arquivo passou a devolver 403 e o acervo ficou ilegivel ate renovar.
    O repositorio e publico, entao o download no runner e gratis.

    `links_unificado_num.json` e o merge das 7 fontes (3.640 books · 237.515 links)
    e ja traz TODAS as dublagens — os arquivos antigos nao tinham."""
    if DADOS:
        return DADOS
    ORDEM = ("links_unificado_num.json", "links_unificado.json",
             "dramio_preencher.json", "matriz_rapida.json",
             "reelshort_catalog_pt_full.json", "links_cf.json", "links_pt.json",
             "links_slow.json", "links_derivados.json", "links_dub.json")
    raizes = [W, os.path.dirname(os.path.abspath(__file__)), os.getcwd()]
    for arq in ORDEM:
        for base in raizes:
            local = os.path.join(base, arq)
            if os.path.exists(local):
                try:
                    DADOS[arq] = json.load(open(local, encoding="utf-8"))
                    print(f"   {arq}: {os.path.getsize(local)/1024/1024:.1f} MB (do repo ✓)", flush=True)
                    break
                except Exception as e:
                    print(f"   {arq}: existe mas nao parseou ({str(e)[:60]})", flush=True)
    essenciais = [a for a in ("dramio_preencher.json", "matriz_rapida.json") if a not in DADOS]
    if essenciais:
        print(f"   ⚠️ FALTANDO no repo: {essenciais} — commite os JSON antes de disparar", flush=True)
    if "links_unificado_num.json" not in DADOS:
        print("   ⚠️ sem o links_unificado_num.json: as dublagens NAO entram (so o PT)", flush=True)
    print(f"   {len(DADOS)} arquivos lidos do repo · 0 bytes baixados do B2 ✓", flush=True)
    return DADOS


def mapa_links(D):
    # ⭐ links_unificado_num.json ja e o merge das 7 fontes, COM as dublagens.
    # Os arquivos antigos (cf/pt/slow/derivados/dub) NAO tinham as dublagens novas:
    # usar so eles gerava episodio com 1 audio (so PT), silenciosamente.
    uni = D.get("links_unificado_num.json") or D.get("links_unificado.json")
    if uni:
        amostra = next(iter(uni.values()), None)
        if isinstance(amostra, dict):
            return {b: {str(k): u for k, u in (v or {}).items()} for b, v in uni.items()}
        # veio em lista -> numera pela posicao (menos preciso, mas nao quebra)
        return {b: {str(i + 1): u for i, u in enumerate(v or [])} for b, v in uni.items()}
    por = {}
    for b, lst in (D.get("links_cf.json") or {}).items():
        for e in (lst or []):
            if e.get("m3u8"):
                por.setdefault(b, {}).setdefault(e.get("ep"), e["m3u8"])
    for k in ("links_pt.json", "links_slow.json"):
        for b, v in (D.get(k) or {}).items():
            for e in ((v or {}).get("episodes") or []):
                if e.get("m3u8"):
                    por.setdefault(b, {}).setdefault(e.get("episode_number"), e["m3u8"])
    for k in ("links_derivados.json", "links_dub.json"):
        for b, v in (D.get(k) or {}).items():
            for e in ((v or {}).get("links") or []):
                if e.get("m3u8"):
                    por.setdefault(b, {}).setdefault(e.get("ep"), e["m3u8"])
    # NUMERADO: {book: {"1": url, "2": url, ...}}. O lote casa por NUMERO, nao por
    # posicao: 1.150 dos 3.640 books tem buraco na numeracao e por posicao o audio de
    # um idioma cairia no episodio ERRADO, silenciosamente.
    return {b: {str(ep): u for ep, u in d.items() if ep is not None} for b, d in por.items()}


def traduz(txt, lg):
    if not txt.strip():
        return ""
    q = urllib.parse.quote(txt[:4500])
    d = baixa(f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl={lg}&dt=t&q={q}",
              30, "https://translate.google.com/")
    if not d:
        return ""
    try:
        return "".join(x[0] for x in json.loads(d.decode())[0] if x and x[0])
    except Exception:
        return ""


def vtt(cs):
    L = ["WEBVTT", ""]
    for i, f, t in cs:
        L += [f"{int(i//3600):02d}:{int((i%3600)//60):02d}:{i%60:06.3f} --> "
              f"{int(f//3600):02d}:{int((f%3600)//60):02d}:{f%60:06.3f}", t or "...", ""]
    return ("\n".join(L)).encode()


def processa(book_pt, pos, slug, idmap, mapa, cat, titulo, eps_total):
    global _MODELO
    w = f"{W}/{slug}_{pos}"
    try:
        if os.path.exists(w):
            shutil.rmtree(w)
        os.makedirs(f"{w}/in", exist_ok=True)
        orig = (mapa.get(book_pt) or {}).get(str(pos + 1))
        if not orig:
            return False, "sem episodio"
        man, segs = baixa_hls(orig)
        if not man:
            return False, "video falhou"
        for n, d in segs.items():
            open(f"{w}/in/{n}", "wb").write(d)
        open(f"{w}/in/index.m3u8", "wb").write(man)

        # 3 qualidades
        for pasta, vf, vbr in (("720", None, None), ("480", "scale=-2:480", "900k"), ("360", "scale=-2:360", "600k")):
            out = f"{w}/v{pasta}"
            os.makedirs(out, exist_ok=True)
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", f"{w}/in/index.m3u8"]
            cmd += ["-an", "-c:v", "copy"] if vf is None else \
                   ["-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-b:v", vbr, "-maxrate", vbr,
                    "-bufsize", "1800k", "-an"]
            cmd += ["-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
                    "-hls_segment_filename", f"{out}/seg%03d.ts", f"{out}/index.m3u8"]
            if subprocess.run(cmd, capture_output=True, timeout=3600).returncode != 0:
                return False, f"ffmpeg {pasta}"

        # audios
        for lg in ["pt"] + [l for l in idmap if l != "pt"]:
            out = f"{w}/a_{lg}"
            os.makedirs(out, exist_ok=True)
            if lg == "pt":
                fonte = f"{w}/in/index.m3u8"
            else:
                u = (mapa.get(idmap.get(lg)) or {}).get(str(pos + 1))
                if not u:
                    continue
                mm, ss = baixa_hls(u)
                if not ss:
                    continue
                d = f"{w}/din_{lg}"
                os.makedirs(d, exist_ok=True)
                for n, c in ss.items():
                    open(f"{d}/{n}", "wb").write(c)
                open(f"{d}/index.m3u8", "wb").write(mm)
                fonte = f"{d}/index.m3u8"
            if subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", fonte, "-vn", "-c:a", "aac",
                               "-b:a", "96k", "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
                               "-hls_segment_filename", f"{out}/seg%03d.ts", f"{out}/index.m3u8"],
                              capture_output=True, timeout=3600).returncode != 0:
                shutil.rmtree(out, ignore_errors=True)

        # legendas
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", f"{w}/in/index.m3u8", "-vn",
                        "-c:a", "aac", "-b:a", "96k", f"{w}/tr.m4a"], capture_output=True, timeout=1200)
        from faster_whisper import WhisperModel
        with _lk:
            if _MODELO is None:
                _MODELO = WhisperModel("base", device="cpu", compute_type="int8")
            m = _MODELO
        st, _ = m.transcribe(f"{w}/tr.m4a", vad_filter=True, beam_size=1)
        cues = [(x.start, x.end, x.text.strip()) for x in st if x.text and x.text.strip()]
        legendas = {}
        if cues:
            legendas["pt"] = vtt(cues)
            full = " ".join(c[2] for c in cues)
            for lg in ("es", "fr", "de", "th", "id", "en"):
                tr = traduz(full, lg)
                if not tr:
                    continue
                partes = tr.split(". ")
                n = len(cues)
                if len(partes) >= n:
                    t = max(1, len(partes) // n)
                    bl = [" ".join(partes[i*t:(i+1)*t]) for i in range(n)]
                else:
                    bl = [partes[i] if i < len(partes) else "" for i in range(n)]
                legendas[lg] = vtt([(c[0], c[1], bl[i]) for i, c in enumerate(cues)])
        dur = max([c[1] for c in cues], default=0) + 1

        # master
        tem_audio = [lg for lg in ["pt", "de", "es", "fr", "id", "th"] if os.path.isdir(f"{w}/a_{lg}")]
        L = ["#EXTM3U", "#EXT-X-VERSION:6", "#EXT-X-INDEPENDENT-SEGMENTS"]
        for i, lg in enumerate(tem_audio):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="{NOME[lg]}",LANGUAGE="{lg}"{pad},'
                     f'URI="audio/{lg}/index.m3u8"')
        for i, lg in enumerate(sorted(legendas)):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="sub",NAME="{NOME[lg]}",LANGUAGE="{lg}"{pad},'
                     f'URI="subs/{lg}.m3u8"')
        for alt, larg, bw in ((360, 640, 700000), (480, 854, 1100000), (720, 1280, 2000000)):
            L.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},AVERAGE-BANDWIDTH={int(bw*0.9)},'
                     f'RESOLUTION={larg}x{alt},AUDIO="aud",SUBTITLES="sub"')
            L.append(f"video/{alt}/index.m3u8")
        master = ("\n".join(L) + "\n").encode()

        P = f"dramio/{slug}/ep{pos+1:03d}"
        T = []
        for alt in (360, 480, 720):
            d = f"{w}/v{alt}"
            if not os.path.isdir(d):
                continue
            for arq in sorted(os.listdir(d)):
                T.append((f"{P}/video/{alt}/{arq}", open(f"{d}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg in tem_audio:
            d = f"{w}/a_{lg}"
            for arq in sorted(os.listdir(d)):
                T.append((f"{P}/audio/{lg}/{arq}", open(f"{d}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg, dados in legendas.items():
            T.append((f"{P}/subs/{lg}.vtt", dados, "text/vtt"))
            pl = (f"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{int(dur)+1}\n#EXT-X-MEDIA-SEQUENCE:0\n"
                  f"#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:{dur:.3f},\n{lg}.vtt\n#EXT-X-ENDLIST\n")
            T.append((f"{P}/subs/{lg}.m3u8", pl.encode(), "application/vnd.apple.mpegurl"))
        T.append((f"{P}/master.m3u8", master, "application/vnd.apple.mpegurl"))
        if pos == 0:
            for lg, vb in idmap.items():
                cov = next((x.get("book_pic") for x in cat if x["book_id"] == vb), None)
                if cov:
                    dd = baixa(cov, 45)
                    if dd:
                        T.append((f"dramio/{slug}/capa/{lg}.jpg", dd, "image/jpeg"))
            idx = {"slug": slug, "titulo": titulo, "book_id_reelshort": book_pt, "episodios": eps_total,
                   "idiomas_audio": sorted(tem_audio), "idiomas_legenda": sorted(legendas),
                   "qualidades": [360, 480, 720],
                   "estrutura": {"master": "epNNN/master.m3u8", "video": "epNNN/video/<altura>/index.m3u8",
                                 "audio": "epNNN/audio/<idioma>/index.m3u8", "legenda": "epNNN/subs/<idioma>.vtt",
                                 "capa": "capa/<idioma>.jpg"}}
            T.append((f"dramio/{slug}/index.json", json.dumps(idx, ensure_ascii=False, indent=1).encode(),
                      "application/json"))
        okn = 0
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            for r in ex.map(lambda x: sobe(*x), T):
                okn += 1 if r else 0
        shutil.rmtree(w, ignore_errors=True)
        return okn > len(T) * 0.7, f"{okn}/{len(T)} · {len(tem_audio)}aud · {len(legendas)}leg"
    except Exception as e:
        shutil.rmtree(w, ignore_errors=True)
        return False, str(e)[:110]


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    total = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    os.makedirs(W, exist_ok=True)
    print(f"=== shard {shard}/{total} ===", flush=True)
    D = pega_dados()
    mapa = mapa_links(D)
    mat = D.get("matriz_rapida.json") or {}
    cat = D.get("reelshort_catalog_pt_full.json") or []
    pre = D.get("dramio_preencher.json") or []

    alvos = {}
    for r in pre:
        b = r["book_id"]
        if b in alvos:
            continue
        n = len(mapa.get(b) or [])
        if n == 0:
            continue
        alvos[b] = {"titulo": r["title_dramio"], "slug": slug_de(r["title_dramio"]), "eps": n,
                    "idmap": {lg: (x or {}).get("book") for lg, x in ((mat.get(b) or {}).get("idiomas") or {}).items()}}
    print(f"dramas: {len(alvos)}", flush=True)

    tarefas = []
    for b, i in alvos.items():
        for pos in range(i["eps"]):
            tarefas.append((b, pos, i))
    minha = [t for n, t in enumerate(tarefas) if n % total == shard]
    print(f"meu shard: {len(minha)} de {len(tarefas)} episodios", flush=True)

    os.makedirs("out", exist_ok=True)
    okc = errc = 0
    t0 = time.time()
    feitos = []
    for n, (b, pos, i) in enumerate(minha, 1):
        ok, msg = processa(b, pos, i["slug"], i["idmap"], mapa, cat, i["titulo"], i["eps"])
        okc += 1 if ok else 0
        errc += 1 if not ok else 0
        if ok:
            feitos.append(f"{b}:{pos}")
        if n % 5 == 0 or n == len(minha):
            print(f"  {n}/{len(minha)} | ok={okc} err={errc} | {(time.time()-t0)/60:.0f}min | "
                  f"{i['titulo'][:26]} · {msg[:50]}", flush=True)
            json.dump({"feitos": feitos}, open(f"out/estado_{shard}.json", "w", encoding="utf-8"))
    json.dump({"feitos": feitos}, open(f"out/estado_{shard}.json", "w", encoding="utf-8"))
    print(f"\n=== FIM shard {shard}: {okc} ok · {errc} erros · {(time.time()-t0)/60:.0f}min ===", flush=True)


main()
