#!/usr/bin/env python3
"""LOTE: processa os dramas da Dramio em paralelo, com retomada.
   Por episodio: 720p + 480p + 360p + 6 audios + 7 legendas + master -> B2.
Uso: python3 lote.py <quantos_dramas> <paralelismo>
"""
import base64, concurrent.futures as cf, hashlib, json, os, re, shutil, subprocess, sys, threading, time
import unicodedata, urllib.request, urllib.error

KID, AK = "8801f4ba4734", "005ce2e11689a8ae8f4e06e19c044ad1163393435d"
BUCKET = "dramio-videos"
B2 = "https://api.backblazeb2.com/b2api/v2"
BASE_W = "https://dramio-video.cinemioplay.workers.dev"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0"
ESTADO = os.environ.get("LOTE_ESTADO", "/root/rs/lote_estado.json")
NOME = {"pt": "Português", "es": "Español", "fr": "Français", "de": "Deutsch",
        "th": "ไทย", "id": "Bahasa Indonesia", "en": "English"}
basic = base64.b64encode(f"{KID}:{AK}".encode()).decode()


def b2(url, body=None, token=None, ba=False):
    h = {"Accept": "application/json"}
    h["Authorization"] = f"Basic {basic}" if ba else (token or "")
    d = json.dumps(body).encode() if body is not None else None
    if d:
        h["Content-Type"] = "application/json"
    with urllib.request.urlopen(urllib.request.Request(url, data=d, method="POST" if d else "GET", headers=h), timeout=120) as r:
        return json.loads(r.read().decode() or "{}")


a = b2(f"{B2}/b2_authorize_account", ba=True)
api, tok, acc = a["apiUrl"], a["authorizationToken"], a["accountId"]
bid = os.environ.get("B2_BUCKET_ID") or "28d8f0116fc43b2aa4170314"
# ⚠️ NAO chamar b2_list_buckets: a chave RESTRITA (listFiles+readFiles+writeFiles, que e a
# que deve rodar nos runners) NAO tem listBuckets e devolve 401/403 -> 100 shards falhando
# em 'processar meu shard' (aconteceu em 13/10). O id do bucket vai direto ✓
_pool, _lk = [], threading.Lock()
POOL_ALVO = 64          # tokens de upload mantidos aquecidos


def nova():
    u = b2(f"{api}/b2api/v2/b2_get_upload_url", {"bucketId": bid}, tok)
    return u["uploadUrl"], u["authorizationToken"]


def aquece(n=POOL_ALVO):
    """enche o pool ANTES de comecar, fora de qualquer lock.
    Sem isso, cada vez que o pool esvaziava o nova() rodava DENTRO do _lk e
    serializava TODOS os uploads (~110 arquivos/episodio x 8 threads) -> 4x mais lento."""
    novos = []
    for _ in range(n):
        try:
            novos.append(nova())
        except Exception:
            pass
    with _lk:
        _pool.extend(novos)
    return len(novos)


def sobe(nome, dados, ctype="application/octet-stream"):
    with _lk:
        par = _pool.pop() if _pool else None
    if par is None:
        try:
            par = nova()        # FORA do lock: nao trava os outros uploads
        except Exception:
            return False
    url, tk = par
    try:
        with _sem_upload:
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


def baixa(u, timeout=90):
    for i in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA,
                                                                          "Referer": "https://www.reelshort.com/"}), timeout=timeout) as r:
                return r.read()
        except Exception:
            if i == 2:
                return None
            time.sleep(1 + i)
    return None


def slug_de(t):
    s = unicodedata.normalize("NFKD", str(t or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:70]


# POOL COMPARTILHADO para baixar segmento de HLS.
# Antes o baixa_hls criava um ThreadPoolExecutor(16) POR CHAMADA. Com PAR=8 e ate
# 7 downloads por episodio (video + 6 dublagens) isso virava ate 896 threads ✗✗ —
# explosao de threads, context-switch e o lote caindo para ~1,7 episodio/min com a
# maquina OCIOSA (load 6,6 de 12). Um pool unico e limitado resolve.
_POOL_HLS = cf.ThreadPoolExecutor(max_workers=120)
# Teto de uploads simultaneos ao B2. Com PAR alto (20), 20 episodios x 8 threads de
# upload = 160 conexoes ao mesmo tempo -> o B2 comeca a recusar. 48 passa folgado.
_sem_upload = threading.Semaphore(48)


def baixa_hls(m3u8):
    man = baixa(m3u8)
    if not man:
        return None, {}
    base = m3u8.rsplit("/", 1)[0]
    segs = [l.strip() for l in man.decode("utf-8", "replace").splitlines() if l.strip() and not l.startswith("#")]
    out = {}

    def um(s):
        return (s.split("?")[0].rsplit("/", 1)[-1], baixa(s if s.startswith("http") else f"{base}/{s}", 60))

    for n, d in _POOL_HLS.map(um, segs):
        if d:
            out[n] = d
    return man, out


def roda_episodio_OLD(book_pt, pos, slug, idiomas_map, dub, cat, idx, titulo_drama="", eps_total=0):
    """processa UM episodio completo. devolve (ok, info)"""
    W = f"/root/rs/w_{idx}"
    try:
        if os.path.exists(W):
            shutil.rmtree(W)
        os.makedirs(f"{W}/in", exist_ok=True)

        # ⚠️ casa por NUMERO do episodio, NUNCA por posicao: 1.150 dos 3.680 books tem
        # buraco na numeracao (ep faltando) — por posicao o audio de um idioma cairia
        # no episodio ERRADO, silenciosamente
        def link_de(vb, p):
            return (mapa_links_num().get(vb) or {}).get(str(p + 1))

        # video de origem (PT) — idem, pelo numero
        orig = link_de(book_pt, pos)
        if not orig:
            return False, "sem episodio"

        man, segs = baixa_hls(orig)
        if not man:
            return False, "video falhou"
        for n, d in segs.items():
            open(f"{W}/in/{n}", "wb").write(d)
        open(f"{W}/in/index.m3u8", "wb").write(man)

        # 3 qualidades (720 e as duas menores)
        for pasta, vf, vbr in (("720", None, None), ("480", "scale=-2:480", "900k"), ("360", "scale=-2:360", "600k")):
            out = f"{W}/v{pasta}"
            os.makedirs(out, exist_ok=True)
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", f"{W}/in/index.m3u8"]
            if vf is None:
                cmd += ["-an", "-c:v", "copy"]
            else:
                cmd += ["-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-b:v", vbr,
                        "-maxrate", vbr, "-bufsize", "1800k", "-an"]
            cmd += ["-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
                    "-hls_segment_filename", f"{out}/seg%03d.ts", f"{out}/index.m3u8"]
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            if p.returncode != 0:
                return False, f"ffmpeg {pasta} falhou"

        # audios: pt (do original) + as dublagens
        for lg in ["pt"] + [l for l in idiomas_map if l != "pt"]:
            out = f"{W}/a_{lg}"
            os.makedirs(out, exist_ok=True)
            if lg == "pt":
                fonte = f"{W}/in/index.m3u8"
            else:
                vb = idiomas_map.get(lg)
                u = link_de(vb, pos)
                if not u:
                    continue
                mm, ss = baixa_hls(u)
                if not ss:
                    continue
                d = f"{W}/din_{lg}"
                os.makedirs(d, exist_ok=True)
                for n, c in ss.items():
                    open(f"{d}/{n}", "wb").write(c)
                open(f"{d}/index.m3u8", "wb").write(mm)
                fonte = f"{d}/index.m3u8"
            p = subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", fonte, "-vn",
                                "-c:a", "aac", "-b:a", "96k", "-f", "hls", "-hls_time", "6",
                                "-hls_playlist_type", "vod", "-hls_segment_filename", f"{out}/seg%03d.ts",
                                f"{out}/index.m3u8"], capture_output=True, text=True, timeout=3600)
            if p.returncode != 0 and os.path.isdir(out):
                shutil.rmtree(out, ignore_errors=True)

        # legendas: transcreve o pt e traduz
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", f"{W}/in/index.m3u8",
                        "-vn", "-c:a", "aac", "-b:a", "96k", f"{W}/tr.m4a"], capture_output=True, timeout=900)
        from faster_whisper import WhisperModel
        global _MODELO
        with _lk:
            if _MODELO is None:
                _MODELO = WhisperModel("base", device="cpu", compute_type="int8")
            m = _MODELO
        segs_txt, _ = m.transcribe(f"{W}/tr.m4a", vad_filter=True, beam_size=1)
        cues = [(s.start, s.end, s.text.strip()) for s in segs_txt if s.text and s.text.strip()]
        dur = max([c[1] for c in cues], default=0) + 1
        legendas = {}
        if cues:
            def vtt(cs):
                L = ["WEBVTT", ""]
                for i, f, t in cs:
                    L += [f"{int(i//3600):02d}:{int((i%3600)//60):02d}:{i%60:06.3f} --> "
                          f"{int(f//3600):02d}:{int((f%3600)//60):02d}:{f%60:06.3f}", t or "...", ""]
                return ("\n".join(L)).encode()
            legendas["pt"] = vtt(cues)
            txt_full = " ".join(c[2] for c in cues)
            for lg in ("es", "fr", "de", "th", "id", "en"):
                import urllib.parse
                try:
                    q = urllib.parse.quote(txt_full[:4500])
                    d = baixa(f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl={lg}&dt=t&q={q}", 30)
                    tr = "".join(x[0] for x in json.loads(d.decode())[0] if x and x[0]) if d else ""
                    if tr:
                        partes = tr.split(". ")
                        n = len(cues)
                        bl = ([" ".join(partes[i*max(1,len(partes)//n):(i+1)*max(1,len(partes)//n)]) for i in range(n)]
                              if len(partes) >= n else [partes[i] if i < len(partes) else "" for i in range(n)])
                        legendas[lg] = vtt([(c[0], c[1], bl[i]) for i, c in enumerate(cues)])
                except Exception:
                    pass

        # master
        L = ["#EXTM3U", "#EXT-X-VERSION:6", "#EXT-X-INDEPENDENT-SEGMENTS"]
        tem_audio = [lg for lg in ["pt", "de", "es", "fr", "id", "th"] if os.path.isdir(f"{W}/a_{lg}")]
        for i, lg in enumerate(tem_audio):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="{NOME[lg]}",LANGUAGE="{lg}"{pad},'
                     f'URI="audio/{lg}/index.m3u8"')
        for i, lg in enumerate(sorted(legendas)):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="sub",NAME="{NOME[lg]}",LANGUAGE="{lg}"{pad},'
                     f'URI="subs/{lg}.m3u8"')
        for h_, bw in ((360, 700000), (480, 1100000), (720, 2000000)):
            L.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},AVERAGE-BANDWIDTH={int(bw*0.9)},'
                     f'RESOLUTION={h_ if h_==720 else int(h_*0.5625)}x{h_ if h_!=720 else 1280},AUDIO="aud",SUBTITLES="sub"')
            L.append(f"video/{h_}/index.m3u8")
        master = ("\n".join(L) + "\n").encode()

        # sobe
        P = f"dramio/{slug}/ep{pos+1:03d}"
        T = []
        for h_ in (360, 480, 720):
            d = f"{W}/v{h_}"
            if not os.path.isdir(d):
                continue
            for arq in sorted(os.listdir(d)):
                T.append((f"{P}/video/{h_}/{arq}", open(f"{d}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg in tem_audio:
            d = f"{W}/a_{lg}"
            for arq in sorted(os.listdir(d)):
                T.append((f"{P}/audio/{lg}/{arq}", open(f"{d}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg, dados in legendas.items():
            T.append((f"{P}/subs/{lg}.vtt", dados, "text/vtt"))
            pl = (f"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{int(dur)+1}\n"
                  f"#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:{dur:.3f},\n{lg}.vtt\n#EXT-X-ENDLIST\n")
            T.append((f"{P}/subs/{lg}.m3u8", pl.encode(), "application/vnd.apple.mpegurl"))
        T.append((f"{P}/master.m3u8", master, "application/vnd.apple.mpegurl"))

        # CAPAS por idioma (no primeiro episodio apenas — a capa e do drama ✓)
        if pos == 0:
            for lg, vb in (idiomas_map or {}).items():
                cov = (cat or {}).get(vb) if cat else None
                if not cov:
                    try:
                        _c = json.load(open("/root/rs/reelshort_catalog_pt_full.json", encoding="utf-8"))
                        cov = next((x.get("book_pic") for x in _c if x["book_id"] == vb), None)
                    except Exception:
                        cov = None
                if cov:
                    dd = baixa(cov, 45)
                    if dd:
                        T.append((f"dramio/{slug}/capa/{lg}.jpg", dd, "image/jpeg"))
            # index.json do drama: descreve tudo (o Codex le e sabe o que tem ✓)
            idx = {
                "slug": slug,
                "titulo": titulo_drama,
                "book_id_reelshort": book_pt,
                "episodios": eps_total,
                "idiomas_audio": sorted(tem_audio),
                "idiomas_legenda": sorted(legendas),
                "qualidades": [360, 480, 720],
                "estrutura": {
                    "master": "epNNN/master.m3u8",
                    "video": "epNNN/video/<altura>/index.m3u8",
                    "audio": "epNNN/audio/<idioma>/index.m3u8",
                    "legenda": "epNNN/subs/<idioma>.vtt",
                    "capa": "capa/<idioma>.jpg",
                },
            }
            T.append((f"dramio/{slug}/index.json", json.dumps(idx, ensure_ascii=False, indent=1).encode(),
                      "application/json"))
        okn = 0
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            for r in ex.map(lambda x: sobe(*x), T):
                okn += 1 if r else 0
        shutil.rmtree(W, ignore_errors=True)
        return okn > len(T) * 0.7, f"{okn}/{len(T)} arquivos · {len(tem_audio)} audios · {len(legendas)} legendas"
    except Exception as e:
        shutil.rmtree(W, ignore_errors=True)
        return False, str(e)[:120]



# ============================================================================
# PIPELINE NOVO — o master do ReelShort entrega MUITO mais do que a gente usava
# (medido em 13/10, nao deduzido):
#   · VIDEO: video/540p · video/720p · video/1080p  -> 1080p EXISTE. O "teto de
#     720p" veio de sondar a familia de URL errada (vod-112094/...-sd.m3u8).
#   · LEGENDAS OFICIAIS: 19 idiomas no MESMO master, .vtt embrulhado em .m3u8:
#     tr ru pl bg ko cs ro ar en de fr ja es in pt vi th zh-TW it
#     -> nao precisa de Whisper nem do Google Tradutor: legenda profissional e
#        o episodio fica ~10x mais rapido (CPU quase zero)
#   · AUDIO: muxado no video (MEDIA_AUDIO=nao), entao para a dublagem baixa-se a
#     MENOR rendition (~9,5 MB) em vez da maior (~36 MB)
# ============================================================================
ALTURAS = (1080, 720, 540)      # o que subir; a fonte entrega exatamente isso
LGS_SUB = None                  # None = todas as que o master oferecer


def analisa_master(u):
    """(video{altura: url}, subs{lg: url da playlist do vtt}) do master do ReelShort"""
    man = baixa(u)
    if not man:
        return {}, {}
    d = man.decode("utf-8", "replace")
    base = u.rsplit("/", 1)[0]
    video, subs = {}, {}
    linhas = [l.strip() for l in d.splitlines() if l.strip()]
    for i, l in enumerate(linhas):
        if l.startswith("#EXT-X-STREAM-INF"):
            r = re.search(r"RESOLUTION=(\d+)x(\d+)", l)
            nxt = linhas[i + 1] if i + 1 < len(linhas) else None
            if r and nxt and not nxt.startswith("#"):
                alt = min(int(r.group(1)), int(r.group(2)))   # vertical: altura = menor lado
                video[alt] = nxt if nxt.startswith("http") else f"{base}/{nxt}"
    for l in linhas:
        if l.startswith("#EXT-X-MEDIA") and "TYPE=SUBTITLES" in l:
            lm = re.search(r'LANGUAGE="([^"]+)"', l)
            um = re.search(r'URI="([^"]+)"', l)
            if lm and um:
                subs[lm.group(1)] = um.group(1) if um.group(1).startswith("http") else f"{base}/{um.group(1)}"
    return video, subs


def vtt_oficial(playlist_url):
    """a playlist da legenda embrulha 1 .vtt -> devolve os bytes do vtt"""
    p = baixa(playlist_url, 60)
    if not p:
        return None
    rel = [l.strip() for l in p.decode("utf-8", "replace").splitlines()
           if l.strip() and not l.startswith("#") and ".vtt" in l]
    if not rel:
        return None
    u = rel[0] if rel[0].startswith("http") else playlist_url.rsplit("/", 1)[0] + "/" + rel[0]
    return baixa(u, 60)


def whisper_legendas(fonte_m3u8, W):
    """PLANO B — familia sem legendas oficiais (vod-112094): transcreve o PT e traduz.
    So roda quando o master NAO traz legenda (75% dos pares)."""
    import urllib.parse
    global _MODELO
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", fonte_m3u8, "-vn",
                    "-c:a", "aac", "-b:a", "96k", f"{W}/tr.m4a"], capture_output=True, timeout=900)
    from faster_whisper import WhisperModel
    with _lk:
        if _MODELO is None:
            # cpu_threads=2: no padrao (todos os nucleos) varias transcricoes concorrentes
            # disputam os 12 nucleos e o lote inteiro trava
            _MODELO = WhisperModel("base", device="cpu", compute_type="int8", cpu_threads=2)
        m = _MODELO
    segs_txt, _ = m.transcribe(f"{W}/tr.m4a", vad_filter=True, beam_size=1)
    cues = [(s.start, s.end, s.text.strip()) for s in segs_txt if s.text and s.text.strip()]
    if not cues:
        return {}

    def vtt(cs):
        L = ["WEBVTT", ""]
        for i, f, t in cs:
            L += [f"{int(i//3600):02d}:{int((i%3600)//60):02d}:{i%60:06.3f} --> "
                  f"{int(f//3600):02d}:{int((f%3600)//60):02d}:{f%60:06.3f}", t or "...", ""]
        return ("\n".join(L)).encode()

    out = {"pt": vtt(cues)}
    txt_full = " ".join(c[2] for c in cues)
    for lg in ("es", "fr", "de", "th", "id", "en"):
        try:
            q = urllib.parse.quote(txt_full[:4500])
            d = baixa(f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl={lg}&dt=t&q={q}", 30)
            tr = "".join(x[0] for x in json.loads(d.decode())[0] if x and x[0]) if d else ""
            if tr:
                partes = tr.split(". ")
                n = len(cues)
                bl = ([" ".join(partes[i*max(1, len(partes)//n):(i+1)*max(1, len(partes)//n)]) for i in range(n)]
                      if len(partes) >= n else [partes[i] if i < len(partes) else "" for i in range(n)])
                out[lg] = vtt([(c[0], c[1], bl[i]) for i, c in enumerate(cues)])
        except Exception:
            pass
    return out


def roda_episodio(book_pt, pos, slug, idiomas_map, dub, cat, idx, titulo_drama="", eps_total=0):
    """UM episodio: video 540/720/1080 copiado da fonte + audio de cada dublagem +
    legendas OFICIAIS. Sem re-encode de video, sem Whisper, sem traducao."""
    W = f"/root/rs/w_{idx}"
    try:
        if os.path.exists(W):
            shutil.rmtree(W)
        os.makedirs(W, exist_ok=True)

        def link_de(vb, p):
            return (mapa_links_num().get(vb) or {}).get(str(p + 1))

        orig = link_de(book_pt, pos)
        if not orig:
            return False, "sem episodio"

        # ---------- origem ----------
        # familia MASTER (vtt-m3u8): 1080/720/540 + 19 legendas OFICIAIS
        # familia MEDIA  (vod-112094 / h264-ordinary): playlist de segmentos 720p, sem
        #   master escondido (testado: -hd/-fhd/-1080/master.m3u8/index.m3u8 = 404) e sem
        #   legenda oficial -> nesse caso as legendas saem do Whisper (plano B)
        vid, subs = analisa_master(orig)
        if not vid:
            vid, subs, modo = {720: orig}, {}, "media(720p)"
        else:
            modo = "master"
        quero = [h for h in ALTURAS if h in vid] or [max(vid)]
        dur = 0.0
        for h in quero:
            mm, ss = baixa_hls(vid[h])
            if not mm or not ss:
                continue
            dd = f"{W}/v{h}"
            os.makedirs(dd, exist_ok=True)
            for n, c in ss.items():
                open(f"{dd}/{n}", "wb").write(c)
            open(f"{dd}/index.m3u8", "wb").write(mm)
            if not dur:
                dur = sum(float(x) for x in re.findall(r"#EXTINF:([\d.]+)", mm.decode("utf-8", "replace")))
        feitos = [h for h in quero if os.path.isdir(f"{W}/v{h}")]
        if not feitos:
            return False, "nenhuma rendition baixou"
        menor = f"{W}/v{min(feitos)}/index.m3u8"

        # ---------- legendas: OFICIAIS do master; se a familia nao traz, Whisper ----------
        legendas = {}
        for lg, u in subs.items():
            if LGS_SUB and lg not in LGS_SUB:
                continue
            v = vtt_oficial(u)
            if v and v.lstrip().startswith(b"WEBVTT"):
                legendas["id" if lg == "in" else lg] = v    # 'in' = codigo antigo de indonesio
        if not legendas:
            legendas = whisper_legendas(menor, W)

        # ---------- audios ----------
        tem_audio = []
        for lg in ["pt"] + [l for l in idiomas_map if l != "pt"]:
            out = f"{W}/a_{lg}"
            os.makedirs(out, exist_ok=True)
            if lg == "pt":
                fonte = menor
            else:
                vb = idiomas_map.get(lg)
                u = link_de(vb, pos)
                if not u:
                    shutil.rmtree(out, ignore_errors=True)
                    continue
                # 3 tentativas: sob throttle do CDN a 1a volta vazia e o episodio sairia
                # com MENOS faixas de audio do que o drama tem (visto: 2 em vez de 4) ✗
                mm = ss = v2 = None
                for tent in range(3):
                    if v2 is None:
                        v2, _ = analisa_master(u)
                    fonte_dub = v2[min(v2)] if v2 else u
                    mm, ss = baixa_hls(fonte_dub)
                    if ss:
                        break
                    if tent < 2:
                        time.sleep(1.5 + 2.5 * tent)
                if not ss:
                    shutil.rmtree(out, ignore_errors=True)
                    continue
                dd = f"{W}/din_{lg}"
                os.makedirs(dd, exist_ok=True)
                for n, c in ss.items():
                    open(f"{dd}/{n}", "wb").write(c)
                open(f"{dd}/index.m3u8", "wb").write(mm)
                fonte = f"{dd}/index.m3u8"
            p = subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", fonte, "-vn",
                                "-c:a", "aac", "-b:a", "96k", "-f", "hls", "-hls_time", "6",
                                "-hls_playlist_type", "vod", "-hls_segment_filename",
                                f"{out}/seg%03d.ts", f"{out}/index.m3u8"],
                               capture_output=True, text=True, timeout=3600)
            if p.returncode == 0 and os.path.isdir(out) and os.listdir(out):
                tem_audio.append(lg)
            else:
                shutil.rmtree(out, ignore_errors=True)

        # ---------- master ----------
        L = ["#EXTM3U", "#EXT-X-VERSION:6", "#EXT-X-INDEPENDENT-SEGMENTS"]
        for i, lg in enumerate(tem_audio):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="{NOME.get(lg, lg)}",'
                     f'LANGUAGE="{lg}"{pad},URI="audio/{lg}/index.m3u8"')
        for i, lg in enumerate(sorted(legendas)):
            pad = ",DEFAULT=YES,AUTOSELECT=YES" if i == 0 else ",AUTOSELECT=YES"
            L.append(f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="sub",NAME="{NOME.get(lg, lg)}",'
                     f'LANGUAGE="{lg}"{pad},URI="subs/{lg}.m3u8"')
        for h in sorted(feitos):
            bw = {1080: 2800000, 720: 1500000, 540: 1000000}.get(h, 1000000)
            L.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},AVERAGE-BANDWIDTH={int(bw*0.9)},'
                     f'RESOLUTION={int(h*9/16)}x{h},AUDIO="aud",SUBTITLES="sub"')
            L.append(f"video/{h}/index.m3u8")
        master = ("\n".join(L) + "\n").encode()

        # ---------- sobe ----------
        P = f"dramio/{slug}/ep{pos+1:03d}"
        T = []
        for h in feitos:
            dd = f"{W}/v{h}"
            for arq in sorted(os.listdir(dd)):
                T.append((f"{P}/video/{h}/{arq}", open(f"{dd}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg in tem_audio:
            dd = f"{W}/a_{lg}"
            for arq in sorted(os.listdir(dd)):
                T.append((f"{P}/audio/{lg}/{arq}", open(f"{dd}/{arq}", "rb").read(),
                          "application/vnd.apple.mpegurl" if arq.endswith(".m3u8") else "video/mp2t"))
        for lg, dados in legendas.items():
            T.append((f"{P}/subs/{lg}.vtt", dados, "text/vtt"))
            pl = (f"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{int(dur)+1}\n"
                  f"#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:{dur:.3f},\n"
                  f"{lg}.vtt\n#EXT-X-ENDLIST\n")
            T.append((f"{P}/subs/{lg}.m3u8", pl.encode(), "application/vnd.apple.mpegurl"))
        T.append((f"{P}/master.m3u8", master, "application/vnd.apple.mpegurl"))

        if pos == 0:
            for lg, vb in (idiomas_map or {}).items():
                cov = (cat or {}).get(vb) if cat else None
                if not cov:
                    try:
                        _c = json.load(open("/root/rs/reelshort_catalog_pt_full.json", encoding="utf-8"))
                        cov = next((x.get("book_pic") for x in _c if x["book_id"] == vb), None)
                    except Exception:
                        cov = None
                if cov:
                    dd2 = baixa(cov, 45)
                    if dd2:
                        T.append((f"dramio/{slug}/capa/{lg}.jpg", dd2, "image/jpeg"))
            i2 = {"slug": slug, "titulo": titulo_drama, "book_id_reelshort": book_pt,
                  "episodios": eps_total, "idiomas_audio": sorted(tem_audio),
                  "idiomas_legenda": sorted(legendas), "qualidades": sorted(feitos),
                  "estrutura": {"master": "epNNN/master.m3u8",
                                "video": "epNNN/video/<altura>/index.m3u8",
                                "audio": "epNNN/audio/<idioma>/index.m3u8",
                                "legenda": "epNNN/subs/<idioma>.vtt",
                                "capa": "capa/<idioma>.jpg"},
                  "fonte": "ReelShort · video 540/720/1080 e legendas OFICIAIS"}
            T.append((f"dramio/{slug}/index.json", json.dumps(i2, ensure_ascii=False, indent=1).encode(),
                      "application/json"))

        okn = 0
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            for r in ex.map(lambda x: sobe(*x), T):
                okn += 1 if r else 0
        shutil.rmtree(W, ignore_errors=True)
        return okn > len(T) * 0.7, (f"{okn}/{len(T)} arquivos · {len(tem_audio)} audios · "
                                    f"{len(legendas)} subs · {sorted(feitos)}")
    except Exception as e:
        shutil.rmtree(W, ignore_errors=True)
        return False, str(e)[:120]


LINKS_UNIF = None
LINKS_NUM = None


def mapa_links_num():
    """{book: {"1": m3u8, "2": m3u8, ...}} — a fonte CERTA para o lote.

    Le o links_unificado_num.json (merge das 7 fontes: cf + pt + slow + derivados +
    dub + dub2 + dub_movie = 3.680 books · 237.639 links). Casa por NUMERO, nao por
    posicao, porque 1.150 books tem buraco na numeracao.
    Se o arquivo nao existir, cai no mapa antigo (lista) — nao quebra, mas ai a
    posicao pode desalinhar."""
    global LINKS_NUM
    if LINKS_NUM is not None:
        return LINKS_NUM
    R = "/root/rs"
    num = {}
    for nome in ("links_unificado_num.json", "links_unificado.json"):
        try:
            d = json.load(open(f"{R}/{nome}", encoding="utf-8")) or {}
        except Exception:
            continue
        if not d:
            continue
        # se veio em lista, converte pra numero (posicao)
        amostra = next(iter(d.values()))
        if isinstance(amostra, list):
            num = {b: {str(i + 1): u for i, u in enumerate(v)} for b, v in d.items()}
        else:
            num = {b: {str(k): u for k, u in (v or {}).items()} for b, v in d.items()}
        print(f"mapa_links_num: {nome} -> {len(num)} books", flush=True)
        break
    if not num:
        print("mapa_links_num: NENHUM arquivo unificado — usando o mapa antigo (posicao!)", flush=True)
        num = {b: {str(i + 1): u for i, u in enumerate(lst)} for b, lst in mapa_links().items()}
    LINKS_NUM = num
    return LINKS_NUM


def mapa_links():
    """Une TODAS as fontes de links num unico mapa: book -> lista ordenada de m3u8.
    (antes o lote so olhava links_cf -> pegava 45 de 559 dramas)"""
    global LINKS_UNIF
    if LINKS_UNIF is not None:
        return LINKS_UNIF
    R = "/root/rs"
    por_book = {}

    def add(b, ep, u):
        if not b or not u:
            return
        por_book.setdefault(b, {})
        if ep is None:
            ep = len(por_book[b]) + 1
        por_book[b].setdefault(ep, u)

    # links_cf: {book: [{ep, m3u8}]}
    try:
        for b, lst in (json.load(open(f"{R}/links_cf.json", encoding="utf-8")) or {}).items():
            for e in (lst or []):
                add(b, e.get("ep"), e.get("m3u8"))
    except Exception:
        pass
    # links_pt / links_slow: {book: {episodes: [{episode_number, m3u8}]}}
    for arq in ("links_pt.json", "links_slow.json"):
        try:
            for b, v in (json.load(open(f"{R}/{arq}", encoding="utf-8")) or {}).items():
                for e in (v.get("episodes") or []):
                    add(b, e.get("episode_number"), e.get("m3u8"))
        except Exception:
            pass
    # links_derivados: {book: {links: [...]}}
    try:
        for b, v in (json.load(open(f"{R}/links_derivados.json", encoding="utf-8")) or {}).items():
            for e in ((v or {}).get("links") or []):
                add(b, e.get("ep"), e.get("m3u8"))
    except Exception:
        pass
    # links_dub: {book: {links: [...]}}
    try:
        for b, v in (json.load(open(f"{R}/links_dub.json", encoding="utf-8")) or {}).items():
            for e in ((v or {}).get("links") or []):
                add(b, e.get("ep"), e.get("m3u8"))
    except Exception:
        pass

    # ordena por numero do episodio
    LINKS_UNIF = {b: [u for _, u in sorted(d.items(), key=lambda x: x[0])] for b, d in por_book.items()}
    return LINKS_UNIF


_MODELO = None


def main():
    limite = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    PAR = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    R = "/root/rs"
    pre = json.load(open(f"{R}/dramio_preencher.json", encoding="utf-8"))
    mat = json.load(open(f"{R}/matriz_rapida.json", encoding="utf-8"))
    # links_dub.json NAO e mais usado pelo pipeline novo: o audio de cada idioma vem do
    # links_unificado_num.json (via mapa_links_num). Nos runners do GitHub ele nem existe
    # -> a leitura tem que tolerar a ausencia, senao o shard morre aqui ✗
    try:
        dub = json.load(open(f"{R}/links_dub.json", encoding="utf-8"))
    except Exception:
        dub = {}
    lcf = json.load(open(f"{R}/links_cf.json", encoding="utf-8"))

    # so dramas que tem audio PT e episodios com link
    # ⚠️ DEDUPLICA POR DRAMA — bug sério que estava aqui:
    # `dramio_preencher.json` tem 700 entradas / 559 book_ids para apenas 433 drama_ids:
    # 126 book_ids sao matches EXTRAS do MESMO drama. Montando os alvos por book_id, o
    # lote processava o mesmo drama varias vezes, escrevendo no MESMO slug (o slug vem do
    # titulo) -> o match mais pobre SOBRESCREVIA o mais rico e o episodio terminava com
    # menos faixas de audio (visto em producao: 2 em vez de 4, 1 em vez de 4) ✗✗
    # Fica o match com MAIS idiomas (e, no empate, o com mais episodios).
    por_drama = {}
    for r in pre:
        b = r.get("book_id")
        if not b:
            continue
        m = mat.get(b) or {}
        n = len(mapa_links_num().get(b) or {})
        if n == 0:
            continue
        idiomas = {lg: (x or {}).get("book") for lg, x in ((m.get("idiomas") or {}).items())}
        did = r.get("drama_id") or b
        cand = {"titulo": r.get("title_dramio"), "slug": slug_de(r.get("title_dramio")),
                "eps": n, "idiomas": idiomas, "book": b, "n_id": len(idiomas)}
        atual = por_drama.get(did)
        if atual is None or cand["n_id"] > atual["n_id"] or \
                (cand["n_id"] == atual["n_id"] and cand["eps"] > atual["eps"]):
            por_drama[did] = cand

    alvos = {v["book"]: {"titulo": v["titulo"], "slug": v["slug"], "eps": v["eps"],
                         "idiomas": v["idiomas"]}
             for v in list(por_drama.values())[:limite]}
    print(f"dramas alvo: {len(alvos)} (deduplicados de {len(pre)} entradas · "
          f"{sum(1 for r in pre if r.get('book_id'))} book_ids)", flush=True)

    estado = {}
    if os.path.exists(ESTADO):
        try:
            estado = json.load(open(ESTADO, encoding="utf-8"))
        except Exception:
            estado = {}
    feitos = set(estado.get("feitos") or [])
    print(f"ja feitos antes: {len(feitos)}", flush=True)

    tarefas = []
    for b, info in alvos.items():
        for pos in range(info["eps"]):
            chave = f"{b}:{pos}"
            if chave not in feitos:
                tarefas.append((b, pos, info))
    # MODO SHARD (GitHub Actions): cada runner pega 1/N da fila.
    # O indice e estavel (a ordem de `tarefas`), entao N shards cobrem tudo sem sobrepor ✓
    SHARD = os.environ.get("SHARD")
    SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL") or 0)
    if SHARD is not None and SHARD_TOTAL > 0:
        s = int(SHARD)
        tarefas = [t for i, t in enumerate(tarefas) if i % SHARD_TOTAL == s]
        print(f"SHARD {s}/{SHARD_TOTAL}: {len(tarefas)} episodios desta fatia", flush=True)
    print(f"episodios a processar: {len(tarefas)} | paralelismo: {PAR}", flush=True)
    print(f"aquecendo {aquece()} tokens de upload do B2...", flush=True)

    t0 = time.time()
    okc = errc = 0
    with cf.ThreadPoolExecutor(max_workers=PAR) as ex:
        futs = {ex.submit(roda_episodio, b, p, i["slug"], i["idiomas"], dub, None, f"{b[:8]}_{p}", i["titulo"], i["eps"]): (b, p, i)
                for b, p, i in tarefas}
        n = 0
        for f in cf.as_completed(futs):
            b, p, i = futs[f]
            n += 1
            try:
                ok, msg = f.result()
            except Exception as e:
                ok, msg = False, str(e)[:80]
            if ok:
                okc += 1
                feitos.add(f"{b}:{p}")
            else:
                errc += 1
            if n % 10 == 0 or n == len(tarefas):
                dt = time.time() - t0
                taxa = n / max(1, dt)
                resta = (len(tarefas) - n) / max(0.001, taxa)
                print(f"  {n}/{len(tarefas)} | ok={okc} err={errc} | {dt/60:.0f}min | "
                      f"faltam ~{resta/60:.0f}min | {i['titulo'][:30]} {msg[:60]}", flush=True)
                json.dump({"feitos": sorted(feitos)}, open(ESTADO, "w", encoding="utf-8"))
    json.dump({"feitos": sorted(feitos)}, open(ESTADO, "w", encoding="utf-8"))
    print(f"\n=== FIM: {okc} ok · {errc} erros · {(time.time()-t0)/60:.0f} min ===", flush=True)


# sem o guard, IMPORTAR o modulo ligava o lote — e nao dava pra testar 1 episodio
if __name__ == "__main__":
    main()
