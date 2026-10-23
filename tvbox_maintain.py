#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox source maintainer.

Pipeline (one run):
  1. Fetch upstream TVBox configs from SEEDS (priority order), parse tolerantly.
  2. Merge: fetched configs are first absolutized (relative spider / site
     api+ext / lives url+epg resolved against the upstream's own URL, since
     TVBox resolves relatives against the config URL and the merged file is
     re-hosted). Sites dedupe by (type, api, ext); type-3 (spider) sites are
     kept only from the primary upstream (the highest-priority OK upstream
     that carries a `spider` field) because the output config can only carry
     ONE spider. Type 0/1/4 sites merge from all upstreams. `lives` merge by
     name. parses/flags/ads/wallpaper come from the primary upstream.
  3. Validate sites/lives with lightweight probes (see probe_* below).
  4. Apply per-site failure state (state.json): a probed-dead site increments
     its consecutive-fail count; sites at >= 2 are excluded from the output
     (2-day grace: on a daily run, a site must fail twice in a row to drop).
  5. Emit out/tvbox.json (classic TVBox format) + out/report.txt.

Guard: if ALL upstreams fail to fetch, no tvbox.json is written.

Stdlib only. Notes: an unverified TLS context is used on purpose -- many
source hosts have broken/self-signed certs and the TVBox client itself is
equally tolerant; this is a liveness tool, not a trust decision.
"""

import base64
import hashlib
import json
import os
import re
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, "out")
STATE_PATH = os.path.join(BASE, "state.json")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

FETCH_TIMEOUT = 10          # seconds, per spec
PROBE_TIMEOUT = 10          # seconds, per request
PROBE_WORKERS = 12
PROBE_BUDGET = 420          # seconds wall-clock cap for the whole probe phase
CONFIG_MAX_BYTES = 16 * 1024 * 1024
PROBE_MAX_BYTES = 512 * 1024

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# Upstream seeds in priority order: (name, url)
SEEDS = [
    ("饭太硬",   "http://www.饭太硬.com/tv/"),
    ("肥猫",     "http://肥猫.com"),
    ("王二小",   "http://tvbox.王二小放牛娃.top"),
    ("讴歌",     "http://tv.nxog.top/m/"),
    ("巧技",     "http://cdn.qiaoji8.com/tvbox.json"),
    ("春盈天下", "https://盒子迷.top/春盈天下"),
    ("qist-js",  "https://raw.githubusercontent.com/gaotianliuyun/gao/master/js.json"),
]

SOURCES_PATH = os.path.join(BASE, "sources.txt")
UPSTREAM_ORIGIN = "built-in seeds"


def load_upstreams():
    """Upstream list comes from sources.txt next to this script when present:
    one entry per line, either 'name | url' or a bare URL ('#' comments and
    blank lines ignored; name defaults to the host). Falls back to the
    built-in SEEDS when the file is missing or has no usable entry."""
    global UPSTREAM_ORIGIN
    entries = []
    seen = set()
    try:
        with open(SOURCES_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "|" in line:
                    name, url = line.split("|", 1)
                    name, url = name.strip(), url.strip()
                else:
                    name, url = "", line
                if not url.lower().startswith(("http://", "https://")):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                if not name:
                    name = urlsplit(url).hostname or url
                entries.append((name, url))
    except OSError:
        entries = []
    if entries:
        UPSTREAM_ORIGIN = "sources.txt (%d entries)" % len(entries)
        return entries
    return list(SEEDS)


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def normalize_url(url):
    """IDNA-encode the host and percent-encode non-ASCII path/query so that
    urllib can handle Chinese domain names and paths."""
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        try:
            host = host.encode("idna").decode("ascii")
        except Exception:
            pass
        netloc = host
        if parts.port:
            netloc += ":%d" % parts.port
        path = quote(parts.path or "", safe="/%")
        query = quote(parts.query or "", safe="=&%?")
        return urlunsplit((parts.scheme, netloc, path, query, ""))
    except Exception:
        return url


def http_get(url, timeout, max_bytes):
    """Return (status, body_bytes). Raises on transport errors; HTTP error
    statuses are returned (not raised) so probes can see the code."""
    status, body, _final = http_get_full(url, timeout, max_bytes)
    return status, body


def http_get_full(url, timeout, max_bytes):
    """Like http_get, but also returns the final URL after redirects (used
    as the base for resolving an upstream config's relative references)."""
    req = urlrequest.Request(normalize_url(url), headers={
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    })
    try:
        with urlrequest.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            return resp.getcode(), resp.read(max_bytes), resp.geturl()
    except urlerror.HTTPError as e:
        try:
            body = e.read(max_bytes)
        except Exception:
            body = b""
        return e.code, body, url


def short_err(exc):
    msg = str(exc).strip()
    if not msg:
        msg = exc.__class__.__name__
    return (exc.__class__.__name__ + ": " + msg)[:110]


def decode_text(raw):
    for enc in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------
# Tolerant config parsing
# --------------------------------------------------------------------------

def _strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    out_lines = []
    for line in text.splitlines():
        m = re.search(r"(?<!:)//", line)   # '//' not part of '://'
        if m:
            line = line[:m.start()]
        out_lines.append(line)
    return "\n".join(out_lines)


def _strip_trailing_commas(text):
    return re.sub(r",\s*([}\]])", r"\1", text)


def parse_config(text):
    """Try hard to turn response text into a config dict. Returns dict|None."""
    if not text:
        return None
    t = text.strip().lstrip("\ufeff").strip()
    candidates = [t]
    cleaned = _strip_trailing_commas(_strip_comments(t))
    if cleaned != t:
        candidates.append(cleaned)
    for cand in candidates:
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    # Whole-body base64 (some configs are served encoded)
    blob = re.sub(r"[^A-Za-z0-9+/=_-]", "", t)
    if len(blob) >= 100:
        padded = blob + "=" * (-len(blob) % 4)
        for alt in (None, b"-_"):
            try:
                raw = base64.b64decode(padded, altchars=alt)
                obj = json.loads(raw.decode("utf-8-sig"))
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
    return None


# --------------------------------------------------------------------------
# Absolutize relative references
#
# TVBox resolves relative refs against the URL the config was loaded from.
# The merged output is re-hosted elsewhere, so every relative ref must be
# resolved against its OWN upstream's URL at merge time, otherwise spider /
# drpy-lib / lives references silently break.
# --------------------------------------------------------------------------

_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")
_FILE_EXT_RE = re.compile(r"\.(?:js|json|txt|jar|py|m3u8?|xml)(?:[?#]|$)")


def _is_relative_ref(value):
    """True when a string looks like a relative path reference (as opposed
    to an absolute URL, a 'csp_*' spider class name, or a JSON blob)."""
    if not isinstance(value, str):
        return False
    s = value.strip()
    if not s or s.startswith(("{", "[")):
        return False
    if s.startswith("//"):
        return False                      # protocol-relative: host is explicit
    if _SCHEME_RE.match(s):
        return False                      # http:, https:, clan:, file:, ...
    if s.startswith(("./", "../", "/")):
        return True
    # No scheme: treat as a path only if it actually looks like one, so bare
    # identifiers (e.g. spider class names like 'csp_XBPQ') stay untouched.
    if "/" in s or _FILE_EXT_RE.search(s):
        return True
    return False


def _resolve_ref(value, base):
    if not _is_relative_ref(value):
        return value
    try:
        joined = urljoin(base, value.strip())
    except Exception:
        return value
    if joined.startswith(("http://", "https://")):
        joined = normalize_url(joined)
    return joined


def _resolve_spider(value, base):
    """Spider syntax is '<url>[;md5;<hash>]...' -- resolve only the URL part."""
    if not isinstance(value, str) or not value:
        return value
    parts = value.split(";")
    parts[0] = _resolve_ref(parts[0], base)
    return ";".join(parts)


def _resolve_ext(value, base):
    """Only plain-string path exts are resolved. Dict/list exts, JSON-encoded
    strings, tokens and other values are left exactly as they are."""
    if not isinstance(value, str):
        return value
    s = value.strip()
    if s.startswith(("{", "[")):
        return value
    try:
        json.loads(s)                     # JSON blob (incl. quoted strings)
        return value
    except Exception:
        pass
    return _resolve_ref(value, base)


def absolutize_config(cfg, base_url):
    """Return a copy of an upstream config with spider / site api+ext /
    lives url+epg resolved against the upstream's own (final) URL."""
    cfg = dict(cfg)
    if cfg.get("spider"):
        cfg["spider"] = _resolve_spider(cfg["spider"], base_url)
    sites = cfg.get("sites")
    if isinstance(sites, list):
        new_sites = []
        for s in sites:
            if isinstance(s, dict):
                s = dict(s)
                if s.get("api"):
                    s["api"] = _resolve_ref(s["api"], base_url)
                if "ext" in s:
                    s["ext"] = _resolve_ext(s["ext"], base_url)
            new_sites.append(s)
        cfg["sites"] = new_sites
    lives = cfg.get("lives")
    if isinstance(lives, list):
        new_lives = []
        for lv in lives:
            if isinstance(lv, dict):
                lv = dict(lv)
                if lv.get("url"):
                    lv["url"] = _resolve_ref(lv["url"], base_url)
                if lv.get("epg"):
                    lv["epg"] = _resolve_ref(lv["epg"], base_url)
            new_lives.append(lv)
        cfg["lives"] = new_lives
    return cfg


# --------------------------------------------------------------------------
# Upstream fetch
# --------------------------------------------------------------------------

def fetch_upstream(name, url):
    rec = {"name": name, "url": url, "ok": False, "reason": "",
           "config": None, "n_sites_raw": 0, "has_spider": False,
           "contributed": 0}
    try:
        status, raw, final_url = http_get_full(url, FETCH_TIMEOUT, CONFIG_MAX_BYTES)
    except Exception as e:
        rec["reason"] = short_err(e)
        return rec
    if status != 200:
        rec["reason"] = "HTTP %d" % status
        return rec
    cfg = parse_config(decode_text(raw))
    if cfg is None:
        rec["reason"] = "unparseable body (%d bytes)" % len(raw)
        return rec
    rec["ok"] = True
    rec["config"] = absolutize_config(cfg, final_url or url)
    sites = cfg.get("sites")
    rec["n_sites_raw"] = len(sites) if isinstance(sites, list) else 0
    rec["has_spider"] = bool(cfg.get("spider"))
    return rec


# --------------------------------------------------------------------------
# Probes. Each returns (status, reason) with status in
# {"alive", "dead"}; callers classify non-probed sites separately.
# --------------------------------------------------------------------------

def probe_url(site):
    sep = "&" if "?" in site["api"] else "?"
    return site["api"] + sep + "ac=list"


def probe_site(site):
    """site: merged site dict. Returns (verdict, reason)."""
    stype = site_type(site)
    api = site.get("api", "")
    try:
        if stype == 1:
            status, raw = http_get(probe_url(site), PROBE_TIMEOUT, PROBE_MAX_BYTES)
            if status != 200:
                return "dead", "HTTP %d" % status
            try:
                obj = json.loads(decode_text(raw))
            except Exception:
                return "dead", "body not JSON"
            if isinstance(obj, dict) and ("list" in obj or "class" in obj):
                return "alive", "json list/class ok"
            return "dead", "JSON without list/class key"
        if stype == 0:
            status, raw = http_get(probe_url(site), PROBE_TIMEOUT, PROBE_MAX_BYTES)
            if status != 200:
                return "dead", "HTTP %d" % status
            body = decode_text(raw)
            if ("<list" in body) or ("<rss" in body) or ("<?xml" in body):
                return "alive", "xml ok"
            return "dead", "body not XML"
        if stype == 4:
            status, raw = http_get(api, PROBE_TIMEOUT, PROBE_MAX_BYTES)
            if status != 200:
                return "dead", "HTTP %d" % status
            try:
                json.loads(decode_text(raw))
                return "alive", "json ok"
            except Exception:
                return "dead", "body not JSON"
    except Exception as e:
        return "dead", short_err(e)
    return "unprobed", "no probe rule"


def probe_live(entry):
    url = entry.get("url", "")
    try:
        status, raw = http_get(url, PROBE_TIMEOUT, 64 * 1024)
    except Exception as e:
        return "dead", short_err(e)
    if status != 200:
        return "dead", "HTTP %d" % status
    if len(raw) > 100:
        return "alive", "%d bytes" % len(raw)
    return "dead", "body too small (%d bytes)" % len(raw)


def run_probes(tasks, kind):
    """tasks: list of keys; kind: 'site'|'live'. Returns {key: (verdict, reason)}.
    Tasks not finished within PROBE_BUDGET come back as ('unprobed','budget')."""
    results = {}
    if not tasks:
        return results
    fn = probe_site if kind == "site" else probe_live

    def rkey(t):
        return id(t) if kind == "site" else t["name"]

    ex = ThreadPoolExecutor(max_workers=PROBE_WORKERS)
    fut2key = {ex.submit(fn, t): t for t in tasks}
    try:
        for fut in as_completed(fut2key, timeout=PROBE_BUDGET):
            t = fut2key[fut]
            try:
                results[rkey(t)] = fut.result()
            except Exception as e:
                results[rkey(t)] = ("dead", short_err(e))
    except FuturesTimeout:
        pass
    # Do NOT wait for stragglers: cancel queued work; in-flight requests
    # end on their own within PROBE_TIMEOUT.
    ex.shutdown(wait=False, cancel_futures=True)
    # anything unfinished -> unprobed (budget)
    if kind == "site":
        for t in tasks:
            results.setdefault(id(t), ("unprobed", "probe budget exhausted"))
    else:
        for t in tasks:
            results.setdefault(t["name"], ("unprobed", "probe budget exhausted"))
    return results


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------

def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_PATH)


def ext_norm(site):
    """Normalized form of a site's ext for identity purposes: many drpy
    sites share one lib `api` and differ only by ext, so ext is part of a
    site's identity. Strings are used as-is; dict/list exts are serialized
    deterministically."""
    ext = site.get("ext")
    if ext is None:
        return ""
    if isinstance(ext, str):
        return ext.strip()
    try:
        return json.dumps(ext, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(ext)


def site_dedupe_key(site):
    return (site_type(site), site.get("api", ""), ext_norm(site))


def site_key(site):
    stype, api, en = site_dedupe_key(site)
    if len(en) > 200:      # keep state.json keys readable for huge exts
        en = "md5:" + hashlib.md5(en.encode("utf-8")).hexdigest()
    return "%s|%s|%s" % (stype, api, en)


def site_type(site):
    try:
        return int(site.get("type"))
    except Exception:
        return -1


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    started = time.time()
    now = datetime.now()
    report = []
    report.append("TVBox source maintenance report - %s" % now.strftime("%Y-%m-%d %H:%M:%S"))
    report.append("=" * 66)

    # ---- fetch upstreams (parallel; list order = priority) ----
    seeds = load_upstreams()
    report.append("upstreams from: %s" % UPSTREAM_ORIGIN)
    with ThreadPoolExecutor(max_workers=max(1, len(seeds))) as ex:
        ups = list(ex.map(lambda s: fetch_upstream(s[0], s[1]), seeds))

    ok_ups = [u for u in ups if u["ok"]]
    report.append("")
    report.append("[upstreams]")
    for u in ups:
        if u["ok"]:
            report.append("  OK    %-8s sites_raw=%-4d spider=%s  %s"
                          % (u["name"], u["n_sites_raw"], "yes" if u["has_spider"] else "no", u["url"]))
        else:
            report.append("  FAIL  %-8s %s  %s" % (u["name"], u["reason"], u["url"]))

    if not ok_ups:
        report.append("")
        report.append("GUARD: all upstreams failed - no tvbox.json written.")
        _write_report(report)
        print("\n".join(report))
        print("\n=== SUMMARY JSON ===")
        print(json.dumps({"guard_all_failed": True,
                          "upstreams": [_up_meta(u) for u in ups]}, ensure_ascii=False))
        return 1

    # ---- primary (for spider + parses/flags/ads/wallpaper) ----
    primary = None
    for u in ok_ups:
        if u["has_spider"]:
            primary = u
            break
    report.append("")
    report.append("[primary] %s" % (primary["name"] if primary else "(none with spider)"))

    # ---- merge sites ----
    merged = []            # list of site dicts (copies, with int 'type' shadow)
    seen = set()
    spider_skipped = 0
    for u in ok_ups:
        sites = u["config"].get("sites")
        if not isinstance(sites, list):
            continue
        for s in sites:
            if not isinstance(s, dict):
                continue
            st = site_type(s)
            api = s.get("api", "")
            if st == 3 and u is not primary:
                spider_skipped += 1
                continue
            key = site_dedupe_key(s)
            if key in seen:
                continue
            seen.add(key)
            entry = dict(s)
            entry["_type"] = st
            entry["_from"] = u["name"]
            merged.append(entry)
            u["contributed"] += 1
    for u in ups:
        if u["ok"]:
            report.append("  contributed %-8s %d sites" % (u["name"], u["contributed"]))
    report.append("  type-3 sites skipped (non-primary upstreams): %d" % spider_skipped)

    # ---- merge lives by name ----
    lives_merged = []
    seen_lives = set()
    for u in ok_ups:
        lives = u["config"].get("lives")
        if not isinstance(lives, list):
            continue
        for lv in lives:
            if not isinstance(lv, dict):
                continue
            nm = lv.get("name", "")
            if nm in seen_lives:
                continue
            seen_lives.add(nm)
            lives_merged.append(dict(lv))

    # ---- classify + probe sites ----
    probe_tasks = []
    verdicts = {}   # id(site) -> (verdict, reason)
    for s in merged:
        st = s["_type"]
        api = s.get("api", "")
        if st == 3:
            verdicts[id(s)] = ("spider", "primary spider site, not probed")
        elif st in (0, 1, 4) and isinstance(api, str) and api.startswith(("http://", "https://")):
            probe_tasks.append(s)
        else:
            verdicts[id(s)] = ("unprobed", "non-http api or unhandled type")
    site_probe = run_probes(probe_tasks, "site")
    verdicts.update(site_probe)

    # ---- probe lives ----
    live_tasks = [lv for lv in lives_merged
                  if isinstance(lv.get("url", ""), str)
                  and lv["url"].startswith(("http://", "https://"))]
    live_probe = run_probes(live_tasks, "live")
    kept_lives, dropped_lives = [], []
    for lv in lives_merged:
        v = live_probe.get(lv.get("name", ""), ("unprobed", "non-http url"))
        if v[0] == "dead":
            dropped_lives.append((lv.get("name", ""), v[1]))
        else:
            kept_lives.append(lv)

    # ---- state update + exclusion (fails>=2) ----
    state = load_state()
    kept_sites, removals = [], []
    tallies = {"alive": 0, "dead": 0, "unprobed": 0, "spider": 0}
    dead_samples = []
    for s in merged:
        verdict, reason = verdicts.get(id(s), ("unprobed", "no verdict"))
        tallies[verdict] = tallies.get(verdict, 0) + 1
        k = site_key(s)
        ent = state.setdefault(k, {"fails": 0, "name": s.get("name", "")})
        ent["name"] = s.get("name", "")
        ent["updated"] = now.strftime("%Y-%m-%d %H:%M:%S")
        if verdict == "alive":
            ent["fails"] = 0
            ent["last"] = "alive"
        elif verdict == "dead":
            ent["fails"] = int(ent.get("fails", 0)) + 1
            ent["last"] = "dead: " + reason
            dead_samples.append((s.get("name", ""), s["_type"], s.get("api", ""), reason,
                                 "fails=%d" % ent["fails"]))
        else:
            ent["last"] = verdict
        if ent.get("fails", 0) >= 2:
            removals.append((s.get("name", ""), s["_type"], s.get("api", ""),
                             "consecutive fails=%d (last: %s)" % (ent["fails"], ent.get("last", ""))))
            continue
        kept_sites.append(s)
    save_state(state)

    # ---- build output config (classic TVBox format) ----
    out_cfg = {}
    pcfg = primary["config"] if primary else {}
    if pcfg.get("spider"):
        out_cfg["spider"] = pcfg["spider"]
    if pcfg.get("wallpaper"):
        out_cfg["wallpaper"] = pcfg["wallpaper"]
    clean_sites = []
    for s in kept_sites:
        cs = {k: v for k, v in s.items() if not k.startswith("_")}
        clean_sites.append(cs)
    out_cfg["sites"] = clean_sites
    out_cfg["lives"] = kept_lives
    for field in ("parses", "flags", "ads"):
        if pcfg.get(field):
            out_cfg[field] = pcfg[field]

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "tvbox.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_cfg, f, ensure_ascii=False, indent=2)
    out_size = os.path.getsize(out_path)

    # ---- report ----
    report.append("")
    report.append("[sites]")
    report.append("  collected (merged, deduped): %d" % len(merged))
    report.append("  probed alive: %d" % tallies.get("alive", 0))
    report.append("  probed dead : %d" % tallies.get("dead", 0))
    report.append("  unprobed    : %d" % tallies.get("unprobed", 0))
    report.append("  spider (type-3, kept unprobed): %d" % tallies.get("spider", 0))
    report.append("  removed (state fails>=2): %d" % len(removals))
    report.append("  kept in output: %d" % len(clean_sites))
    report.append("")
    report.append("[lives]")
    report.append("  collected: %d  kept: %d  dropped: %d"
                  % (len(lives_merged), len(kept_lives), len(dropped_lives)))
    for nm, why in dropped_lives:
        report.append("  dropped live %-20s %s" % (nm, why))
    report.append("")
    report.append("[removals]")
    if removals:
        for nm, st, api, why in removals:
            report.append("  - [%s] type=%s %s (%s)" % (nm, st, api, why))
    else:
        report.append("  (none)")
    report.append("")
    report.append("[dead sites this run: %d]" % len(dead_samples))
    for nm, st, api, why, fk in dead_samples[:40]:
        report.append("  - [%s] type=%s %s -> %s (%s)" % (nm, st, api, why, fk))
    if len(dead_samples) > 40:
        report.append("  ... and %d more" % (len(dead_samples) - 40))
    report.append("")
    report.append("[output] %s (%d bytes), runtime %.1fs" % (out_path, out_size, time.time() - started))
    _write_report(report)

    print("\n".join(report))
    print("\n=== SUMMARY JSON ===")
    print(json.dumps({
        "guard_all_failed": False,
        "upstreams": [_up_meta(u) for u in ups],
        "primary": primary["name"] if primary else None,
        "sites_collected": len(merged),
        "sites_kept": len(clean_sites),
        "tallies": tallies,
        "spider_skipped": spider_skipped,
        "removals": [list(r) for r in removals],
        "dead_samples": [list(d) for d in dead_samples[:8]],
        "lives_collected": len(lives_merged),
        "lives_kept": len(kept_lives),
        "lives_dropped": [list(d) for d in dropped_lives],
        "out_size": out_size,
        "runtime_s": round(time.time() - started, 1),
    }, ensure_ascii=False))
    return 0


def _up_meta(u):
    return {"name": u["name"], "url": u["url"], "ok": u["ok"],
            "reason": u["reason"], "n_sites_raw": u["n_sites_raw"],
            "has_spider": u["has_spider"], "contributed": u["contributed"]}


def _write_report(lines):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "report.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
