"""ZTE MF833V dongle control + IP rotation cache.

We use the dongle in HiLink mode — it presents as a USB-Ethernet adapter at
192.168.0.0/24 with its own internal router. The modem layer is hidden, so
mmcli/ModemManager don't see it. To rotate the public IP, we hit the dongle's
internal web API at http://192.168.0.1.

This module:

- Tracks every public IP we've used through the dongle in
  ~/.cache/listo/dongle_ips.json, with attempts/successes/blocks counters.
- Asks the dongle to disconnect+reconnect when we need a new IP.
- Loops "check current IP → if recently used, rotate, repeat" so we land on
  a fresh-to-us IP without manually fiddling with the modem.

CLI: `python -m listo.fetch.dongle status | rotate | history | clean`.
"""
from __future__ import annotations

import argparse
import base64
import http.cookiejar
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# Where we persist the IP history. Stable across runs by design — the whole
# point is that today's "this IP was burned by Kasada" is still valid tomorrow.
_CACHE_PATH = Path.home() / ".cache" / "listo" / "dongle_ips.json"

# ZTE MF833V default gateway. Override via LISTO_DONGLE_GATEWAY if your
# dongle uses a different LAN subnet (some carriers reflash to 192.168.1.1).
_GATEWAY = os.environ.get("LISTO_DONGLE_GATEWAY", "192.168.0.1")

# Network interface the dongle attaches as. The udev-style names vary
# (enp0s20f0u3, enxXXXXXX, etc.) so we let it be overridden.
_INTERFACE = os.environ.get("LISTO_DONGLE_INTERFACE", "enp0s20f0u3")

# ZTE web-UI password (NOT the Wi-Fi password if any). Stock firmware on the
# Telstra MF833V usually requires login before write commands like
# DISCONNECT/CONNECT_NETWORK go through. Set via LISTO_DONGLE_PASSWORD env
# var. Common defaults to try if you don't know it: last 8 digits of the
# IMEI, or whatever's printed on the dongle's sticker.
_PASSWORD = os.environ.get("LISTO_DONGLE_PASSWORD", "")

# After a disconnect+reconnect, give the modem time to re-establish the
# data session before we trust the new IP. 15s is empirical for AU LTE.
_RECONNECT_WAIT_SECONDS = 15

# Default "recently used" window — if we've used an IP in the last N hours
# we'd rather rotate than risk hitting Kasada's accumulated risk score on it.
_DEFAULT_RECENT_HOURS = 6


def _load_cache() -> dict:
    if not _CACHE_PATH.exists():
        return {"ips": {}}
    try:
        with _CACHE_PATH.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("dongle IP cache unreadable (%s); starting fresh", e)
        return {"ips": {}}


def _save_cache(cache: dict) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CACHE_PATH.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)
    tmp.replace(_CACHE_PATH)


def get_current_ip(interface: str = _INTERFACE, timeout: int = 10) -> str | None:
    """Resolve the current public IP via the dongle interface.

    Bound to the specific interface so we get the dongle's egress even if
    other routes (WiFi, ethernet) are also up. Returns None on failure.
    """
    try:
        result = subprocess.run(
            ["curl", "-s", "--max-time", str(timeout),
             "--interface", interface, "https://api.ipify.org"],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        ip = result.stdout.strip()
        # api.ipify.org returns just the IP as plain text; sanity check.
        if ip.count(".") == 3 and all(p.isdigit() for p in ip.split(".")):
            return ip
        logger.warning("ipify returned unexpected content: %r", ip[:80])
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.warning("get_current_ip failed: %s", e)
    return None


def record_ip(ip: str, *, outcome: str = "seen") -> dict:
    """Update the cache for `ip`. outcome ∈ {"seen", "good", "bad"}.

    "seen" just touches the timestamp/attempts (we used the IP, no verdict yet).
    "good" / "bad" also bumps the success/block counter — call from the
    fetcher's success path / BlockedError path so the cache learns over time
    which IPs Kasada accepts and which it has flagged.
    """
    if outcome not in {"seen", "good", "bad"}:
        raise ValueError(f"outcome must be seen/good/bad, got {outcome!r}")
    cache = _load_cache()
    now = int(time.time())
    entry = cache["ips"].get(ip) or {
        "first_seen": now,
        "last_seen": now,
        "attempts": 0,
        "successes": 0,
        "blocks": 0,
    }
    entry["last_seen"] = now
    entry["attempts"] += 1
    if outcome == "good":
        entry["successes"] += 1
    elif outcome == "bad":
        entry["blocks"] += 1
    cache["ips"][ip] = entry
    _save_cache(cache)
    return entry


def seen_recently(ip: str, hours: int = _DEFAULT_RECENT_HOURS) -> bool:
    cache = _load_cache()
    entry = cache["ips"].get(ip)
    if not entry:
        return False
    age_hours = (time.time() - entry["last_seen"]) / 3600
    return age_hours < hours


def is_known_bad(ip: str) -> bool:
    """Heuristic: the IP has been blocked at least once and blocks ≥ successes.

    Conservative — one block on a brand-new IP marks it bad. We can soften
    this later if we find Kasada blocks transiently and the IP recovers.
    """
    cache = _load_cache()
    entry = cache["ips"].get(ip)
    if not entry:
        return False
    blocks = entry.get("blocks", 0)
    successes = entry.get("successes", 0)
    return blocks > 0 and blocks >= successes


def _zte_session(gateway: str) -> urllib.request.OpenerDirector:
    """Build a urllib opener with cookie support — login state lives in cookies."""
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [
        ("Referer", f"http://{gateway}/index.html"),
        ("User-Agent", "Mozilla/5.0"),
    ]
    return opener


def _zte_post(opener, gateway: str, fields: dict) -> dict | None:
    """POST a goform_set_cmd_process call. Returns parsed JSON or None on error.

    The endpoint returns JSON like {"result":"success"} or {"result":"failure"}.
    """
    body = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(
        f"http://{gateway}/goform/goform_set_cmd_process",
        data=body, method="POST",
    )
    try:
        with opener.open(req, timeout=10) as r:
            data = r.read()
        return json.loads(data)
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        logger.warning("ZTE POST %s failed: %s", fields.get("goformId"), e)
        return None


def _zte_login(opener, gateway: str, password: str) -> bool:
    """Authenticate against the ZTE web UI. Cookie ends up on the opener.

    The firmware base64-encodes the password before sending. Result codes:
      "0" success, "3" wrong password, "1" locked out, others firmware-specific.
    """
    if not password:
        logger.warning("LISTO_DONGLE_PASSWORD not set — skipping login")
        return False
    encoded = base64.b64encode(password.encode()).decode()
    res = _zte_post(opener, gateway, {
        "isTest": "false",
        "goformId": "LOGIN",
        "password": encoded,
    })
    if not res:
        return False
    rc = str(res.get("result", ""))
    if rc == "0":
        logger.info("ZTE login successful")
        return True
    if rc == "1":
        logger.warning("ZTE login: account locked (too many failed attempts)")
    elif rc == "3":
        logger.warning("ZTE login: wrong password")
    else:
        logger.warning("ZTE login unexpected result: %s", res)
    return False


def rotate_zte_disconnect(
    gateway: str = _GATEWAY,
    wait: int = _RECONNECT_WAIT_SECONDS,
    password: str = "",
) -> bool:
    """Light rotation: drop + re-establish the data session over the web API.

      DISCONNECT_NETWORK → tear down PPP/IP session
      CONNECT_NETWORK    → re-attach (carrier issues a new IP from the pool)

    Both endpoints require an authenticated session, so we log in first.
    Returns True only if both posts return result="success". A "failure" body
    almost always means we're not authenticated — set LISTO_DONGLE_PASSWORD.
    """
    pw = password or _PASSWORD
    opener = _zte_session(gateway)
    if not pw or not _zte_login(opener, gateway, pw):
        return False

    disc = _zte_post(opener, gateway, {
        "isTest": "false", "goformId": "DISCONNECT_NETWORK",
    })
    if not disc or str(disc.get("result")) != "success":
        logger.warning("DISCONNECT_NETWORK rejected: %s", disc)
        return False
    time.sleep(5)
    conn = _zte_post(opener, gateway, {
        "isTest": "false", "goformId": "CONNECT_NETWORK",
    })
    if not conn or str(conn.get("result")) != "success":
        logger.warning("CONNECT_NETWORK rejected: %s", conn)
        return False
    logger.info("ZTE light rotation issued; waiting %ds for new session", wait)
    time.sleep(wait)
    return True


def rotate_zte_reboot(
    gateway: str = _GATEWAY,
    wait: int = 45,
    password: str = "",
) -> bool:
    """Medium rotation: reboot the dongle.

    Telstra holds the IP for a "sticky session" window after DISCONNECT_NETWORK,
    so the soft path often gives back the same IP. A full reboot forces the
    modem to fully re-attach to the carrier, which reassigns a fresh IP from
    the pool. ~30-45s downtime; preserves all dongle settings (unlike factory
    reset). Requires login.
    """
    pw = password or _PASSWORD
    opener = _zte_session(gateway)
    if not pw or not _zte_login(opener, gateway, pw):
        return False
    res = _zte_post(opener, gateway, {
        "isTest": "false", "goformId": "REBOOT_DEVICE",
    })
    if not res or str(res.get("result")) != "success":
        logger.warning("REBOOT_DEVICE rejected: %s", res)
        return False
    logger.info("ZTE reboot issued; waiting %ds for re-attach", wait)
    time.sleep(wait)
    return True


def rotate_zte_factory_reset(
    gateway: str = _GATEWAY,
    wait: int = 60,
) -> bool:
    """Heavy rotation: trigger the dongle's factory reset.

    This works WITHOUT the web UI password — RESTORE_FACTORY_SETTINGS is
    unauth-accessible on stock Telstra-locked MF833V firmware. The downside is
    the dongle fully reboots (USB drops, comes back ~45s later) and any custom
    Wi-Fi name/password is wiped — fine for our USB-only use case but worth
    knowing.

    Empirically the carrier reassigns a fresh IP on the new attach, which is
    exactly what we want.
    """
    opener = _zte_session(gateway)
    res = _zte_post(opener, gateway, {
        "isTest": "false", "goformId": "RESTORE_FACTORY_SETTINGS",
    })
    if not res or str(res.get("result")) != "success":
        logger.warning("RESTORE_FACTORY_SETTINGS rejected: %s", res)
        return False
    logger.info("ZTE factory reset issued; waiting %ds for re-attach", wait)
    time.sleep(wait)
    return True


def rotate_zte(
    gateway: str = _GATEWAY,
    wait: int = _RECONNECT_WAIT_SECONDS,
    password: str = "",
    method: str = "auto",
) -> bool:
    """Try whatever rotation mechanism we have available.

    method="light"   → DISCONNECT/CONNECT (~20s, but Telstra often returns the
                       SAME IP — the carrier holds the session sticky)
    method="reboot"  → REBOOT_DEVICE (~45s; fresh modem attach reliably gets a
                       new IP; preserves dongle config) — DEFAULT in auto mode
    method="heavy"   → RESTORE_FACTORY_SETTINGS (~60s; wipes config; works
                       without auth)
    method="auto"    → reboot if a password is set; otherwise heavy.

    Light is in the API for completeness but we found it doesn't actually
    rotate the IP on Telstra's network (sticky session). reboot is the right
    default once you have the web-UI password.
    """
    pw = password or _PASSWORD
    if method == "light":
        return rotate_zte_disconnect(gateway, wait, pw)
    if method == "reboot":
        return rotate_zte_reboot(gateway, password=pw)
    if method == "heavy":
        return rotate_zte_factory_reset(gateway)
    # auto:
    if pw:
        if rotate_zte_reboot(gateway, password=pw):
            return True
        logger.info("reboot rotation failed, falling back to factory reset")
    return rotate_zte_factory_reset(gateway)


def rotate_until_fresh(
    max_attempts: int = 5,
    hours: int = _DEFAULT_RECENT_HOURS,
    interface: str = _INTERFACE,
) -> str | None:
    """Rotate the dongle until we land on an IP not in the recent-cache.

    Returns the new IP, or the last seen IP if max_attempts is exhausted
    (caller can decide whether to use it anyway).
    """
    last_ip: str | None = None
    for attempt in range(1, max_attempts + 1):
        ip = get_current_ip(interface)
        if ip is None:
            logger.warning("rotate_until_fresh: no IP yet (attempt %d), waiting", attempt)
            time.sleep(5)
            continue
        last_ip = ip
        if not seen_recently(ip, hours) and not is_known_bad(ip):
            logger.info("dongle IP %s is fresh (attempt %d)", ip, attempt)
            return ip
        verdict = "known bad" if is_known_bad(ip) else f"used <{hours}h ago"
        logger.info(
            "dongle IP %s is %s (attempt %d/%d), rotating",
            ip, verdict, attempt, max_attempts,
        )
        if not rotate_zte():
            logger.warning("rotation API call failed; aborting")
            break
    logger.warning(
        "rotate_until_fresh exhausted %d attempts; using %s anyway",
        max_attempts, last_ip,
    )
    return last_ip


# ─────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────

def _cmd_status() -> None:
    ip = get_current_ip()
    cache = _load_cache()
    entry = cache["ips"].get(ip) if ip else None
    print(f"interface : {_INTERFACE}")
    print(f"gateway   : {_GATEWAY}")
    print(f"public IP : {ip or '(unknown)'}")
    if entry:
        age_h = (time.time() - entry["last_seen"]) / 3600
        print(f"          first_seen={time.strftime('%Y-%m-%d %H:%M', time.localtime(entry['first_seen']))}")
        print(f"          last_seen ={time.strftime('%Y-%m-%d %H:%M', time.localtime(entry['last_seen']))} ({age_h:.1f}h ago)")
        print(f"          attempts={entry['attempts']} successes={entry['successes']} blocks={entry['blocks']}")
        verdict = "KNOWN BAD" if is_known_bad(ip) else (
            "USED RECENTLY" if seen_recently(ip) else "ok"
        )
        print(f"          verdict: {verdict}")
    else:
        print("          (not in cache yet — fresh to us)")
    print(f"cache     : {_CACHE_PATH} ({len(cache['ips'])} IPs total)")


def _cmd_rotate(max_attempts: int) -> None:
    before = get_current_ip()
    print(f"before: {before}")
    if before:
        record_ip(before, outcome="seen")
    ip = rotate_until_fresh(max_attempts=max_attempts)
    print(f"after : {ip}")


def _cmd_history(limit: int) -> None:
    cache = _load_cache()
    if not cache["ips"]:
        print("(no history)")
        return
    rows = sorted(
        cache["ips"].items(),
        key=lambda kv: kv[1]["last_seen"],
        reverse=True,
    )[:limit]
    print(f"{'IP':<16} {'last_seen':<17} {'first_seen':<17} {'att':>4} {'ok':>4} {'blk':>4}  verdict")
    for ip, e in rows:
        v = "BAD" if is_known_bad(ip) else (
            "RECENT" if seen_recently(ip) else "ok"
        )
        print(
            f"{ip:<16} "
            f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(e['last_seen'])):<17} "
            f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(e['first_seen'])):<17} "
            f"{e['attempts']:>4} {e['successes']:>4} {e['blocks']:>4}  {v}"
        )


def _cmd_health() -> None:
    """One-shot health snapshot — current IP + dongle modem state.

    Designed for the watchdog's heartbeat loop: a single line of state we can
    correlate with disconnects after the fact. Never raises; prints
    placeholders on probe failure so the heartbeat doesn't break.
    """
    ip = get_current_ip(timeout=3) or "?"
    base = f"http://{_GATEWAY}"
    try:
        req = urllib.request.Request(
            f"{base}/goform/goform_get_cmd_process?multi_data=1"
            "&cmd=modem_main_state,ppp_status,signal_strength,network_type,"
            "ppp_dial_conn_fail_counter,realtime_tx_thrpt,realtime_rx_thrpt",
            headers={"Referer": f"{base}/index.html"},
        )
        with urllib.request.urlopen(req, timeout=3) as r:
            d = json.loads(r.read())
    except Exception as e:  # noqa: BLE001
        d = {"_err": str(e)}
    # Single-line output for easy log scraping.
    pieces = [f"ip={ip}"]
    for k in ("modem_main_state", "ppp_status", "network_type",
              "signal_strength", "ppp_dial_conn_fail_counter"):
        v = d.get(k, "?")
        if v != "":
            pieces.append(f"{k}={v}")
    if "_err" in d:
        pieces.append(f"err={d['_err']}")
    print(" ".join(pieces))


def _cmd_rotate_if_bad() -> None:
    """Rotate ONLY if the current IP is in the cache marked bad.

    Watchdog hook for use on startup / before each launch. Distinct from
    `rotate` (which rotates unconditionally until fresh) because most
    startups happen on a perfectly-good IP and we don't want to burn 45s on
    every restart for nothing.
    """
    ip = get_current_ip()
    if not ip:
        print("(no IP — skipping)")
        return
    if not is_known_bad(ip):
        print(f"current IP {ip} is not known-bad — no rotation needed")
        return
    print(f"current IP {ip} is known-bad — rotating")
    new_ip = rotate_until_fresh(max_attempts=5)
    print(f"after rotation: {new_ip}")


def _cmd_record(outcome: str) -> None:
    """Record the current dongle IP with the given outcome.

    Used by the watchdog: `record-bad` after a stall so the cache learns
    which IPs Kasada is now flagging; `record-good` when we want to mark
    a known-working IP that we'd otherwise be forced to rotate from.
    """
    ip = get_current_ip()
    if not ip:
        print("(no IP — skipping)")
        return
    entry = record_ip(ip, outcome=outcome)
    print(f"recorded {outcome}: {ip} (attempts={entry['attempts']}, "
          f"successes={entry['successes']}, blocks={entry['blocks']})")


def _cmd_clean(older_than_days: int) -> None:
    cache = _load_cache()
    cutoff = time.time() - older_than_days * 86400
    keep = {ip: e for ip, e in cache["ips"].items() if e["last_seen"] >= cutoff}
    removed = len(cache["ips"]) - len(keep)
    cache["ips"] = keep
    _save_cache(cache)
    print(f"removed {removed} entries older than {older_than_days} days; kept {len(keep)}")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="ZTE dongle IP cache + rotation")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="show current IP and its cache entry")
    r = sub.add_parser("rotate", help="rotate until we get a fresh IP")
    r.add_argument("--max-attempts", type=int, default=5)
    h = sub.add_parser("history", help="show last N IPs we've used")
    h.add_argument("-n", "--limit", type=int, default=20)
    sub.add_parser("record-bad", help="mark current IP as blocked (watchdog hook)")
    sub.add_parser("record-good", help="mark current IP as working (watchdog hook)")
    sub.add_parser("rotate-if-bad", help="rotate ONLY if current IP is known-bad")
    sub.add_parser("health", help="one-line dongle health snapshot")
    c = sub.add_parser("clean", help="drop cache entries older than N days")
    c.add_argument("--older-than-days", type=int, default=30)
    args = p.parse_args()
    if args.cmd == "status":
        _cmd_status()
    elif args.cmd == "rotate":
        _cmd_rotate(args.max_attempts)
    elif args.cmd == "history":
        _cmd_history(args.limit)
    elif args.cmd == "record-bad":
        _cmd_record("bad")
    elif args.cmd == "record-good":
        _cmd_record("good")
    elif args.cmd == "rotate-if-bad":
        _cmd_rotate_if_bad()
    elif args.cmd == "health":
        _cmd_health()
    elif args.cmd == "clean":
        _cmd_clean(args.older_than_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
