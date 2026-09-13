#!/usr/bin/env python3
"""X/Twitter collector using twscrape (GraphQL, account-session based).

Reads accounts from the twscrape SQLite DB (TWSCRAPE_DB env, default
x_accounts.db). Requires at least one active account added via:

    twscrape add_cookie <name>        # paste auth_token + ct0 from x.com

Then collects recent tweets for the given accounts / queries and prints
a JSON array of normalized items to stdout.

Optimized for large account lists (2026-09-12): user lookups and tweet
fetches run concurrently (bounded by a semaphore), so a 50+ account config
finishes in ~1-2 min instead of timing out a 3-minute budget.

Usage:
    collect.py --accounts openai anthropicai --queries "coding agent"
    collect.py --accounts openai --limit 10
"""
import argparse
import asyncio
import json
import os
import sys

from twscrape import API

# Bounded concurrency: twscrape's single-account rate limiter tolerates a few
# in-flight requests; a big first burst triggers 429 cooldowns (10-20 min) that
# block the whole cycle — keep it gentle (3).
SEM = asyncio.Semaphore(3)

# Auth probe budget: a healthy session answers a user lookup in <1s. Keep it
# far below the Go sidecar budget so a dead session is reported immediately.
PROBE_TIMEOUT_S = 30


async def _account_tweets(api, handle, limit, out):
    """Fetch one account's recent tweets; never raises."""
    handle = handle.lstrip("@")
    try:
        async with SEM:
            user = await api.user_by_login(handle)
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"error": "user_by_login", "handle": handle,
                          "detail": str(e)}), file=sys.stderr)
        return
    if user is None:
        print(json.dumps({"error": "user_not_found", "handle": handle}),
              file=sys.stderr)
        return
    try:
        async with SEM:
            tweets = [t async for t in api.user_tweets(user.id, limit=limit)]
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"error": "user_tweets", "handle": handle,
                          "detail": str(e)}), file=sys.stderr)
        return
    for t in tweets:
        out.append(_item(t, handle))


async def _query_tweets(api, q, limit, out):
    """Run one search query; never raises."""
    try:
        async with SEM:
            tweets = [t async for t in api.search(q, limit=limit)]
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"error": "search", "query": q, "detail": str(e)}),
              file=sys.stderr)
        return
    for t in tweets:
        out.append(_item(t, t.user.username if t.user else "unknown"))


async def _list_tweets(api, lid, limit, out):
    """Fetch one X list timeline; never raises."""
    try:
        async with SEM:
            tweets = [t async for t in api.list_timeline(lid, limit=limit)]
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"error": "list_timeline", "list": lid,
                          "detail": str(e)}), file=sys.stderr)
        return
    for t in tweets:
        out.append(_item(t, t.user.username if t.user else "unknown"))


async def _session_ok(api):
    """Cheap probe: is the injected X session still authenticated?

    twscrape accepts expired cookies at add time and only fails on the first
    real request (`XClIdAccountError: Logged-out X web app`) — and then parks
    the calling task in `get_for_queue_or_wait` until the 15-minute queue
    cooldown expires, i.e. past the sidecar's budget. So the probe is capped:
    a live session answers a user lookup in <1s, anything slower means the
    session is dead/throttled and the cycle must be skipped now.

    Returns (ok, detail): ok=False only for auth failure / no answer.
    """
    try:
        await asyncio.wait_for(api.user_by_login("X"), timeout=PROBE_TIMEOUT_S)
        return True, ""
    except asyncio.TimeoutError:
        return False, f"probe timed out after {PROBE_TIMEOUT_S}s"
    except Exception as e:  # noqa: BLE001
        detail = f"{type(e).__name__}: {e}"
        if "Logged-out" in detail or "XClIdAccountError" in detail:
            return False, detail
        return True, detail


async def collect(accounts, queries, lists, limit):
    db = os.environ.get("TWSCRAPE_DB", "x_accounts.db")
    api = API(db)

    # Ensure the session cookie from env is registered (idempotent).
    auth = os.environ.get("X_AUTH_TOKEN")
    ct0 = os.environ.get("X_CT0")
    if auth and ct0:
        try:
            await api.pool.add_account_cookies("radar_session",
                                               json.dumps({"auth_token": auth,
                                                           "ct0": ct0}))
        except Exception as e:  # noqa: BLE001
            print(json.dumps({"warn": "add_cookie", "detail": str(e)}),
                  file=sys.stderr)

    # Active-cooldown guard: twscrape BLOCKS on get_for_queue_or_wait when X
    # rate-limited us (10-20 min), which would burn the whole sidecar budget.
    # Read the persisted lock table and skip the cycle in <1s instead — the
    # 20m scheduler retries at the next slot, and the other collectors never
    # wait behind a throttled X.
    throttled = _cooldown_state(db)
    if throttled is not None:
        print(json.dumps({"warn": "throttled_skip",
                          "next_available": throttled}), file=sys.stderr)
        return []

    try:
        infos = await api.pool.accounts_info()
    except Exception:  # noqa: BLE001
        infos = []
    active = sum(1 for a in infos if a.get("active") is True)
    if active == 0:
        print(json.dumps({"error": "no_active_accounts",
                          "hint": "set X_AUTH_TOKEN + X_CT0 env"}),
              file=sys.stderr)
        return []

    ok, detail = await _session_ok(api)
    if not ok:
        print(json.dumps({"error": "x_session_expired", "detail": detail,
                          "hint": "refresh X_AUTH_TOKEN + X_CT0 from the "
                                  "browser (x.com cookies)"}), file=sys.stderr)
        return []

    out = []
    tasks = []
    for handle in accounts:
        tasks.append(asyncio.create_task(_account_tweets(api, handle, limit, out)))
    for q in queries:
        tasks.append(asyncio.create_task(_query_tweets(api, q, limit, out)))
    for lid in lists:
        tasks.append(asyncio.create_task(_list_tweets(api, lid, limit, out)))
    if tasks:
        await asyncio.gather(*tasks)

    # dedupe by tweet id
    uniq = []
    seen = set()
    for it in out:
        if it["source_id"] in seen:
            continue
        seen.add(it["source_id"])
        uniq.append(it)
    return uniq


def _cooldown_state(db):
    """Return the nearest future lock timestamp, or None when no queue is
    throttled. Mirrors twscrape's `accounts.locks` JSON (queue -> ISO time)."""
    try:
        import sqlite3
        from datetime import datetime
        con = sqlite3.connect(db)
        rows = con.execute("SELECT locks FROM accounts").fetchall()
        con.close()
    except Exception:  # noqa: BLE001
        return None
    now = datetime.now()
    nearest = None
    for (locks_json,) in rows:
        try:
            locks = json.loads(locks_json or "{}")
        except ValueError:  # noqa: BLE001
            continue
        for ts in locks.values():
            try:
                when = datetime.fromisoformat(ts)
            except ValueError:  # noqa: BLE001
                continue
            if when > now and (nearest is None or when < nearest):
                nearest = when
    return nearest.isoformat() if nearest else None


def _item(t, handle):
    text = t.rawContent or t.text or ""
    author = handle or (t.user.username if t.user else "unknown")
    return {
        "source": "x",
        "source_id": "x:" + str(t.id),
        "author": author,
        "title": (text.split("\n", 1)[0])[:160],
        "url": f"https://twitter.com/{author}/status/{t.id}",
        "content": text,
        "published_at": t.date.isoformat() if t.date else None,
        "collected_at": None,
        "topics": ["software-engineering", "open-source"],
        "language": t.lang or "en",
        "metadata": {"mode": "twscrape", "likes": t.likeCount,
                     "retweets": t.retweetCount},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", nargs="*", default=[])
    ap.add_argument("--queries", nargs="*", default=[])
    ap.add_argument("--lists", nargs="*", default=[])
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    items = asyncio.run(collect(args.accounts, args.queries, args.lists, args.limit))
    print(json.dumps(items, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()