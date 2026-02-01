import os
import re
import html
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import requests

SUBREDDIT = "wallstreetbets"
SEARCH_URL = f"https://www.reddit.com/r/{SUBREDDIT}/search.json"
USER_AGENT = "earnings-calendar-scraper/1.0 (github-actions; contact: you)"

# Your Discord incoming webhook (required)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Optional: avoid reposting the same Reddit post
STATE_FILE = Path("last_earnings_post.txt")

# Search query aimed at WSB weekly earnings thread
# You can tune these if WSB changes titles/flairs.
QUERY = 'flair_name:"Earnings Thread" OR "Weekly Earnings Thread" OR "Weekly Earnings"'
LIMIT = 15  # how many recent posts to scan


def http_get_json(url: str, params: dict, timeout: int = 30) -> dict:
    r = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
    )
    r.raise_for_status()
    return r.json()


def choose_latest_weekly_earnings_post(data: dict) -> Optional[dict]:
    """
    Picks the most likely "weekly earnings thread" from reddit search JSON results.
    """
    children = (data.get("data") or {}).get("children") or []
    posts = [c.get("data") for c in children if isinstance(c, dict) and c.get("data")]

    # Heuristics: title contains "Weekly Earnings" and/or flair contains "Earnings"
    def score(p: dict) -> int:
        title = (p.get("title") or "").lower()
        flair = (p.get("link_flair_text") or "").lower()
        s = 0
        if "weekly" in title:
            s += 2
        if "earnings" in title:
            s += 3
        if "thread" in title:
            s += 1
        if "earnings" in flair:
            s += 2
        if p.get("is_self"):
            # self-posts often contain images in body; still possible
            s += 0
        return s

    posts.sort(key=lambda p: (score(p), p.get("created_utc", 0)), reverse=True)

    # Return first plausible post
    for p in posts:
        if score(p) >= 4:
            return p
    # fallback: newest post
    return posts[0] if posts else None


def extract_best_image_url(post: dict) -> Optional[str]:
    """
    Tries multiple patterns:
    - preview.images[0].source.url
    - gallery media_metadata (largest)
    - crosspost_parent_list recursion
    """
    # Crosspost handling
    cross = post.get("crosspost_parent_list")
    if isinstance(cross, list) and cross:
        url = extract_best_image_url(cross[0])
        if url:
            return url

    # Gallery handling
    media_metadata = post.get("media_metadata")
    if isinstance(media_metadata, dict) and media_metadata:
        best = None
        best_area = -1
        for _, meta in media_metadata.items():
            if not isinstance(meta, dict):
                continue
            s = meta.get("s") or {}
            u = s.get("u")
            x = s.get("x")
            y = s.get("y")
            if u and x and y:
                area = int(x) * int(y)
                if area > best_area:
                    best_area = area
                    best = u
        if best:
            return html.unescape(best)

    # Preview image handling (most common)
    preview = post.get("preview") or {}
    images = preview.get("images") or []
    if images and isinstance(images, list):
        src = (images[0].get("source") or {}).get("url")
        if src:
            return html.unescape(src)

    # Direct URL (sometimes i.redd.it)
    url = post.get("url_overridden_by_dest") or post.get("url")
    if isinstance(url, str) and re.search(r"(i\.redd\.it|i\.imgur\.com|\.png$|\.jpg$|\.jpeg$|\.webp$)", url):
        return html.unescape(url)

    return None


def download_image(url: str, out_path: Path) -> None:
    r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    out_path.write_bytes(r.content)


def post_to_discord(webhook_url: str, image_path: Path, message: str) -> None:
    with image_path.open("rb") as f:
        files = {"file": (image_path.name, f, "image/png")}
        data = {"content": message}
        r = requests.post(webhook_url, data=data, files=files, timeout=60)
        r.raise_for_status()


def read_last_post_id() -> Optional[str]:
    if STATE_FILE.exists():
        txt = STATE_FILE.read_text(encoding="utf-8").strip()
        return txt or None
    return None


def write_last_post_id(post_id: str) -> None:
    STATE_FILE.write_text(post_id, encoding="utf-8")


def main():
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL env var is missing.")

    params = {
        "q": QUERY,
        "restrict_sr": 1,
        "sort": "new",
        "limit": LIMIT,
        "include_over_18": "on",
    }

    # Small retry loop for transient reddit hiccups
    last_err = None
    data = None
    for attempt in range(3):
        try:
            data = http_get_json(SEARCH_URL, params=params)
            break
        except Exception as e:
            last_err = e
            time.sleep(2 + attempt)
    if data is None:
        raise RuntimeError(f"Failed to fetch Reddit search JSON: {last_err}")

    post = choose_latest_weekly_earnings_post(data)
    if not post:
        raise RuntimeError("No posts found in Reddit search.")

    post_id = post.get("id")
    title = post.get("title") or "(no title)"
    created_utc = post.get("created_utc") or 0
    post_url = "https://reddit.com" + (post.get("permalink") or "")

    # Duplicate protection
    last_id = read_last_post_id()
    if last_id and post_id and post_id == last_id:
        print(f"Latest post already posted (id={post_id}). Exiting.")
        return

    img_url = extract_best_image_url(post)
    if not img_url:
        # Pure scraper only: if no image, still post a link so it’s not silent
        msg = f"WSB Weekly Earnings Thread found but no image detected.\n**{title}**\n{post_url}"
        r = requests.post(DISCORD_WEBHOOK_URL, data={"content": msg}, timeout=30)
        r.raise_for_status()
        if post_id:
            write_last_post_id(post_id)
        return

    tmp = Path("wsb_weekly_earnings.png")
    download_image(img_url, tmp)

    # Message includes title + link for provenance
    dt = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"**{title}**\nPosted: {dt}\n{post_url}"

    post_to_discord(DISCORD_WEBHOOK_URL, tmp, msg)

    if post_id:
        write_last_post_id(post_id)

    print(f"Posted WSB image from {post_url}")


if __name__ == "__main__":
    main()