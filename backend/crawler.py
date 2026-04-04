"""
High-speed async website link crawler
Uses aiohttp for concurrent requests + SQLite for streaming storage
"""

import asyncio
import aiohttp
import sqlite3
import uuid
import time
import random
import re
from urllib.parse import urljoin, urlparse, urldefrag
from bs4 import BeautifulSoup
from datetime import datetime

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".tar",
    ".gz", ".mp3", ".mp4", ".avi", ".mov", ".css", ".js",
    ".woff", ".woff2", ".ttf", ".eot", ".map", ".xml",
}

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_page TEXT,
            link_url TEXT,
            link_text TEXT,
            link_type TEXT,
            status_code TEXT,
            final_url TEXT,
            crawled_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_link_url ON links(link_url)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_link_type ON links(link_type)")
    conn.commit()
    return conn


def save_link(conn, from_page, link_url, link_text, link_type, status_code, final_url):
    conn.execute(
        "INSERT INTO links (from_page, link_url, link_text, link_type, status_code, final_url, crawled_at) VALUES (?,?,?,?,?,?,?)",
        (from_page, link_url, link_text[:500], link_type, str(status_code), final_url or "", datetime.now().isoformat())
    )
    conn.commit()


def set_meta(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO job_meta (key, value) VALUES (?,?)", (key, str(value)))
    conn.commit()


def get_meta(conn, key):
    row = conn.execute("SELECT value FROM job_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


# ── URL helpers ───────────────────────────────────────────────────────────────

def normalize_url(url):
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return parsed._replace(path=path, query=parsed.query).geturl()


def is_crawlable(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    path = parsed.path.lower()
    return not any(path.endswith(ext) for ext in SKIP_EXTENSIONS)


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }


# ── Async Crawler ─────────────────────────────────────────────────────────────

class AsyncCrawler:
    def __init__(self, start_url, db_path, max_pages=100000,
                 concurrency=20, delay=0.0, own_site=True):
        self.start_url = start_url
        self.db_path = db_path
        self.max_pages = max_pages
        self.concurrency = concurrency
        self.delay = delay
        self.own_site = own_site

        parsed = urlparse(start_url)
        self.base_domain = parsed.netloc
        self.base_path = parsed.path.rstrip("/")

        self.visited = set()
        self.queued = set()
        self.checked_links = {}
        self.queue = asyncio.Queue()
        self.pages_crawled = 0
        self.links_found = 0
        self.running = True
        self.conn = None

    async def fetch(self, session, url, method="GET"):
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            if method == "HEAD":
                async with session.head(url, headers=get_headers(),
                                        timeout=timeout, allow_redirects=True) as r:
                    if r.status == 405:
                        return await self.fetch(session, url, "GET")
                    return r.status, str(r.url), None
            else:
                async with session.get(url, headers=get_headers(),
                                       timeout=timeout, allow_redirects=True) as r:
                    content_type = r.headers.get("Content-Type", "")
                    if "text/html" not in content_type:
                        return r.status, str(r.url), None
                    html = await r.text(errors="replace")
                    return r.status, str(r.url), html
        except aiohttp.ClientSSLError:
            return "SSL Error", url, None
        except aiohttp.ClientConnectorError:
            return "Connection Error", url, None
        except asyncio.TimeoutError:
            return "Timeout", url, None
        except Exception as e:
            return f"Error", url, None

    async def process_page(self, session, url):
        if url in self.visited:
            return
        self.visited.add(url)

        status, final_url, html = await self.fetch(session, url, "GET")
        self.pages_crawled += 1

        set_meta(self.conn, "pages_crawled", self.pages_crawled)
        set_meta(self.conn, "links_found", self.links_found)
        set_meta(self.conn, "current_url", url)

        if not html:
            return

        soup = BeautifulSoup(html, "lxml")

        for tag in soup.find_all("a", href=True):
            raw_href = tag["href"].strip()
            if not raw_href or raw_href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue

            abs_url = urljoin(final_url, raw_href)
            abs_url_norm = normalize_url(abs_url)
            link_parsed = urlparse(abs_url_norm)
            link_text = tag.get_text(strip=True)[:300]

            is_internal = (link_parsed.netloc == self.base_domain or link_parsed.netloc == "")
            link_type = "internal" if is_internal else "external"

            # Check status (cached)
            if abs_url_norm not in self.checked_links:
                if is_internal:
                    # We'll get the status when we crawl it, use HEAD for now
                    lk_status, lk_final, _ = await self.fetch(session, abs_url_norm, "HEAD")
                else:
                    lk_status, lk_final, _ = await self.fetch(session, abs_url_norm, "HEAD")
                self.checked_links[abs_url_norm] = (lk_status, lk_final)
            else:
                lk_status, lk_final = self.checked_links[abs_url_norm]

            save_link(self.conn, url, abs_url_norm, link_text, link_type,
                      lk_status, lk_final if lk_final != abs_url_norm else "")
            self.links_found += 1

            # Queue internal crawlable pages
            if (is_internal
                    and is_crawlable(abs_url_norm)
                    and abs_url_norm not in self.visited
                    and abs_url_norm not in self.queued
                    and self.pages_crawled < self.max_pages):
                self.queued.add(abs_url_norm)
                await self.queue.put(abs_url_norm)

        if self.delay > 0:
            await asyncio.sleep(self.delay)

    async def worker(self, session):
        while self.running:
            try:
                url = await asyncio.wait_for(self.queue.get(), timeout=5.0)
                if self.pages_crawled >= self.max_pages:
                    self.running = False
                    break
                await self.process_page(session, url)
                self.queue.task_done()
            except asyncio.TimeoutError:
                if self.queue.empty():
                    break
            except Exception:
                break

    async def run(self):
        self.conn = init_db(self.db_path)
        set_meta(self.conn, "status", "running")
        set_meta(self.conn, "start_url", self.start_url)
        set_meta(self.conn, "started_at", datetime.now().isoformat())
        set_meta(self.conn, "pages_crawled", 0)
        set_meta(self.conn, "links_found", 0)

        start_norm = normalize_url(self.start_url)
        self.queued.add(start_norm)
        await self.queue.put(start_norm)

        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            ssl=False,
            enable_cleanup_closed=True
        )
        async with aiohttp.ClientSession(connector=connector) as session:
            workers = [asyncio.create_task(self.worker(session))
                       for _ in range(self.concurrency)]
            await asyncio.gather(*workers, return_exceptions=True)

        set_meta(self.conn, "status", "done")
        set_meta(self.conn, "finished_at", datetime.now().isoformat())
        set_meta(self.conn, "pages_crawled", self.pages_crawled)
        set_meta(self.conn, "links_found", self.links_found)
        self.conn.close()


def start_crawl(job_id, start_url, max_pages, concurrency, delay, db_dir="jobs"):
    import os
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, f"{job_id}.db")
    crawler = AsyncCrawler(
        start_url=start_url,
        db_path=db_path,
        max_pages=max_pages,
        concurrency=concurrency,
        delay=delay,
    )
    asyncio.run(crawler.run())
    return db_path
