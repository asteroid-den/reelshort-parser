import asyncio
import csv
import io
import json
import logging
import re
import time

import aiohttp
import anyio

from config import settings

logger = logging.getLogger(__name__)

CSV_FIELDS = [
    "series_title",
    "series_url",
    "cover_image_url",
    "description",
    "genre_category",
    "num_episodes",
    "status",
    "tags",
    "ranking",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.reelshort.com/",
}

TAG_CATEGORIES = ["/tags/movie-actors", "/tags/movie-actresses", "/tags/movie-identities", "/tags/story-beats"]

CATEGORIES_IDS = ("1000", "1011", "1012", "1013", "1014", "1015", "1020", "1022", "1023", "1024")


class ProxyManager:
    def __init__(self, proxies: list[str], rotate_every: int = 50) -> None:
        """
        Initialize proxy manager.

        Args:
            proxies: List of proxy URLs (empty list for direct connection)
            rotate_every: Number of requests before rotating proxy
        """
        self.proxies = proxies
        self.rotate_every = rotate_every
        self.current_idx = 0
        self.requests_on_proxy = 0

    @property
    def current(self) -> str | None:
        if not self.proxies:
            return None
        return self.proxies[self.current_idx]

    def rotate(self) -> None:
        if len(self.proxies) <= 1:
            self.requests_on_proxy = 0
            return
        self.current_idx = (self.current_idx + 1) % len(self.proxies)
        self.requests_on_proxy = 0

    def next_if_needed(self) -> None:
        self.requests_on_proxy += 1
        if self.requests_on_proxy >= self.rotate_every:
            self.rotate()


def extract_next_data(html: str) -> dict | None:
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def prepare_slug(title: str) -> str:
    slug = title.lower()

    for ch in "'!?,.;—–()\"'":
        slug = slug.replace(ch, "")

    return re.sub(r"[^a-z0-9]+", "-", slug).strip("-")


def is_waf_blocked(html: str) -> bool:
    checks = [
        "awsWaf" in html,
        "AWSALB" in html,
        "challenge" in html.lower(),
        "Checking your browser" in html,
        "Just a moment" in html,
        "cf-browser-verification" in html,
        "<title>Attention Required" in html,
    ]
    return any(checks)


def parse_detail_meta(nd: dict) -> dict:
    """Extract only the metadata fields needed for CSV output."""
    pp = nd["props"]["pageProps"]
    data = pp.get("data", {})
    meta = {}

    # Extract required scalar fields with field name mapping
    for src_field, dst_field in [
        ("book_title", "series_title"),
        ("book_pic", "series_pic"),
        ("special_desc", "special_desc"),
        ("chapter_count", "chapter_count"),
        ("update_status", "update_status"),
    ]:
        if val := data.get(src_field):
            meta[dst_field] = val

    # Extract and organize tag categories
    tag_list = data.get("tag_list", [])
    tag_cats = {}
    for tag in tag_list:
        if cid := tag.get("category_id"):
            tag_cats.setdefault(cid, []).append(tag.get("text", ""))

    # Convert lists to semicolon-separated strings
    for cid, value in tag_cats.items():
        tag_cats[cid] = "; ".join(value)

    # Build genre_category from categories 1010 and 1010001
    genre_parts = []
    if v := tag_cats.get("1010"):
        genre_parts.append(v)
    if v := tag_cats.get("1010001"):
        genre_parts.append(v)
    meta["genre_category"] = "; ".join(genre_parts)

    # Build tags from all other relevant categories (excluding 1010 and 1010001)
    tags_parts = [tag_cats[cid] for cid in CATEGORIES_IDS if cid in tag_cats]

    meta["tags"] = "; ".join(tags_parts)

    return meta


def make_row(s: dict, meta: dict | None) -> dict:
    """Build a CSV row from series data and metadata."""
    # Merge series data with metadata (metadata takes precedence)
    data = {**s, **(meta or {})}

    title = data["series_title"]
    series_id = data["series_id"]
    slug = prepare_slug(title)
    series_url = f"{settings.base_url}/movie/{slug}-{series_id}"
    cover = data.get("series_pic", "")
    desc = data.get("special_desc", "")
    genre = data.get("genre_category", "")
    eps = data.get("chapter_count", "")
    tags = data.get("tags", "")

    # Status mapping
    status = ""
    if status_val := data.get("update_status"):
        if status_val == 1:
            status = "completed"
        elif status_val == 0:
            status = "ongoing"
        elif status_val:
            status = str(status_val)

    # Ranking
    read_count = data.get("read_count", "")
    collect_count = data.get("collect_count", "")
    ranking = f"reads={read_count}; collects={collect_count}" if read_count else ""

    return {
        "series_title": title,
        "series_url": series_url,
        "cover_image_url": cover,
        "description": desc,
        "genre_category": genre,
        "num_episodes": str(eps) if eps else "",
        "status": status,
        "tags": tags,
        "ranking": ranking,
    }


async def fetch_page(
    url: str,
    session: aiohttp.ClientSession,
    proxy_manager: ProxyManager,
    semaphore: asyncio.Semaphore,
) -> tuple[list[dict], str | None]:
    """Fetch a single tag page and extract series list."""
    async with semaphore:
        proxy_url = proxy_manager.current
        for _ in range(3):
            try:
                async with session.get(
                    url,
                    headers=HEADERS,
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        break
                    if resp.status in (403, 429, 503):
                        proxy_manager.rotate()
                        proxy_url = proxy_manager.current
                        continue
                    proxy_manager.rotate()
                    proxy_url = proxy_manager.current
                    continue
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                proxy_manager.rotate()
                proxy_url = proxy_manager.current
                await asyncio.sleep(0.5)
                continue
        else:
            logger.warning("Failed to fetch %s after 3 attempts", url)
            return [], None

    nd = extract_next_data(html)
    if not nd:
        logger.warning("Could not extract __NEXT_DATA__ from %s", url)
        return [], None
    try:
        pp = nd["props"]["pageProps"]
    except KeyError:
        logger.warning("Missing pageProps in __NEXT_DATA__ for %s", url)
        return [], None

    tb = pp.get("tagBooks", {})
    series_list = tb.get("books", [])
    next_link = pp.get("nextPageLink") or None
    return series_list, next_link


async def collect_ids(
    session: aiohttp.ClientSession,
    proxy_manager: ProxyManager,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Collect all series IDs from tag pages via proxy."""
    seen_ids = set()
    all_series = []

    for cat_path in TAG_CATEGORIES:
        page_url = f"{settings.base_url}{cat_path}"
        page_num = 0
        while page_url:
            page_num += 1
            series_list, next_link = await fetch_page(page_url, session, proxy_manager, semaphore)
            logger.info("Category %s, page %d: got %d series", cat_path, page_num, len(series_list))
            for s in series_list:
                sid = s["book_id"]
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    all_series.append(
                        {
                            "series_id": sid,
                            "series_title": s.get("book_title", "Unknown"),
                            "series_pic": s.get("book_pic", ""),
                            "chapter_count": s.get("chapter_count", ""),
                            "special_desc": s.get("special_desc", ""),
                            "read_count": s.get("read_count", ""),
                            "collect_count": s.get("collect_count", ""),
                        },
                    )
            proxy_manager.next_if_needed()
            if next_link and "javascript" not in next_link:
                page_url = f"{settings.base_url}{next_link}" if next_link.startswith("/") else next_link
            else:
                break

    return all_series


async def fetch_one(  # noqa: C901
    s: dict,
    session: aiohttp.ClientSession,
    proxy_manager: ProxyManager,
    semaphore: asyncio.Semaphore,
) -> tuple[dict, dict | None]:
    """Fetch detailed metadata for a single series."""
    async with semaphore:
        title = s["series_title"]
        sid = s["series_id"]
        slug = prepare_slug(title)
        url = f"{settings.base_url}/movie/{slug}-{sid}"
        proxy_url = proxy_manager.current

        for _ in range(3):
            try:
                resp = await session.get(
                    url,
                    headers=HEADERS,
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=30),
                )

                # Detect redirects — follow canonical URL
                final_url = str(resp.url)
                if final_url != url:
                    match = re.search(r"-([a-f0-9]{24})$", final_url)
                    if match:
                        canonical_id = match.group(1)
                        if canonical_id != s["series_id"]:
                            logger.info("Redirect: %s → %s (ID: %s)", url, final_url, canonical_id)
                            s["series_id"] = canonical_id

                if resp.status in (403, 429, 503):
                    html = await resp.text()
                    if is_waf_blocked(html) or resp.status in (429, 503):
                        proxy_manager.rotate()
                        continue

                if resp.status != 200:
                    proxy_manager.rotate()
                    continue

                html = await resp.text()
                if is_waf_blocked(html):
                    proxy_manager.rotate()
                    continue

                next_data = extract_next_data(html)
                if not next_data:
                    proxy_manager.rotate()
                    continue

                meta = parse_detail_meta(next_data)
                return s, meta  # noqa: TRY300

            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                proxy_manager.rotate()
                await asyncio.sleep(0.5)
                continue

        return s, None


async def fetch_details(series: list[dict], proxy_manager: ProxyManager, semaphore: asyncio.Semaphore, start_idx: int):
    """Fetch detailed metadata from /movie/ pages via proxy."""
    logger.info("Starting to fetch details for %d series", len(series))
    connector = aiohttp.TCPConnector(
        limit=settings.concurrency,
        limit_per_host=2,
        force_close=True,
        ttl_dns_cache=300,
    )
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_one(s, session, proxy_manager, semaphore) for s in series]
        done_count = start_idx
        ok_count = 0
        fail_count = 0

        for coro in asyncio.as_completed(tasks):
            s, meta = await coro
            done_count += 1

            if meta:
                ok_count += 1
                results.append((s, meta))
            else:
                fail_count += 1
                results.append((s, None))

            # Log progress every 10 series
            if done_count % 10 == 0:
                logger.info(
                    "Fetched %d/%d series (OK: %d, Fail: %d)",
                    done_count,
                    start_idx + len(tasks),
                    ok_count,
                    fail_count,
                )

        return results, ok_count, fail_count


async def main():  # noqa: C901, PLR0915
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("ReelShort Scraper started")

    t0 = time.time()

    proxies = []

    proxies_file_path = anyio.Path(settings.proxies_file)
    csv_file_path = anyio.Path(settings.csv_path)

    if await proxies_file_path.exists():
        async with await proxies_file_path.open() as file:
            content = await file.read()
            proxies.extend(line.strip() for line in content.splitlines() if line.strip())

    if proxies:
        logger.info("Loaded %d proxies", len(proxies))
    else:
        logger.warning("No proxies found. Using direct connection.")

    proxy_manager = ProxyManager(proxies, rotate_every=settings.proxy_rotate_every)
    connector = aiohttp.TCPConnector(limit=20)
    semaphore = asyncio.Semaphore(settings.concurrency)

    logger.info("Collecting series IDs from tag pages")

    async with aiohttp.ClientSession(connector=connector) as session:
        all_series = await collect_ids(session=session, proxy_manager=proxy_manager, semaphore=semaphore)

    logger.info("Collected %d unique series", len(all_series))
    # Load already processed IDs from CSV
    processed_ids = set()

    if await csv_file_path.exists():
        async with await csv_file_path.open() as file:
            reader = csv.DictReader(await file.readlines())
            for row in reader:
                url = row.get("series_url", "")
                match = re.search(r"-([a-f0-9]{24})$", url)
                if match:
                    processed_ids.add(match.group(1))
        logger.info("Found %d already processed series", len(processed_ids))

    # Filter out already processed series
    series_to_fetch = [s for s in all_series if s.get("series_id") not in processed_ids]
    logger.info("Series to fetch: %d", len(series_to_fetch))

    if not series_to_fetch:
        elapsed = time.time() - t0
        logger.info("All series already processed (elapsed: %.0fs)", elapsed)
        return

    logger.info("Fetching detailed metadata for %d series", len(series_to_fetch))
    results, ok_count, fail_count = await fetch_details(
        series_to_fetch,
        proxy_manager,
        semaphore,
        start_idx=len(processed_ids),
    )

    elapsed = time.time() - t0

    # Combine with existing results
    existing_rows = {}

    if await csv_file_path.exists():
        async with await csv_file_path.open() as file:
            reader = csv.DictReader(await file.readlines())
            for row in reader:
                url = row.get("series_url", "")
                match = re.search(r"-([a-f0-9]{24})$", url)
                if match:
                    existing_rows[match.group(1)] = row

    # Add new results
    for s, meta in results:
        row = make_row(s, meta)
        existing_rows[s.get("series_id", "")] = row

    rows = list(existing_rows.values())
    logger.info("Total rows: %d", len(rows))

    # Write CSV
    await csv_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV to memory first (csv module is sync), then async write to file
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS)
    writer.writeheader()
    writer.writerows(rows)

    await csv_file_path.write_text(buffer.getvalue(), encoding="utf-8")

    # Log final summary
    logger.info("Scraping complete: %d/%d successful, %d failed", ok_count, len(rows), fail_count)
    logger.info("Time elapsed: %.0fs (%.1f min)", elapsed, elapsed / 60)
    if elapsed > 60:
        logger.info("Rate: %.0f series/min", ok_count / (elapsed / 60))
    logger.info("Results written to %s", settings.csv_path)


if __name__ == "__main__":
    asyncio.run(main())
