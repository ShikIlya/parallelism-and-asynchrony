import asyncio
import json
import sys
import time
import tracemalloc
from pathlib import Path
from urllib.request import urlopen

sys.path.append(
    str(Path(__file__).parent)
)

from crawler import AsyncCrawler, AdvancedCrawler

OUTPUT_DIR = Path("output")

TEST_URL = "https://httpbingo.org/delay/1"

SYNC_COMPARISON_REQUESTS = 10

SCALE_LIMITS = [
    100,
    500,
    1000,
]

MAX_CONCURRENT = 10
REQUESTS_PER_SECOND = 10.0


def fetch_sync(url: str) -> bool:
    try:
        with urlopen(
            url,
            timeout=20,
        ) as response:
            response.read()

        return True

    except Exception:
        return False


def run_sync_test(
    urls: list[str],
) -> dict:
    tracemalloc.start()

    started_at = time.perf_counter()

    successful = sum(
        fetch_sync(url)
        for url in urls
    )

    elapsed = time.perf_counter() - started_at

    _, peak_memory = tracemalloc.get_traced_memory()

    tracemalloc.stop()

    return {
        "mode": "synchronous",
        "requests": len(urls),
        "successful": successful,
        "failed": len(urls) - successful,
        "elapsed_seconds": round(elapsed, 3),
        "requests_per_second": round(
            len(urls) / elapsed,
            3,
        ) if elapsed else 0.0,
        "peak_memory_mb": round(
            peak_memory / 1024 / 1024,
            3,
        ),
    }


async def run_async_loader_test(
    urls: list[str],
) -> dict:
    tracemalloc.start()

    crawler = AsyncCrawler(
        max_concurrent=MAX_CONCURRENT,
        max_per_domain=MAX_CONCURRENT,
        max_depth=0,
        requests_per_second=REQUESTS_PER_SECOND,
        rate_limit_per_domain=False,
        respect_robots=False,
    )

    started_at = time.perf_counter()

    try:
        results = await crawler.fetch_urls(urls)
    finally:
        await crawler.close()

    elapsed = time.perf_counter() - started_at

    _, peak_memory = tracemalloc.get_traced_memory()

    tracemalloc.stop()

    successful = sum(
        1
        for content in results.values()
        if content
    )

    return {
        "mode": "asynchronous_loader",
        "requests": len(urls),
        "successful": successful,
        "failed": len(urls) - successful,
        "max_concurrent": MAX_CONCURRENT,
        "elapsed_seconds": round(elapsed, 3),
        "requests_per_second": round(
            len(urls) / elapsed,
            3,
        ) if elapsed else 0.0,
        "peak_memory_mb": round(
            peak_memory / 1024 / 1024,
            3,
        ),
    }

async def run_advanced_crawler_test(
    max_pages: int,
) -> dict:
    tracemalloc.start()

    crawler = AdvancedCrawler.from_config(
        "config.json",
    )

    crawler.start_urls = [
        "https://httpbingo.org",
    ]
    crawler.sitemap_urls = []
    crawler.max_pages = max_pages
    crawler.max_depth = 1
    crawler.same_domain_only = True

    started_at = time.perf_counter()

    try:
        await crawler.crawl()
        stats = crawler.get_stats()
    finally:
        await crawler.close()

    elapsed = time.perf_counter() - started_at

    _, peak_memory = tracemalloc.get_traced_memory()

    tracemalloc.stop()

    processed = stats["total_pages"]

    return {
        "mode": "advanced_crawler",
        "requested_max_pages": max_pages,
        "processed_pages": processed,
        "successful": stats["successful"],
        "failed": stats["failed"],
        "elapsed_seconds": round(elapsed, 3),
        "pages_per_second": round(
            processed / elapsed,
            3,
        ) if elapsed else 0.0,
        "peak_memory_mb": round(
            peak_memory / 1024 / 1024,
            3,
        ),
    }


def print_result(result: dict) -> None:
    print("\n" + "-" * 70)

    for key, value in result.items():
        print(f"{key}: {value}")


async def main() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 70)
    print("ТЕСТ 1: СИНХРОННАЯ И АСИНХРОННАЯ ЗАГРУЗКА")
    print("=" * 70)

    comparison_urls = [
        f"{TEST_URL}?comparison={index}"
        for index in range(SYNC_COMPARISON_REQUESTS)
    ]

    sync_result = run_sync_test(
        comparison_urls,
    )

    async_result = await run_async_loader_test(
        comparison_urls,
    )

    speedup = (
        sync_result["elapsed_seconds"]
        / async_result["elapsed_seconds"]
        if async_result["elapsed_seconds"] > 0
        else 0.0
    )

    print_result(sync_result)
    print_result(async_result)

    print(
        "\nУскорение async относительно sync: "
        f"{speedup:.2f}x"
    )

    print("\n" + "=" * 70)
    print("ТЕСТ 2: МАСШТАБИРУЕМОСТЬ AdvancedCrawler")
    print("=" * 70)

    scale_results = []

    for max_pages in SCALE_LIMITS:
        print(
            f"\nЗапуск AdvancedCrawler "
            f"с max_pages={max_pages}"
        )

        result = await run_advanced_crawler_test(
            max_pages,
        )

        scale_results.append(result)
        print_result(result)

    report = {
        "comparison": {
            "synchronous": sync_result,
            "asynchronous_loader": async_result,
            "async_speedup": round(speedup, 3),
        },
        "advanced_crawler_scalability": scale_results,
        "configuration": {
            "test_url": TEST_URL,
            "sync_comparison_requests": (
                SYNC_COMPARISON_REQUESTS
            ),
            "max_concurrent": MAX_CONCURRENT,
            "requests_per_second": REQUESTS_PER_SECOND,
            "scale_limits": SCALE_LIMITS,
        },
    }

    report_path = (
        OUTPUT_DIR / "performance_report.json"
    )

    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n" + "=" * 70)
    print(
        "Отчёт сохранён: "
        f"{report_path}"
    )
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())