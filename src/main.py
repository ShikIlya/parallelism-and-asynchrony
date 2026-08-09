from crawler import AsyncCrawler

import time
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

URLS = [
    "https://httpbingo.org/delay/2?request=1",
    "https://httpbingo.org/delay/2?request=2",
    "https://httpbingo.org/delay/2?request=3",
    "https://httpbingo.org/delay/2?request=4",
    "https://httpbingo.org/delay/2?request=5",
    "https://httpbingo.org/delay/2?request=6",
    "https://httpbingo.org/delay/2?request=7",
    "https://httpbingo.org/status/404",
    "https://httpbingo.org/status/500",
    "https://this-domain-does-not-exist-12345.com",
]

async def load_sequentially(
    crawler: AsyncCrawler,
    urls: list[str],
) -> dict[str, str]:
    results = {}

    for url in urls:
        results[url] = await crawler.fetch_url(url)

    return results

def print_results(
    title: str,
    results: dict[str, str],
) -> None:
    print(f"\n{title}")

    for url, content in results.items():
        if content:
            print(f"[OK] {url}: {len(content)} символов")
        else:
            print(f"[ERROR] {url}")

async def measure_parallel() -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=10)

    try:
        start = time.perf_counter()

        results = await crawler.fetch_urls(URLS)

        elapsed = time.perf_counter() - start

        return results, elapsed

    finally:
        await crawler.close()

async def measure_sequential() -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=1)

    try:
        start = time.perf_counter()

        results = await load_sequentially(crawler, URLS)

        elapsed = time.perf_counter() - start

        return results, elapsed

    finally:
        await crawler.close()

async def main() -> None:
    print("Проверка внешних URL с задержкой 2 секунды")
    print(f"Количество URL: {len(URLS)}")

    parallel_results, parallel_time = await measure_parallel()
    print_results("Параллельная загрузка:", parallel_results)

    sequential_results, sequential_time = await measure_sequential()
    print_results("Последовательная загрузка:", sequential_results)

    print("\nРезультаты сравнения:")
    print(f"Параллельно:     {parallel_time:.2f} секунд")
    print(f"Последовательно: {sequential_time:.2f} секунд")

    if parallel_time < sequential_time:
        speedup = sequential_time / parallel_time

        print(f"Ускорение: примерно в {speedup:.2f} раза")
        print("Параллельная загрузка быстрее")
    else:
        print("Параллельная загрузка не оказалась быстрее")

if __name__ == "__main__":
    asyncio.run(main())