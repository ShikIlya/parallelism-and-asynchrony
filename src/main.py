from crawler import AsyncCrawler
from exceptions import TransientError, NetworkError

import time
import json
import asyncio
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# День 1: последовательная vs параллельная загрузка
# --------------------------------------------------------------------------

DAY1_URLS = [
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


async def measure_parallel(urls: list[str]) -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=10)

    try:
        start = time.perf_counter()

        results = await crawler.fetch_urls(urls)

        elapsed = time.perf_counter() - start

        return results, elapsed
    finally:
        await crawler.close()


async def measure_sequential(urls: list[str]) -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=1)

    try:
        start = time.perf_counter()

        results = await load_sequentially(crawler, urls)

        elapsed = time.perf_counter() - start

        return results, elapsed
    finally:
        await crawler.close()


async def demo_day1_loading() -> None:
    """Демонстрация дня 1: сравнение скорости последовательной и параллельной загрузки."""
    print("\n" + "#" * 70)
    print("# ДЕНЬ 1: Последовательная vs параллельная загрузка")
    print("#" * 70)
    print(f"Количество URL: {len(DAY1_URLS)}")

    parallel_results, parallel_time = await measure_parallel(DAY1_URLS)
    print_results("Параллельная загрузка:", parallel_results)

    sequential_results, sequential_time = await measure_sequential(DAY1_URLS)
    print_results("Последовательная загрузка:", sequential_results)

    print("\nРезультаты сравнения:")
    print(f"Параллельно: {parallel_time:.2f} секунд")
    print(f"Последовательно: {sequential_time:.2f} секунд")

    if parallel_time < sequential_time:
        speedup = sequential_time / parallel_time

        print(f"Ускорение: примерно в {speedup:.2f} раза")
        print("Параллельная загрузка быстрее")
    else:
        print("Параллельная загрузка не оказалась быстрее")


# --------------------------------------------------------------------------
# День 2: парсинг HTML (HTMLParser + AsyncCrawler.fetch_and_parse)
# --------------------------------------------------------------------------

DAY2_URLS = [
    "https://example.com",
    "https://httpbingo.org/html",
    "https://this-domain-does-not-exist-12345.com",
]


async def fetch_and_parse_all(
    crawler: AsyncCrawler,
    urls: list[str],
) -> list[dict]:
    tasks = [
        crawler.fetch_and_parse(url)
        for url in urls
    ]

    return await asyncio.gather(*tasks)


def summarize(result: dict) -> dict:
    return {
        "url": result["url"],
        "title": result["title"],
        "text_length": len(result["text"]),
        "links_count": len(result["links"]),
        "links": result["links"][:5],
        "images_count": len(result.get("images", [])),
        "headings_count": len(result.get("headings", [])),
        "tables_count": len(result.get("tables", [])),
        "lists_count": len(result.get("lists", [])),
        "error": result.get("error"),
    }


def print_page_report(result: dict) -> None:
    summary = summarize(result)

    print("\n" + "=" * 70)
    print(f"URL: {summary['url']}")
    print("=" * 70)

    if summary["error"]:
        print(f"[ОШИБКА] {summary['error']}")
        return

    print(f"Заголовок страницы : {summary['title'] or '(не найден)'}")
    print(f"Длина текста        : {summary['text_length']} символов")
    print(f"Ссылок найдено      : {summary['links_count']}")
    print(f"Изображений найдено : {summary['images_count']}")
    print(f"Заголовков (h1-h3)  : {summary['headings_count']}")
    print(f"Таблиц              : {summary['tables_count']}")
    print(f"Списков             : {summary['lists_count']}")

    if summary["links"]:
        print("\nПервые ссылки:")
        for link in summary["links"]:
            print(f"  - {link}")

    if result.get("headings"):
        print("\nЗаголовки страницы:")
        for heading in result["headings"][:5]:
            print(f"  [{heading['level']}] {heading['text']}")


def print_overall_statistics(results: list[dict]) -> None:
    total_pages = len(results)
    successful = [r for r in results if not r.get("error")]
    failed = [r for r in results if r.get("error")]

    total_links = sum(len(r["links"]) for r in successful)
    total_text_length = sum(len(r["text"]) for r in successful)
    total_images = sum(len(r.get("images", [])) for r in successful)

    print("\n" + "=" * 70)
    print("ОБЩАЯ СТАТИСТИКА")
    print("=" * 70)
    print(f"Всего страниц обработано : {total_pages}")
    print(f"Успешно                  : {len(successful)}")
    print(f"С ошибками               : {len(failed)}")
    print(f"Суммарно ссылок          : {total_links}")
    print(f"Суммарно изображений     : {total_images}")
    print(f"Суммарная длина текста   : {total_text_length} символов")

    if successful:
        avg_text_length = total_text_length / len(successful)
        print(f"Средняя длина текста     : {avg_text_length:.0f} символов/страница")

    if failed:
        print("\nСтраницы с ошибками:")
        for r in failed:
            print(f"  - {r['url']}: {r['error']}")


async def demo_day2_parsing() -> None:
    """Демонстрация дня 2: загрузка + парсинг HTML, статистика по страницам."""
    print("\n" + "#" * 70)
    print("# ДЕНЬ 2: Парсинг HTML (HTMLParser + fetch_and_parse)")
    print("#" * 70)
    print(f"Количество URL для обработки: {len(DAY2_URLS)}")

    crawler = AsyncCrawler(max_concurrent=5)

    try:
        results = await fetch_and_parse_all(crawler, DAY2_URLS)
    finally:
        await crawler.close()

    for result in results:
        print_page_report(result)

    print_overall_statistics(results)

# --------------------------------------------------------------------------
# День 3: управление конкурентностью и очередями
# --------------------------------------------------------------------------

DAY3_URLS = ["https://example.com"]

def save_crawl_results(
    results: list[dict],
    failed_urls: dict[str, str],
    filename: str = "day3_crawl_results.json",
) -> Path:
    data = {
        "processed_pages": len(results),
        "failed_pages": len(failed_urls),
        "results": results,
        "failed_urls": failed_urls,
    }

    output_path = Path(filename)

    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return output_path

async def demo_day3_crawling():
    print("\n" + "#" * 70)
    print("# ДЕНЬ 3: управление конкурентностью и очередями")
    print("#" * 70)

    crawler = AsyncCrawler(
        max_concurrent=10,
        max_depth=2,
    )

    try:
        results = await crawler.crawl(
            start_urls=DAY3_URLS,
            max_pages=50,
            same_domain_only=True,
        )

        output_path = save_crawl_results(
            results=results,
            failed_urls=crawler.failed_urls,
        )

        print(f"Данные сохранены: {output_path.resolve()}")
        print(f"Обработано: {len(results)} страниц")
        print(f"Посещено URL: {len(crawler.visited_urls)}")
        print(f"Ошибок: {len(crawler.failed_urls)}")

        if crawler.failed_urls:
            print("Неудачные URL:")
            for url, error in crawler.failed_urls.items():
                print(f" - {url}: {error}")

    finally:
        await crawler.close()


# --------------------------------------------------------------------------
# День 4: мониторинг скорости и прогресса
# --------------------------------------------------------------------------

DAY4_URLS = [
    "https://example.com",
    "https://httpbingo.org/html",
]

async def demo_day4_monitoring() -> None:
    print("\n" + "#" * 70)
    print("# ДЕНЬ 4: Мониторинг скорости и прогресса")
    print("#" * 70)

    crawler = AsyncCrawler(
        max_concurrent=3,
        max_depth=1,
        max_per_domain=2,
        requests_per_second=2.0,
        rate_limit_per_domain=True,
        respect_robots=False,
        user_agent="Day4Demo/1.0",
        min_delay=0.3,
        jitter=0.2,
        backoff_factor=1.0,
        max_retries=3,
    )

    print(f"Конфигурация:")
    print(f"  - max_concurrent: {crawler.max_concurrent}")
    print(f"  - max_per_domain: {crawler.semaphore_manager.max_per_domain}")
    print(f"  - requests_per_second: {crawler.rate_limiter.requests_per_second}")
    print(f"  - min_delay: {crawler.min_delay}s, jitter: {crawler.jitter}s")
    print(f"  - backoff_factor={crawler.backoff_factor}s, max_retries={crawler.max_retries}s")
    print()

    try:
        results = await crawler.crawl(
            start_urls=DAY4_URLS,
            max_pages=10,
            same_domain_only=False,
        )

        print("\n" + "=" * 70)
        print("ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 70)
        print(f"✅ Успешно обработано: {len(results)} страниц")
        print(f"❌ Ошибок: {len(crawler.failed_urls)}")
        print(f"🚫 Robots.txt: {len(crawler.blocked_urls)}")
        print(f"⚡ Текущий RPS: {crawler.get_current_rps():.2f} req/sec")
        print(f"⏱ Средняя задержка: {crawler.get_average_delay():.3f}s")
        print(f"⏰ Общее время: {crawler.get_elapsed_time():.2f}s")
        print(f"📊 Всего запросов: {crawler._total_requests}")

        if crawler.failed_urls:
            print("\nСтраницы с ошибками:")
            for url, error in crawler.failed_urls.items():
                print(f"  - {url}: {error}")

        if results:
            print("\nПример результата:")
            first = results[0]
            print(f"  URL: {first['url']}")
            print(f"  Заголовок: {first.get('title', 'N/A')}")
            print(f"  Ссылок: {len(first.get('links', []))}")
            print(f"  Изображений: {len(first.get('images', []))}")

    finally:
        await crawler.close()

# --------------------------------------------------------------------------
# День 5: retry, exponential backoff, timeout и статистика ошибок
# --------------------------------------------------------------------------

DAY5_URLS = [
    "https://httpbingo.org/status/200",
    "https://httpbingo.org/status/404",
    "https://httpbingo.org/status/429",
    "https://httpbingo.org/status/503",
    "https://this-domain-does-not-exist-12345.com",
]


def save_day5_report(
    report: dict,
    filename: str = "day5_retry_report.json",
) -> Path:
    output_path = Path(filename)

    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return output_path


async def demo_day5_retry_and_errors() -> None:
    print("\n" + "#" * 70)
    print("# ДЕНЬ 5: Retry, backoff, timeout и статистика ошибок")
    print("#" * 70)

    crawler = AsyncCrawler(
        max_concurrent=5,
        max_retries=3,
        backoff_factor=0.25,
        retry_on=[
            TransientError,
            NetworkError,
        ],
        retry_limits={
            TransientError: 3,
            NetworkError: 2,
        },
        backoff_factors={
            TransientError: 0.25,
            NetworkError: 0.5,
        },
        total_timeout=10.0,
        connect_timeout=5.0,
        read_timeout=5.0,
        timeout_backoff_factor=1.5,
        max_timeout=30.0,
    )

    print("Конфигурация retry:")
    print(f"  - Общий лимит повторов: {crawler.retry_strategy.max_retries}")
    print("  - Лимиты по типам: TransientError=3, NetworkError=2")
    print("  - Backoff: TransientError=0.25с, NetworkError=0.5с")
    print("  - Timeout: total=10с, connect=5с, read=5с")
    print("  - Рост timeout: x1.5, максимум=30с")

    print("\nПроверяем URL:")
    for url in DAY5_URLS:
        print(f"  - {url}")

    try:
        results = await crawler.fetch_urls(DAY5_URLS)
        statistics = crawler.get_error_statistics()

        print("\n" + "=" * 70)
        print("РЕЗУЛЬТАТЫ ЗАГРУЗКИ")
        print("=" * 70)

        for url, content in results.items():
            if content:
                print(f"[OK]    {url}: {len(content)} символов")
            else:
                print(f"[ERROR] {url}")

        print("\n" + "=" * 70)
        print("СТАТИСТИКА RETRY И ОШИБОК")
        print("=" * 70)

        print(f"Всего HTTP-попыток: {crawler._total_requests}")

        print(
            "Успешных операций после retry: "
            f"{statistics['successful_retries']}"
        )

        print(f"Задержки retry: {statistics['retry_delays']}")

        print(
            "Средняя задержка retry: "
            f"{statistics['average_retry_delay']:.2f} сек."
        )

        print("\nОшибки по типам:")

        if statistics["errors_by_type"]:
            for error_type, count in statistics["errors_by_type"].items():
                print(f"  - {error_type}: {count}")
        else:
            print("  - Ошибок нет")

        print("\nURL с постоянными ошибками:")

        if statistics["permanent_error_urls"]:
            for url in statistics["permanent_error_urls"]:
                print(f"  - {url}")
        else:
            print("  - Нет")

        print("\nНеудачные URL:")

        if statistics["failed_urls"]:
            for url, error in statistics["failed_urls"].items():
                print(f"  - {url}: {error}")
        else:
            print("  - Нет")

        report = {
            "day": 5,
            "topic": "retry, exponential backoff, timeout handling",
            "configuration": {
                "max_retries": crawler.retry_strategy.max_retries,
                "retry_limits": {
                    "TransientError": 3,
                    "NetworkError": 2,
                },
                "backoff_factors": {
                    "TransientError": 0.25,
                    "NetworkError": 0.5,
                },
                "total_timeout": crawler.total_timeout,
                "connect_timeout": crawler.connect_timeout,
                "read_timeout": crawler.read_timeout,
                "timeout_backoff_factor": crawler.timeout_backoff_factor,
                "max_timeout": crawler.max_timeout,
            },
            "results": {
                url: {
                    "success": bool(content),
                    "content_length": len(content),
                }
                for url, content in results.items()
            },
            "statistics": statistics,
        }

        output_path = save_day5_report(report)

        print(f"\nОтчёт сохранён: {output_path.resolve()}")

    finally:
        await crawler.close()

async def main() -> None:
    await demo_day1_loading()
    await demo_day2_parsing()
    await demo_day3_crawling()
    await demo_day4_monitoring()
    await demo_day5_retry_and_errors()

if __name__ == "__main__":
    asyncio.run(main())