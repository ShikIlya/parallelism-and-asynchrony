from collections import Counter
from urllib.parse import urlparse

class CrawlerStats:
    def __init__(self, crawler) -> None:
        self.crawler = crawler

    def get_stats(self) -> dict:
        successful = len(self.crawler.processed_urls)
        failed = len(self.crawler.failed_urls)
        total_pages = successful + failed

        elapsed_seconds = self.crawler.get_elapsed_time()

        average_pages_per_second = (
            total_pages / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0
        )

        domains = Counter(
            urlparse(url).netloc
            for url in self.crawler.processed_urls
        )

        retry_statistics = self.crawler.get_error_statistics()

        return {
            "total_pages": total_pages,
            "successful": successful,
            "failed": failed,
            "total_requests": self.crawler._total_requests,
            "status_codes": dict(self.crawler.status_codes),
            "average_pages_per_second": average_pages_per_second,
            "elapsed_seconds": elapsed_seconds,
            "current_rps": self.crawler.get_current_rps(),
            "active_tasks": self.crawler.semaphore_manager.active_tasks,
            "top_domains": dict(domains.most_common(10)),
            "errors_by_type": retry_statistics["errors_by_type"],
            "successful_retries": retry_statistics["successful_retries"],
            "average_retry_delay": retry_statistics["average_retry_delay"],
        }