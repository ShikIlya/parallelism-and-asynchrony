from __future__ import annotations
import time

class CrawlerQueue:
    def __init__(self):
        self.queue = []
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self._start_time = time.monotonic()


    def add_url(self, url: str, priority: int = 0):
        if url in self.visited_urls:
            return

        self.visited_urls.add(url)
        self.queue.append((url, priority))

    async def get_next(self) -> str | None:
        if len(self.queue) == 0:
            return None

        item = max(self.queue, key=lambda i: i[1])
        self.queue.remove(item)

        return item[0]

    def mark_processed(self, url: str, result: dict = None):
        self.processed_urls[url] = result

    def mark_failed(self, url: str, error: str):
        self.failed_urls[url] = error

    def get_stats(self) -> dict:
        elapsed = max(time.monotonic() - self._start_time, 1e-9)
        processed_count = len(self.processed_urls)

        return {
            'count_queue': len(self.queue),
            'failed_urls': len(self.failed_urls),
            'processed_urls': len(self.processed_urls),
            'visited_urls': len(self.visited_urls),
            'elapsed_sec': round(elapsed, 2),
            'pages_per_sec': round(processed_count / elapsed, 3),
        }