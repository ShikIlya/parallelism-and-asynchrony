# Async Web Crawler

Асинхронный веб-краулер на Python с ограничением конкурентности, rate limiting, обработкой `robots.txt`, retry/backoff, хранением данных, CLI, мониторингом и отчётами.

Проект разработан как итоговая интеграция тем асинхронного программирования: `asyncio`, `aiohttp`, очереди задач, семафоры, обработка ошибок, конфигурация и измерение производительности.

## Возможности

- Асинхронная загрузка страниц через `aiohttp`
- Ограничение общего числа одновременных задач
- Ограничение одновременных запросов на домен
- Rate limiting с изменением лимита через CLI
- Поддержка `robots.txt` и `crawl-delay`
- Обход ссылок до указанной глубины
- Фильтрация ссылок по домену, include/exclude patterns
- Повторы запросов при временных ошибках
- Exponential backoff и настраиваемые timeout
- Парсинг HTML: текст, заголовок, ссылки, изображения, таблицы и списки
- Сохранение данных в JSON, CSV и PostgreSQL
- `MultiStorage` для записи в несколько хранилищ
- Sitemap-поддержка, включая вложенные sitemap
- CLI-интерфейс на основе `argparse` с подкомандами
- Логирование в консоль и файл с ротацией
- Мониторинг прогресса, RPS, ETA, очереди и активных задач
- JSON-статистика и HTML-отчёт
- Performance benchmark с измерением времени и памяти

## Структура проекта

```text
parallelism/
├── config.json
├── requirements.txt
├── README.md
├── logs/
├── output/
└── src/
    ├── crawler.py
    ├── crawler_queue.py
    ├── crawler_stats.py
    ├── csv_storage.py
    ├── data_storage.py
    ├── exceptions.py
    ├── html_parser.py
    ├── json_storage.py
    ├── main.py
    ├── models.py
    ├── multi_storage.py
    ├── performance_test.py
    ├── postgresql_storage.py
    ├── rate_limiter.py
    ├── retry_strategy.py
    ├── robots_parser.py
    ├── semaphore_manager.py
    ├── sitemap_parser.py
    ├── storage_parser.py
    └── utils.py
```

## Установка

### Требования

- Python 3.10+
- виртуальное окружение Python рекомендуется
- PostgreSQL — только если в `config.json` включён `PostgreSQLStorage`

Все внешние зависимости проекта перечислены в `requirements.txt`.

Создание виртуального окружения:

```bash
python -m venv venv
```

Активация в Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

Активация в Linux/macOS:

```bash
source venv/bin/activate
```

Установка всех зависимостей из корня проекта:

```bash
python -m pip install -r requirements.txt
```

Проверка драйвера PostgreSQL:

```bash
python -m pip show asyncpg
```

## Быстрый запуск

Основной режим запуска — команда `crawl`:

```bash
python src/main.py crawl
```

Запуск с одним URL и ограничением в одну страницу:

```bash
python src/main.py crawl --urls https://example.com --max-pages 1 --max-depth 0 --output output/results.json --rate-limit 1
```

Для Linux/macOS можно использовать переносы строк:

```bash
python src/main.py crawl \
  --urls https://example.com \
  --max-pages 1 \
  --max-depth 0 \
  --output output/results.json \
  --rate-limit 1
```

Для Windows PowerShell:

```powershell
python src/main.py crawl --urls https://example.com --max-pages 1 --max-depth 0 --output output/results.json --rate-limit 1
```

Пример обхода нескольких страниц:

```bash
python src/main.py crawl --urls https://httpbingo.org --max-pages 10 --max-depth 1 --output output/httpbingo_results.json --respect-robots --rate-limit 2
```

Справка по всем командам:

```bash
python src/main.py --help
```

Справка по основному crawler-режиму:

```bash
python src/main.py crawl --help
```

## Команды

CLI разделён на подкоманды. Поэтому `--help` не запускает сетевые демонстрации, а дни 1–6 выполняются только по явной команде.

| Команда | Назначение |
|---|---|
| `crawl` | Запуск полноценного `AdvancedCrawler` |
| `day1` | Сравнение последовательной и параллельной загрузки |
| `day2` | Парсинг HTML |
| `day3` | Управление конкурентностью и очередями |
| `day4` | Мониторинг скорости и прогресса |
| `day5` | Retry, exponential backoff и обработка ошибок |
| `day6` | Сохранение данных в JSON, CSV и PostgreSQL |

Примеры:

```bash
python src/main.py day1
python src/main.py day2
python src/main.py day3
python src/main.py day4
python src/main.py day5
python src/main.py day6
```

Команды `day1`–`day6` не запускаются автоматически при вызове `crawl` или `--help`.

## CLI-параметры

Параметры crawler-а передаются после подкоманды `crawl`:

```bash
python src/main.py crawl [параметры]
```

| Аргумент | Описание |
|---|---|
| `--urls URL [URL ...]` | Стартовый URL или несколько URL |
| `--max-pages N` | Максимальное число обрабатываемых страниц |
| `--max-depth N` | Максимальная глубина обхода |
| `--output PATH` | Путь к JSON-файлу итоговой статистики |
| `--config PATH` | Путь к JSON-конфигурации; по умолчанию `config.json` |
| `--respect-robots` | Включить соблюдение правил `robots.txt` |
| `--rate-limit N` | Лимит запросов в секунду |

CLI-параметры имеют приоритет над соответствующими значениями из `config.json`.

`--rate-limit` изменяет фактическую задержку между запросами. Например:

```text
--rate-limit 4
min_interval = 1 / 4 = 0.25 секунды
```

## Конфигурация

Crawler читает конфигурацию из JSON через:

```python
crawler = AdvancedCrawler.from_config("config.json")
```

Пример структуры `config.json`:

```json
{
  "crawler": {
    "max_concurrent": 3,
    "max_depth": 1,
    "max_per_domain": 2,
    "requests_per_second": 1.0,
    "rate_limit_per_domain": true,
    "respect_robots": true,
    "user_agent": "AsyncCrawler/1.0",
    "min_delay": 0.0,
    "jitter": 0.0,
    "max_retries": 2,
    "backoff_factor": 0.5,
    "total_timeout": 30.0,
    "connect_timeout": 10.0,
    "read_timeout": 20.0,
    "timeout_backoff_factor": 1.5,
    "max_timeout": 120.0
  },
  "crawl": {
    "start_urls": ["https://example.com"],
    "sitemap_urls": [],
    "max_pages": 10,
    "same_domain_only": true,
    "include_patterns": [],
    "exclude_patterns": []
  },
  "storage": {
    "json": {"filename": "output/pages.json"},
    "csv": {"filename": "output/pages.csv"}
  }
}
```

### Основные настройки

| Группа | Поле | Назначение |
|---|---|---|
| `crawler` | `max_concurrent` | Максимальное число активных задач |
| `crawler` | `max_per_domain` | Одновременные запросы к одному домену |
| `crawler` | `requests_per_second` | Лимит запросов в секунду |
| `crawler` | `respect_robots` | Проверять правила `robots.txt` |
| `crawler` | `max_retries` | Число повторов временных ошибок |
| `crawler` | `backoff_factor` | Базовая задержка retry |
| `crawl` | `start_urls` | Начальные адреса обхода |
| `crawl` | `sitemap_urls` | Адреса sitemap |
| `crawl` | `max_pages` | Лимит страниц |
| `crawl` | `same_domain_only` | Не переходить на внешние домены |
| `crawl` | `include_patterns` | Разрешённые подстроки URL |
| `crawl` | `exclude_patterns` | Исключённые подстроки URL |

### Обработка sitemap

Sitemap и вложенные sitemap загружаются через общий pipeline crawler-а. Поэтому такие запросы используют rate limiting, retry, timeout, robots.txt, логирование и общую HTTP-статистику.

Если один sitemap недоступен, ошибка записывается в лог, а обработка остальных sitemap может продолжиться.

## Использование API

```python
import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from crawler import AdvancedCrawler


async def main() -> None:
    crawler = AdvancedCrawler.from_config("config.json")

    try:
        await crawler.crawl()
        stats = crawler.get_stats()

        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")

        crawler.export_to_json("output/api_stats.json")
        crawler.export_to_html_report("output/api_report.html")
    finally:
        await crawler.close()


asyncio.run(main())
```

### `AdvancedCrawler`

| Метод | Назначение |
|---|---|
| `AdvancedCrawler.from_config(filename)` | Создаёт crawler и загружает JSON-конфигурацию |
| `await crawler.crawl()` | Запускает обход стартовых URL и sitemap |
| `crawler.get_stats()` | Возвращает итоговую статистику |
| `crawler.export_to_json(filename)` | Экспортирует статистику в JSON |
| `crawler.export_to_html_report(filename)` | Создаёт HTML-отчёт |
| `crawler.rate_limiter.set_requests_per_second(value)` | Изменяет лимит и пересчитывает интервал запросов |
| `await crawler.close()` | Закрывает HTTP-сессию и подключённые хранилища |

## Мониторинг

Во время обхода crawler выводит состояние в реальном времени:

```text
📄 Обработано: 24/100 (24.0%) | ✅ Успешно: 20 | ❌ Ошибок: 4 |
⏳ В очереди: 15 | 🔄 Активно: 2 | 🚫 Robots: 0 | ⚡ RPS: 1.03 |
⏱ Задержка: 1.00с | ⌛ ETA: 73.9с | ⏰ Время: 26.3с
```

## Логирование

- Консоль: `INFO` и выше
- Файл: `logs/crawler.log`
- Файл: `DEBUG` и выше
- Ротация: 1 MB на файл, до 3 резервных файлов
- Формат: дата, время, уровень, имя logger-а и сообщение

## Производительность

Запуск benchmark:

```bash
python src/performance_test.py
```

Отчёт сохраняется в:

```text
output/performance_report.json
```

Тест сравнивает синхронную и асинхронную загрузку, измеряет время и память, а также запускает `AdvancedCrawler` с лимитами 100, 500 и 1000 страниц.

## Выходные файлы

| Файл | Содержимое |
|---|---|
| `logs/crawler.log` | Основной лог crawler-а с ротацией |
| `output/day7_stats.json` | Итоговая статистика основного crawl-режима |
| `output/day7_report.html` | HTML-отчёт основного crawl-режима |
| `output/performance_report.json` | Результаты benchmark |
| `output/day3_crawl_results.json` | Результаты демонстрации дня 3 |
| `output/day5_retry_report.json` | Отчёт по retry и ошибкам |
| `output/day6_pages.json` | Данные страниц в JSON |
| `output/day6_pages.csv` | Данные страниц в CSV |
| `output/day6_report.json` | Сводный отчёт дня 6 |
