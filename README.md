# Async Web Crawler

Асинхронный веб-краулер на Python с ограничением конкурентности, rate limiting, обработкой `robots.txt`, retry/backoff, хранением данных, CLI, мониторингом и отчётами.

Проект разработан как итоговая интеграция тем асинхронного программирования: `asyncio`, `aiohttp`, очереди задач, семафоры, обработка ошибок, конфигурация и измерение производительности.

## Возможности

- Асинхронная загрузка страниц через `aiohttp`
- Ограничение общего числа одновременных задач
- Ограничение одновременных запросов на домен
- Rate limiting: ограничение запросов в секунду
- Поддержка `robots.txt` и `crawl-delay`
- Обход ссылок до указанной глубины
- Фильтрация ссылок по домену, include/exclude patterns
- Повторы запросов при временных ошибках
- Exponential backoff и настраиваемые timeout
- Парсинг HTML: текст, заголовок, ссылки, изображения, таблицы и списки
- Сохранение данных в JSON, CSV и PostgreSQL
- `MultiStorage` для записи в несколько хранилищ
- Sitemap-поддержка
- CLI-интерфейс на основе `argparse`
- Логирование в консоль и файл с ротацией
- Мониторинг прогресса, RPS, ETA, очереди и активных задач
- JSON-статистика и HTML-отчёт
- Performance benchmark с измерением времени и памяти

## Структура проекта

```text
parallelism-and-asynchrony/
├── config.json
├── requirements.txt
├── README.md
├── logs/
│   └── crawler.log
├── output/
│   ├── day7_report.html
│   ├── day7_stats.json
│   └── performance_report.json
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
- `pip`
- PostgreSQL — только если в `config.json` включено `PostgreSQLStorage`

Все Python-зависимости проекта перечислены в файле `requirements.txt`.

Из корневой папки проекта установи их командой:

```bash
pip install -r requirements.txt
```

При использовании виртуального окружения сначала активируй его, затем выполни ту же команду:

```bash
pip install -r requirements.txt
```

## Быстрый запуск

Запуск crawler с настройками из `config.json`:

```bash
python src/main.py
```

Запуск с одним URL и ограничением в одну страницу:

```bash
python src/main.py \
  --urls https://example.com \
  --max-pages 1 \
  --max-depth 0 \
  --output output/results.json \
  --rate-limit 1
```

Пример обхода нескольких страниц:

```bash
python src/main.py \
  --urls https://httpbingo.org \
  --max-pages 10 \
  --max-depth 1 \
  --output output/httpbingo_results.json \
  --respect-robots \
  --rate-limit 2
```

Справка по аргументам:

```bash
python src/main.py --help
```

## CLI-параметры

| Аргумент | Описание |
|---|---|
| `--urls URL [URL ...]` | Стартовый URL или несколько URL |
| `--max-pages N` | Максимальное число обрабатываемых страниц |
| `--max-depth N` | Максимальная глубина обхода |
| `--output PATH` | Путь к JSON-файлу итоговой статистики |
| `--config PATH` | Путь к файлу конфигурации; по умолчанию `config.json` |
| `--respect-robots` | Включить соблюдение правил `robots.txt` |
| `--rate-limit N` | Лимит запросов в секунду |

CLI-параметры имеют приоритет над соответствующими значениями из `config.json`.

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
    "start_urls": [
      "https://example.com"
    ],
    "sitemap_urls": [],
    "max_pages": 10,
    "same_domain_only": true,
    "include_patterns": [],
    "exclude_patterns": []
  },
  "storage": {
    "json": {
      "filename": "output/pages.json"
    },
    "csv": {
      "filename": "output/pages.csv"
    }
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

## Использование API

Минимальный пример:

```python
import asyncio
import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent / "src")
)

from crawler import AdvancedCrawler


async def main() -> None:
    crawler = AdvancedCrawler.from_config(
        "config.json",
    )

    try:
        await crawler.crawl()

        stats = crawler.get_stats()

        print(
            f"Обработано: {stats['total_pages']} страниц"
        )
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")

        crawler.export_to_json(
            "output/api_stats.json",
        )
        crawler.export_to_html_report(
            "output/api_report.html",
        )
    finally:
        await crawler.close()


asyncio.run(main())
```

### `AdvancedCrawler`

| Метод | Назначение |
|---|---|
| `AdvancedCrawler.from_config(filename)` | Создаёт crawler и загружает JSON-конфигурацию |
| `await crawler.crawl()` | Запускает обход start URLs и sitemap |
| `crawler.get_stats()` | Возвращает итоговую статистику |
| `crawler.export_to_json(filename)` | Экспортирует статистику в JSON |
| `crawler.export_to_html_report(filename)` | Создаёт HTML-отчёт |
| `await crawler.close()` | Закрывает HTTP-сессию и подключённые хранилища |

## Мониторинг

Во время обхода crawler выводит состояние в реальном времени:

```text
📄 Обработано: 24/100 (24.0%) | ✅ Успешно: 20 | ❌ Ошибок: 4 |
⏳ В очереди: 15 | 🔄 Активно: 2 | 🚫 Robots: 0 | ⚡ RPS: 1.03 |
⏱ Задержка: 1.00с | ⌛ ETA: 73.9с | ⏰ Время: 26.3с
```

Показатели:

- `Обработано` — завершённые URL и процент от `max_pages`
- `Успешно` — успешно загруженные и распарсенные страницы
- `Ошибок` — URL с постоянной или исчерпавшей retry ошибкой
- `В очереди` — URL, ожидающие обработки
- `Активно` — незавершённые асинхронные задачи
- `RPS` — текущая скорость HTTP-запросов
- `ETA` — оценка времени до достижения лимита страниц
- `Время` — время работы crawler

## Логирование

Логирование настраивается в `src/main.py`.

- Консоль: сообщения уровня `INFO` и выше
- Файл: `logs/crawler.log`
- Уровень файла: `DEBUG` и выше
- Ротация: 1 MB на файл, до 3 резервных файлов
- Формат: дата, время, уровень, имя logger-а, сообщение

Пример записи:

```text
2026-09-27 16:34:12 | INFO     | crawler | Успешно загружено: https://example.com, статус: 200
```

## Производительность

Запуск benchmark:

```bash
python src/performance_test.py
```

Отчёт сохраняется в:

```text
output/performance_report.json
```

## Выходные файлы

| Файл | Содержимое |
|---|---|
| `logs/crawler.log` | Основной лог crawler-а |
| `output/day7_stats.json` | Итоговая статистика по умолчанию |
| `output/day7_report.html` | HTML-отчёт по умолчанию |
| `output/performance_report.json` | Результаты benchmark |
| `output/pages.json` | Данные страниц при JSON storage |
| `output/pages.csv` | Данные страниц при CSV storage |