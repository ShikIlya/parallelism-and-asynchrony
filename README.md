# PostgreSQL setup

Демонстрация и тесты используют одну локальную PostgreSQL-базу и одну таблицу:

```python
TEST_DATABASE = "crawler_db"
TEST_USER = "ilyashik"
```

Таблица `pages` и индексы создаются автоматически программой. Вручную создавать таблицу не нужно.

## 1. Установите PostgreSQL

### macOS

Установите [Postgres.app](https://postgresapp.com/) и запустите приложение.

Чтобы команда `psql` была доступна в Terminal, выполните:

```bash
sudo mkdir -p /etc/paths.d
echo /Applications/Postgres.app/Contents/Versions/latest/bin | sudo tee /etc/paths.d/postgresapp
```

Закройте и заново откройте Terminal. Проверьте установку:

```bash
psql --version
```

### Windows

Установите PostgreSQL через [официальный установщик](https://www.postgresql.org/download/windows/).

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl enable --now postgresql
```

## 2. Создайте базу

Узнайте имя пользователя:

```bash
whoami
```

Создайте базу `crawler_db`:

```bash
createdb -U YOUR_POSTGRES_USER crawler_db
```

На macOS с Postgres.app обычно достаточно:

```bash
createdb -U "$(whoami)" crawler_db
```

Если пользователь PostgreSQL отличается от `ilyashik`, замените его в двух местах:

`src/postgresql_storage.py`:

```python
user: str = "YOUR_POSTGRES_USER"
```

`tests/test_main.py`:

```python
TEST_USER = "YOUR_POSTGRES_USER"
```

## 3. Установите зависимости

В корне проекта:

```bash
python -m pip install -r requirements.txt
```

## 4. Запустите демонстрацию

```bash
python src/main.py
```

При первом сохранении создаются:

- таблица `pages`;
- индекс `idx_pages_url`;
- индекс `idx_pages_crawled_at`;
- индекс `idx_pages_status_code`.

## 5. Проверьте данные

```bash
psql -d crawler_db -U YOUR_POSTGRES_USER
```

Внутри `psql`:

```sql
SELECT
    id,
    url,
    title,
    status_code,
    content_type,
    crawled_at
FROM pages
ORDER BY id DESC
LIMIT 10;
```

Выход:

```sql
\q
```

## 6. Запустите тесты

```bash
python -m pytest -v
```

Тесты используют эту же таблицу `pages`. Fixture `postgres_storage` очищает её перед тестом и после теста:

```sql
TRUNCATE TABLE pages RESTART IDENTITY;
```

Поэтому после запуска тестов таблица `pages` будет пустой. Чтобы снова заполнить её данными для демонстрации, запустите:

```bash
python src/main.py
```
