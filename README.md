# CDC — pet project

Проект демонстрирует простую CDC-пайплайн-архитектуру (симуляция): генерация событий заказов → Kafka → Spark Streaming → S3 (MinIO) → компактация → загрузка в ClickHouse через Airflow.

**Ключевые компоненты**
- **Producer**: `producer/producer.py` — генератор событий заказов, публикует события в топик Kafka `orders`.
- **Kafka**: брокер для событий (сконфигурирован в `docker-compose.yaml`).
- **Spark**: потоковая обработка и батч-джобы в `spark/spark_jobs/`:
	- `kafka_to_s3.py` — стриминг из Kafka в S3 (Parquet, raw).
	- `compact_orders.py` — компактация/пересборка партиций в `silver`.
	- `from_s3_to_click.py` — загрузка из `silver` в ClickHouse.
- **MinIO (S3)**: хранилище для raw/silver данных и чекпоинтов.
- **Airflow**: оркестрация DAG'ов в `dags/`:
	- `compaction_orders` — запускает `compact_orders.py`.
	- `load_orders_to_clickhouse` — ждёт компактацию и запускает загрузку в ClickHouse.
- **ClickHouse**: аналитическая БД, инициализация схемы в `Clickhouse/init.sql`.

Текущая структура (важные файлы/папки):

- `docker-compose.yaml` — локальная инфраструктура (Postgres, Airflow, MinIO, Kafka, Spark, ClickHouse, producer).
- `producer/producer.py` — генератор событий.
- `spark/spark_jobs/` — Spark-скрипты (стрим + batch).
- `dags/` — Airflow DAG'ы.
- `Clickhouse/init.sql` — создание таблиц и materialized view.

Быстрый старт (локально, Docker)

1) Установите Docker и Docker Compose (совместимый с `docker-compose.yaml`).
2) Создайте файл `.env` в корне проекта и задайте необходимые переменные окружения (пример ниже).
3) Запустите инфраструктуру:

```
docker compose up --build
```

Или запустить только ключевые сервисы:

```
docker compose up --build kafka minio clickhouse producer spark-streaming airflow-webserver airflow-scheduler
```

Основные переменные окружения (пример, задаются в `.env`):

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `S3_BUCKET`, `S3_ENDPOINT_URL`, `AWS_DEFAULT_REGION`
- `KAFKA_TOPIC_ORDERS`, `KAFKA_UI_PORT`, `KAFKA_CLUSTER_ID`, `KAFKA_NODE_ID`
- `CLICKHOUSE_HOST`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_PORT`
- Airflow: `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_UID`, `DAGS_START_DATE` и т.д.

Пользовательские действия
- Открыть Airflow UI: `http://localhost:8080` (проверьте .env для учётных данных).
- Kafka UI: порт задан в `KAFKA_UI_PORT` в `.env`.
- Посмотреть логи контейнеров:

```
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f airflow-scheduler
```

Генерация данных
- Producer автоматически публикует события при запуске сервиса `producer` в Compose. Альтернатива: запустить локально

```
python producer/producer.py
```

Архитектурные примечания
- Поток: `producer` → Kafka(`orders`) → `spark_streaming` (`kafka_to_s3.py`) → raw Parquet в MinIO → `compaction_orders` (batch `compact_orders.py`) → silver Parquet → `from_s3_to_click.py` → ClickHouse.
- Airflow использует DockerOperator для запуска Spark job'ов внутри контейнеров (см. `dags/*`).
- ClickHouse содержит MV и агрегаты в `Clickhouse/init.sql` для быстрых аналитик.

Разработка и отладка
- Локально можно запускать отдельные скрипты (например, Spark-скрипты) в контейнере `spark` через `spark-submit`, или запускать `producer` напрямую в вашей среде разработки.
- Для проверки данных в MinIO используйте `mc` или UI MinIO на порту 9001.

TODO / возможные улучшения
- Детализировать `.env.example` с обязательными переменными и значениями по умолчанию.
- Добавить unit/integration тесты для DAG'ов и Spark-логики.
- Добавить инструкции для запуска Spark локально (без Docker) и сборки образов.

Если хотите, могу:
- Сгенерировать файл `.env.example` с рекомендуемыми значениями.
- Дооформить раздел «Развёртывание» и добавить команды для CI/CD.

Автор: Камиль

