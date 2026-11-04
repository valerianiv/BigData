# Big Data Projects

Набор проектов по обработке больших данных: батчевая аналитика на **Apache Spark**, потоковая обработка через **Apache Kafka** и рекомендательная система на датасете в 33 млн оценок.

| # | Проект | Что делает | Стек | Объём данных |
|---|--------|------------|------|--------------|
| 01 | [Amazon Reviews Analytics](01-amazon-reviews-analytics) | Ищет товары с худшими отзывами, джойнит отзывы с метаданными | PySpark, Spark SQL | 2 866 товаров |
| 02 | [Citi Bike Trip Distance](02-citibike-trip-distance) | Считает дистанции поездок по формуле гаверсинусов нативными функциями Spark | PySpark | 1 месяц поездок |
| 03 | [Telegram → Kafka Streaming](03-telegram-kafka-streaming) | Читает новостные Telegram-каналы в реальном времени, считает частоты имён собственных в минутных окнах | Kafka, Telethon, asyncio | стрим |
| 04 | [MovieLens Item-based CF](04-movielens-item-cf) | Item-based коллаборативная фильтрация, на 9% лучше бейзлайна по RMSE | PySpark, Spark ML, NumPy | 33.8 млн оценок |

## Основные результаты

- **Amazon:** у 140 из 2866 товаров (4.9%) средний рейтинг ниже 3; в худшую десятку попали в основном снятые с производства устройства.
- **Citi Bike:** медианная поездка 1.26 км, средняя 1.64 км, максимальная 15.3 км.
- **Telegram:** пайплайн producer → топик Kafka → consumer с окнами по минуте, итоговый топ-10 за 30 минут.
- **MovieLens:** RMSE снизился с 1.064 (бейзлайн) до 0.967.

## Структура

```
.
├── 01-amazon-reviews-analytics/
│   ├── notebook.ipynb        # исследование с результатами
│   └── src/                  # тот же расчёт в виде запускаемого скрипта
├── 02-citibike-trip-distance/
├── 03-telegram-kafka-streaming/
│   ├── docker-compose.yml    # Kafka в режиме KRaft
│   └── src/
├── 04-movielens-item-cf/
└── requirements.txt
```

## Быстрый старт

Нужны Python 3.10+, Java 11+ (для Spark) и Docker (для Kafka в проекте 03).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Данные в репозиторий не входят: как их скачать, написано в README каждого проекта. Файлы кладутся в папку `data/` внутри проекта.
