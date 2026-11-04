# Amazon Reviews Analytics

Батчевая аналитика отзывов Amazon в категории Electronics на PySpark: средние рейтинги товаров, доля плохо оценённых товаров и топ-10 худших с названиями.

## Данные

[Amazon Review Data (2014), Julian McAuley, UCSD](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon/links.html):

- `Electronics_5.json`: 5-core отзывы (у каждого товара и пользователя не меньше 5 отзывов);
- `meta_Electronics.json`: метаданные товаров (`asin`, `title`, ...).

Файлы нужно положить в `data/`.

## Как устроен расчёт

1. Средний `overall` по каждому `asin`: `groupBy` + `avg`.
2. Две метрики: средний рейтинг по всем отзывам и среднее средних рейтингов товаров. Разница между ними показывает, что у популярных товаров оценки выше.
3. `inner join` с метаданными, чтобы получить названия.
4. Фильтр `avg_rating < 3`, сортировка, топ-10.
5. Результат сохраняется в текстовом виде: `рейтинг\tназвание`.

## Запуск

```bash
python src/low_rated_products.py \
    --reviews data/Electronics_5.json \
    --meta data/meta_Electronics.json \
    --output output/worst_rated_electronics \
    --threshold 3.0 --top 10
```

Исследование с выводами лежит в [notebook.ipynb](notebook.ipynb).

## Результаты

| Метрика | Значение |
|---|---|
| Средний рейтинг по всем отзывам | 4.25 |
| Среднее средних рейтингов товаров | 4.15 |
| Товаров с рейтингом < 3 | 140 из 2866 (4.88%) |

| asin | Рейтинг | Товар |
|---|---|---|
| B00000JBIA | 1.00 | Agfa ePhoto SMILE 0.2MP Digital Camera |
| B00005T3Y9 | 1.14 | Terk AF-9330 AM/FM Indoor Amplified Tower Antenna |
| B00005T3XR | 1.29 | Jensen DSC-IR100A Remote Control Extender |
| B00007GWC8 | 1.80 | Digital Concepts TR-036CL Mini Tripod |
| B00004U8K4 | 1.80 | Plantronics T20 Corded 2-Line Headset Telephone |

Низкие оценки чаще получают устаревшая техника, снятые с производства модели и малоизвестные бренды.
