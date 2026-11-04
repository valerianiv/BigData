"""Поиск товаров с самым низким средним рейтингом в отзывах Amazon Electronics.

Пример запуска:
    python src/low_rated_products.py \
        --reviews data/Electronics_5.json \
        --meta data/meta_Electronics.json \
        --output output/worst_rated_electronics
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reviews", default="data/Electronics_5.json")
    parser.add_argument("--meta", default="data/meta_Electronics.json")
    parser.add_argument("--output", default="output/worst_rated_electronics")
    parser.add_argument("--threshold", type=float, default=3.0)
    parser.add_argument("--top", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("AmazonLowRatedProducts")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    meta_df = (
        spark.read.json(args.meta)
        .filter(F.col("asin").isNotNull())
        .select("asin", "title")
    )
    reviews_df = spark.read.json(args.reviews)

    # Средний рейтинг по каждому товару
    avg_rating_df = (
        reviews_df.groupBy("asin")
        .agg(F.avg("overall").alias("avg_rating"))
        .filter(F.col("avg_rating").isNotNull())
        .cache()
    )

    total_avg = reviews_df.select(F.avg("overall")).first()[0]
    avg_of_avg = avg_rating_df.select(F.avg("avg_rating")).first()[0]
    print(f"Средний рейтинг по всем отзывам: {total_avg:.2f}")
    print(f"Среднее значение средних рейтингов товаров: {avg_of_avg:.2f}")

    low_rated_df = (
        avg_rating_df.join(meta_df, on="asin", how="inner")
        .filter(F.col("avg_rating") < args.threshold)
    )
    low_count = low_rated_df.count()
    total_products = avg_rating_df.count()
    print(
        f"Товаров с рейтингом меньше {args.threshold}: {low_count} из {total_products} "
        f"({100 * low_count / total_products:.2f}%)"
    )

    worst = low_rated_df.orderBy("avg_rating").limit(args.top)
    worst.show(truncate=False)

    (
        worst.select(
            F.concat_ws("\t", F.format_number("avg_rating", 4), F.col("title")).alias("result")
        )
        .coalesce(1)
        .write.mode("overwrite")
        .text(args.output)
    )
    print("Результат сохранён в", args.output)

    spark.stop()


if __name__ == "__main__":
    main()
