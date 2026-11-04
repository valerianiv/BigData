"""Расчёт дистанций поездок Citi Bike по формуле гаверсинусов на PySpark.

Пример запуска:
    python src/trip_distance.py --input data/201902-citibike-tripdata.csv
"""
import argparse
import math

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

EARTH_RADIUS_M = 6_371_000.0


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default="data/201902-citibike-tripdata.csv")
    parser.add_argument("--output", default=None, help="куда сохранить поездки с дистанцией (parquet)")
    return parser.parse_args()


def rad(col: Column) -> Column:
    return col * F.lit(math.pi / 180.0)


def haversine_m(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Кратчайшее расстояние между двумя точками на сфере, в метрах."""
    dlat = rad(lat2 - lat1)
    dlon = rad(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(rad(lat1)) * F.cos(rad(lat2)) * F.sin(dlon / 2) ** 2
    # Защита от выхода за [0, 1] из-за погрешности вычислений с плавающей точкой
    a = F.least(F.greatest(a, F.lit(0.0)), F.lit(1.0))
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(a))


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("CitiBikeTripDistance")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    trips = (
        spark.read.option("header", "true").csv(args.input)
        .withColumn("start_lat", F.col("start station latitude").cast("double"))
        .withColumn("start_lon", F.col("start station longitude").cast("double"))
        .withColumn("end_lat", F.col("end station latitude").cast("double"))
        .withColumn("end_lon", F.col("end station longitude").cast("double"))
        .dropna(subset=["start_lat", "start_lon", "end_lat", "end_lon"])
        # Поездки, которые начались и закончились в одной точке, не учитываем
        .filter((F.col("start_lat") != F.col("end_lat")) | (F.col("start_lon") != F.col("end_lon")))
        .withColumn(
            "distance_m",
            haversine_m(F.col("start_lat"), F.col("start_lon"), F.col("end_lat"), F.col("end_lon")),
        )
        .cache()
    )

    trips.select("start_lat", "start_lon", "end_lat", "end_lon", "distance_m").show(10, truncate=False)

    stats = trips.select(
        F.max("distance_m").alias("max"),
        F.mean("distance_m").alias("mean"),
        F.stddev("distance_m").alias("stddev"),
    ).first()
    median = trips.approxQuantile("distance_m", [0.5], 0.01)[0]

    print(f"Максимум:               {stats['max']:.2f} м")
    print(f"Среднее:                {stats['mean']:.2f} м")
    print(f"Стандартное отклонение: {stats['stddev']:.2f} м")
    print(f"Медиана:                {median:.2f} м")

    if args.output:
        trips.write.mode("overwrite").parquet(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
