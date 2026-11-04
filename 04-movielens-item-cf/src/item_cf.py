"""Item-based коллаборативная фильтрация на MovieLens (ml-latest, ~33.8 млн оценок).

Схожесть фильмов считается как корреляция Пирсона по общим пользователям.
Сравнение с бейзлайном «всем предсказываем средний рейтинг» по RMSE.

Пример запуска:
    python src/item_cf.py --ratings data/ml-latest/ratings.csv
"""
import argparse

import numpy as np
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ratings", default="data/ml-latest/ratings.csv")
    parser.add_argument("--min-ratings", type=int, default=100, help="порог популярности фильма")
    parser.add_argument("--max-movies", type=int, default=500, help="сколько популярных фильмов брать в модель")
    parser.add_argument("--batch-size", type=int, default=30)
    parser.add_argument("--min-common-users", type=int, default=10)
    parser.add_argument("--neighbours", type=int, default=15)
    parser.add_argument("--user-sample", type=float, default=0.2, help="доля train-оценок для профилей пользователей")
    parser.add_argument("--test-sample", type=float, default=0.1, help="доля test для оценки RMSE")
    return parser.parse_args()


def compute_similarities(train, movie_ids, batch_size, min_common_users):
    """Попарная корреляция Пирсона между фильмами внутри батчей."""
    movie_ratings = {
        movie_id: {r["userId"]: r["rating"] for r in train.filter(F.col("movieId") == movie_id)
                   .select("userId", "rating").collect()}
        for movie_id in movie_ids
    }

    similarities = []
    for start in range(0, len(movie_ids), batch_size):
        batch = movie_ids[start:start + batch_size]
        for m1 in batch:
            for m2 in batch:
                if m1 >= m2:
                    continue
                r1, r2 = movie_ratings[m1], movie_ratings[m2]
                common = set(r1) & set(r2)
                if len(common) < min_common_users:
                    continue
                corr = np.corrcoef([r1[u] for u in common], [r2[u] for u in common])[0, 1]
                if not np.isnan(corr) and abs(corr) > 0.1:
                    similarities.append((m1, m2, float(corr), len(common)))
    return similarities


def build_similarity_index(similarities):
    index = {}
    for m1, m2, corr, common in similarities:
        index.setdefault(m1, []).append((m2, corr, common))
        index.setdefault(m2, []).append((m1, corr, common))
    for neighbours in index.values():
        neighbours.sort(key=lambda x: abs(x[1]), reverse=True)
    return index


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("MovieLensItemCF")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.memory.fraction", "0.8")
        .getOrCreate()
    )
    sc = spark.sparkContext

    ratings = spark.read.csv(args.ratings, header=True, inferSchema=True)
    train, test = ratings.randomSplit([0.8, 0.2], seed=42)

    # Бейзлайн: глобальный средний рейтинг
    global_avg = train.agg(F.avg("rating")).first()[0]
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    baseline_rmse = evaluator.evaluate(test.withColumn("prediction", F.lit(global_avg)))
    print(f"Средний рейтинг в train: {global_avg:.4f}")
    print(f"RMSE бейзлайна: {baseline_rmse:.4f}")

    movie_stats = train.groupBy("movieId").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("rating_count"),
    ).cache()
    popular = movie_stats.filter(F.col("rating_count") >= args.min_ratings).select("movieId").cache()
    train_popular = train.join(popular, "movieId").cache()

    movie_ids = [r["movieId"] for r in popular.limit(args.max_movies).collect()]
    similarities = compute_similarities(train_popular, movie_ids, args.batch_size, args.min_common_users)
    similarity_index = build_similarity_index(similarities)
    print(f"Значимых пар фильмов: {len(similarities)}, фильмов в модели: {len(similarity_index)}")

    movie_avg = {r["movieId"]: float(r["avg_rating"]) for r in movie_stats.collect()}

    user_ratings = {}
    for r in train.sample(args.user_sample, seed=42).select("userId", "movieId", "rating").collect():
        user_ratings.setdefault(r["userId"], {})[r["movieId"]] = float(r["rating"])
    print(f"Профили пользователей: {len(user_ratings)}")

    b_movie_avg = sc.broadcast(movie_avg)
    b_similarity = sc.broadcast(similarity_index)
    b_user_ratings = sc.broadcast(user_ratings)
    neighbours_k = args.neighbours

    def predict(user_id, movie_id):
        base = b_movie_avg.value.get(movie_id, global_avg)
        rated = b_user_ratings.value.get(user_id)
        neighbours = b_similarity.value.get(movie_id)
        if not rated or not neighbours:
            return float(base)

        weighted_sum = total_weight = 0.0
        used = 0
        for other, corr, common in neighbours[:neighbours_k]:
            if other in rated:
                # Доверие к корреляции растёт с числом общих пользователей
                weight = abs(corr) * min(1.0, common / 25.0)
                # Для отрицательной корреляции «отражаем» оценку пользователя
                rating = rated[other] if corr > 0 else 6.0 - rated[other]
                weighted_sum += weight * rating
                total_weight += weight
                used += 1

        if used >= 2 and total_weight > 0:
            prediction = 0.8 * weighted_sum / total_weight + 0.2 * base
        else:
            prediction = base
        return float(max(0.5, min(5.0, prediction)))

    predict_udf = F.udf(predict, DoubleType())
    test_sample = test.sample(args.test_sample, seed=42)
    cf_rmse = evaluator.evaluate(test_sample.withColumn("prediction", predict_udf("userId", "movieId")))

    print(f"RMSE item-based CF: {cf_rmse:.4f}")
    print(f"Улучшение относительно бейзлайна: {(baseline_rmse - cf_rmse) / baseline_rmse * 100:.2f}%")

    spark.stop()


if __name__ == "__main__":
    main()
