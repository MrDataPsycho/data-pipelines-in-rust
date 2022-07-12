from dataclasses import dataclass
from pathlib import Path
import logging
import time
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from dotenv import load_dotenv

logging.basicConfig(format='[%(asctime)s  %(levelname)s amazon_review_pipeline] %(message)s', level=logging.INFO)

def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("MovieReview")
        .getOrCreate()
    )


def read_data(path: str) -> DataFrame:
    spark = create_spark_session()
    spark.conf.set("spark.sql.caseSensitive", "true")
    column_list = [
        "asin",
        "vote",
        "verified",
        "unixReviewTime",
        "reviewTime",
        "reviewText",
    ]
    df = spark.read.json(path)
    df = df.select(*column_list)

    df = df.select(
        col("asin"),
        fn.coalesce(col("vote"), fn.lit("0")).alias("vote"),
        fn.to_timestamp(col("unixReviewTime")).alias("reviewed_at"),
        fn.coalesce(col("reviewText"), fn.lit("")).alias("review_text"),
    )
    df = df.withColumn("review_text_len", fn.length(col("review_text")))
    df = df.withColumn("reviewed_year", fn.year(col("reviewed_at")))
    df = df.withColumn("reviewed_month", fn.month(col("reviewed_at")))
    logging.info("Data loading plan created successfully!")
    return df


def add_processed_columns(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "review_text_ctg",
        fn.when(col("review_text_len") >= 200, "long")
        .when((col("review_text_len") > 10) & (col("review_text_len") < 200), "medium")
        .when((col("review_text_len") > 1) & (col("review_text_len") <= 10), "short")
        .otherwise("invalid")
    )
    logging.info("Plan for processed layer created successfully!");
    return df.repartition(20)

def prepare_aggregated_insights(df: DataFrame) -> DataFrame:
    df = (
        df.filter(col("review_text_len") > 0)
        .groupBy(col("asin"), col("reviewed_year"), col("reviewed_month"))
        .agg(
            fn.count(col("asin")).alias("total_review")
        )
        .sort(col("reviewed_year"), col("reviewed_month"))
    )
    logging.info("Plan for Aggregate Layer created successfully!");
    return df.repartition(12, col("reviewed_year"))


def main():
    DATA_PATH = 'datalayers/landing/Toys_and_Games_5.json'
    df = read_data(DATA_PATH)
    processed_df = add_processed_columns(df)
    insights_df = prepare_aggregated_insights(processed_df)
    processed_df.write.mode("overwrite").csv("datalayers/analytics/toys_n_game")
    logging.info("Data Written successfully in analytics layer!")
    insights_df.write.mode("overwrite").csv("datalayers/insights/toys_n_game")
    logging.info("Data Written successfully in insights layer!")
    # insights_df.show(3)

if __name__ == "__main__":
    st = time.time()
    main()
    et = time.time()
    res = et - st
    logging.info("Pipeline executed successfully!")
    logging.info(f'Pipeline Execution time: {res}s.')
    



