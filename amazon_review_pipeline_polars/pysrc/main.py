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
        .appName("Review")
        .getOrCreate()
    )


def read_data(path: str) -> DataFrame:
    spark = create_spark_session()
    spark.conf.set("spark.sql.caseSensitive", "true")
    # column_list = [
    #     "asin",
    #     "vote",
    #     "verified",
    #     "unixReviewTime",
    #     "reviewTime",
    #     "reviewText",
    # ]
    df = spark.read.json(path)
    # df = df.select(*column_list)

    # df = df.select(
    #     col("asin"),
    #     fn.coalesce(col("vote"), fn.lit("0")).alias("vote"),
    #     fn.to_timestamp(col("unixReviewTime")).alias("reviewed_at"),
    #     fn.coalesce(col("reviewText"), fn.lit("")).alias("review_text"),
    # )
    # df = df.withColumn("review_text_len", fn.length(col("review_text")))
    # df = df.withColumn("reviewed_year", fn.year(col("reviewed_at")))
    # df = df.withColumn("reviewed_month", fn.month(col("reviewed_at")))
    logging.info("Data loading plan created successfully!")
    return df


def main():
    DATA_PATH = 'datalayers/landing/Toys_and_Games_5.json'
    df = read_data(DATA_PATH)
    schema_info = ", ".join(df.columns)
    logging.info(f"Column List: {schema_info}")
    print(df.show(n=5))


if __name__ == "__main__":
    st = time.time()
    main()
    et = time.time()
    res = et - st
    logging.info("Pipeline executed successfully!")
    logging.info(f'Pipeline Execution time: {res}s.')