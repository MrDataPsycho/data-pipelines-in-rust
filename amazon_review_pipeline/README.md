# Amazon Review Pipeline (DataFusion)
The data is based on Amazon Product review on games and toys with 800+ Megabyte of data. Each record in the data is a product review.

The following transformation is demonestrated in the data pipeline:
- Reading Json Data with DataFusion
- Select sebsection of the data
- Add transformed timestamp, categorical, numerical columns
- Remove null values with a fixed value
- SQL like case when statement to create categorical column
- Filter and Sort the Data
- Repartition the Data
- Save the data with partition

Framework use:
- Rust: DataFusion
- Python: PySpark

## Performance Logs:

Logs for Pyspark:
```
datapsycho@dataops:~/.../amazon_review_pipeline$ make run_pyspark_release 
poetry run python pysrc/main.py
[2022-07-14 01:20:47,833  INFO amazon_review_pipeline] Data loading plan created successfully!
[2022-07-14 01:20:47,920  INFO amazon_review_pipeline] Plan for processed layer created successfully!
[2022-07-14 01:20:48,096  INFO amazon_review_pipeline] Plan for Aggregate Layer created successfully!
[2022-07-14 01:21:00,630  INFO amazon_review_pipeline] Data Written successfully in analytics layer!
[2022-07-14 01:21:11,641  INFO amazon_review_pipeline] Data Written successfully in insights layer!
[2022-07-14 01:21:11,642  INFO amazon_review_pipeline] Pipeline executed successfully!
[2022-07-14 01:21:11,642  INFO amazon_review_pipeline] Pipeline Execution time: 35.40674662590027s.
```

Logs for Rust DataFusion:
```
datapsycho@dataops:~/.../amazon_review_pipeline$ make run_rust_release 
RUST_LOG=info ./target/release/amazon_review_pipeline
[2022-07-13T23:21:52Z INFO  amazon_review_pipeline] Data loading plan created successfully!
[2022-07-13T23:21:52Z INFO  amazon_review_pipeline] Year month lenght column added plan created successfully!
[2022-07-13T23:21:52Z INFO  amazon_review_pipeline] Plan for processed layer created successfully!
[2022-07-13T23:21:52Z INFO  amazon_review_pipeline] Plan for Aggregate Layer created successfully!
[2022-07-13T23:22:01Z INFO  amazon_review_pipeline] Data Written successfully in analytics layer!
[2022-07-13T23:22:12Z INFO  amazon_review_pipeline] Data Written successfully in analytics layer!
[2022-07-13T23:22:12Z INFO  amazon_review_pipeline] Pipeline executed successfully!
[2022-07-13T23:22:12Z INFO  amazon_review_pipeline] Pipeline Execution time: 20.104681726s
```
