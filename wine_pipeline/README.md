# Wine Pipeline
More details about the data set can be found in in the `datastore` directory.

The following transformation is demonestrated in the data pipeline:
- Reading data with Polars CsvReader
- Show summary of the Dataset
- Groupby and Aggregation
- Create Calculated Column
- Upsample the data
- Aggregation over Up sampled data to measure execution time


## Peformance Logs:
Logs for Polars:
```
datapsycho@dataops:~/.../wine_pipeline$ make run_rust_release 
RUST_LOG=info ./target/release/wine_pipeline
[2022-07-15T23:23:45Z INFO  wine_pipeline] Data read successfully!
[2022-07-15T23:23:45Z INFO  wine_pipeline] Basic Statistics calculated.
[2022-07-15T23:23:45Z INFO  wine_pipeline] Mean Max Distribution of Proline calculated.
[2022-07-15T23:23:45Z INFO  wine_pipeline] Ration data frame is Ceated.
[2022-07-15T23:23:46Z INFO  wine_pipeline] Random sample created with size 50000000!
[2022-07-15T23:23:48Z INFO  wine_pipeline] Aggregated result calculated.
[2022-07-15T23:23:48Z INFO  wine_pipeline] Pipeline executed successfully!
[2022-07-15T23:23:48Z INFO  wine_pipeline] CPU Execution time: 3.18567858s
```

Logs for Pandas:
```
datapsycho@dataops:~/.../wine_pipeline$ poetry run python pysrc/main.py
[2022-07-16 01:30:58,307 root INFO] Data read successfully!
[2022-07-16 01:30:58,318 root INFO] Basic Statistics calculated.
[2022-07-16 01:30:58,325 root INFO] Mean Max Distribution of Proline calculated.
[2022-07-16 01:30:58,326 root INFO] Ration data frame is Ceated.
[2022-07-16 01:30:58,331 root INFO] Aggregated result calculated.
[2022-07-16 01:31:00,105 root INFO] Random sample created with size 50000000!
[2022-07-16 01:31:04,155 root INFO] Aggregated result calculated.
[2022-07-16 01:31:04,165 root INFO] Pipeline executed successfully!
[2022-07-16 01:31:04,165 root INFO] CPU Execution time: 5.838280569s.
```

Which shows by using Polars in Rust you can decrease the cpu time by more than 50%.

## Setup Configuration:
The cpu is used to run the pipelines is intel core i7, 8th Gen with 16 GB RAM on Linux Mint OS.
