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
datapsycho@dataops:~/.../wine_pipeline$ make run_python 
poetry run python pysrc/main.py
[2022-06-25 07:32:38,737 root INFO] Data read successfully!
[2022-06-25 07:32:38,749 root INFO] Basic Statistics calculated.
[2022-06-25 07:32:38,755 root INFO] Mean Max Distribution of Proline calculated.
[2022-06-25 07:32:38,755 root INFO] Ration data frame is Ceated.
[2022-06-25 07:32:38,766 root INFO] Aggregated result calculated.
[2022-06-25 07:32:40,464 root INFO] Random sample created with size 50000000!
[2022-06-25 07:32:43,934 root INFO] Aggregated result calculated.
[2022-06-25 07:32:43,947 root INFO] Pipeline executed successfully!
[2022-06-25 07:32:43,947 root INFO] CPU Execution time: 5.2120559360000005s.
```

Logs for Pandas:
```
datapsycho@dataops:~/.../wine_pipeline$ poetry run python pysrc/main.py 
[2022-06-25 07:30:49,166 root INFO] Data read successfully!
[2022-06-25 07:30:49,176 root INFO] Basic Statistics calculated.
[2022-06-25 07:30:49,181 root INFO] Mean Max Distribution of Proline calculated.
[2022-06-25 07:30:49,181 root INFO] Ration data frame is Ceated.
[2022-06-25 07:30:49,186 root INFO] Aggregated result calculated.
[2022-06-25 07:30:50,873 root INFO] Random sample created with size 50000000!
[2022-06-25 07:30:54,290 root INFO] Aggregated result calculated.
[2022-06-25 07:30:54,302 root INFO] Pipeline executed successfully!
[2022-06-25 07:30:54,302 root INFO] CPU Execution time: 5.1388861630000005s.
```

Which shows by using Polars in Rust you can decrease the cpu time by more than 50%.

## Setup Configuration:
The cpu is used to run the pipelines is intel core i7, 8th Gen with 16 GB RAM on Linux Mint OS.
