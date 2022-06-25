# Data Pipeline in Rust (Polars DataFrame)

The repository data pipeline code written in Rust (Polars) and Python (Pandas). The main intend was to write data pipelin in Rust instead of python. But to compare the cpu time peformance gain the same pipeline is also writen in Pandas. 

Each directory in the project is a separate data pipeline. The smaller datasets are included along with the pipeline and only if it has free to use license. If the data set is big there will be a bash script added into the `datastore` directory of the particular project to download the data.

## Project Structure:
Each directory in the root is a separate data pipeline. Inside of the directory where will be:
- A Rust Project setup (cargo.toml) which you can use to build the project
- A Poetry Python Project Setup (pyproject.toml) to create your virtual environment
- pysrc folder contains the data pipeline written in Python
- src folder contains the data pipeline written in Rust
- Makefile is used to wrap the frequently used bash command but not necessary

## Contribution
If you want to add more data pipeline or some complex pipeline helpful for others feel free to fork and send a pr request. **But only condition are you pipeline must be written in Polars or other Rust dataframe framework. Do not use python binding for Polars, it is about democratizing Rust over Python**

## Pipelines:
- `wine_pipeline` is a small data pipeline written for the famous wine data which as three different class of wine and other measurements as feature variable. The data set is quite popular in ML community.