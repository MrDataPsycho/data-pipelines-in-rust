from pathlib import Path
import pandas as pd
import logging
import time


pd.set_option('display.max_rows', 500)
logging.basicConfig(format='[%(asctime)s %(name)s %(levelname)s] %(message)s', level=logging.INFO)

DATASTORE = Path(Path.cwd()).joinpath("datastore")


def read_csv_into_df(path: Path, filename: str) -> pd.DataFrame:
    """
    Read files by given a filepath and filename
    param: path: Path of the main data directory
    param: filename: Name of the wine data file to read
    """
    full_path = path.joinpath(filename)
    _df = pd.read_csv(full_path, header=None)
    columns = [
        'Class label', 'Alcohol', 'Malic acid', 'Ash',
        'Alcalinity of ash', 'Magnesium', 'Total phenols',
        'Flavanoids', 'Nonflavanoid phenols', 'Proanthocyanins',
        'Color intensity', 'Hue', 'OD280/OD315 of diluted wines',
        'Proline'
    ]
    columns = [item.lower().replace(" ", "_") for item in columns]
    _df.columns = columns
    logging.info("Data read successfully!")
    return _df

def describe_top_features(df: pd.DataFrame) -> None:
    top_feature_list = [
        "class_label",
        "proline", 
        "flavanoids", 
        "color_intensity", 
        "od280/od315_of_diluted_wines", 
        "alcohol"
    ]
    result = df[top_feature_list].describe()
    logging.info("Basic Statistics calculated.")
    total_groups = df["class_label"].unique().tolist()


def get_proline_agg_df(df: pd.DataFrame):
    group_colname = "class_label"
    features = ["class_label", "proline"]

    _df = df[features].groupby(group_colname).agg(
        max_proline=("proline", "max"),
        median_proline=("proline", "median"),
        min_proline=("proline", "mean"),
    )
    logging.info("Mean Max Distribution of Proline calculated.")
    # print(_df)


def create_arbitary_ration_df(df: pd.DataFrame):
    data = dict(
        class_label=df["class_label"],
        proline_alcohol_ratio=df["proline"]/df["alcohol"],
        flavanoids_color_ration=df["flavanoids"]/df["color_intensity"],
        od_hue_ration=df["od280/od315_of_diluted_wines"]/df["hue"],
    )
    _df = pd.DataFrame(data=data)
    logging.info("Ration data frame is Ceated")
    # print(_df.head())

def get_up_sampled_df(df: pd.DataFrame, size=100) -> pd.DataFrame:
    _df = df.sample(size, replace=True, random_state=1)
    logging.info(f"random sample created with size {len(_df)}!")
    return _df


def aggregate_features_df(df: pd.DataFrame) -> pd.DataFrame:
    groups = ["class_label"]
    agg_map = dict(
            mean_proline=("proline", "mean"),
            median_proline=("proline", "median"),
            mean_hue=("hue", "mean"),
            median_hue=("hue", "median"),
            mean_flavanoids=("flavanoids", "mean"),
            median_flavanoids=("flavanoids", "median"),
        )
    _df = df.groupby(groups).agg(**agg_map).reset_index().sort_values(groups)
    logging.info("Aggregated result:")
    # print(_df)
    return _df

    

def main():
    wine_df = read_csv_into_df(DATASTORE, "wine.data")
    describe_top_features(wine_df)
    get_proline_agg_df(wine_df)
    create_arbitary_ration_df(wine_df)
    aggregate_features_df(wine_df)
    wine_up_sampled_df = get_up_sampled_df(wine_df, 50000000)
    aggregate_features_df(wine_up_sampled_df)


if __name__ == "__main__":
    st = time.process_time()
    main()
    et = time.process_time()
    res = et - st
    logging.info("Program executed successfully!")
    logging.info(f'CPU Execution time: {res} seconds.')
    