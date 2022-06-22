from pathlib import Path
import pandas as pd
import logging


pd.set_option('display.max_rows', 500)
logging.basicConfig(format='=== %(asctime)s::%(name)s::%(levelname)s:: %(message)s', level=logging.INFO)

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
    logging.info("Basic Statistics")
    print(result)
    total_groups = df["class_label"].unique().tolist()
    logging.info(f"Class Labels {total_groups}")


def get_proline_agg_df(df: pd.DataFrame):
    group_colname = "class_label"
    features = ["class_label", "proline"]
    feature_agg_max = df[features].groupby(group_colname).max().reset_index().rename(columns={"proline": "max_proline"})
    feature_agg_min = df[features].groupby(group_colname).min().reset_index().rename(columns={"proline": "min_proline"})
    feature_agg_median = df[features].groupby(group_colname).median().reset_index().rename(columns={"proline": "median_proline"})
    _df = (
        feature_agg_min
        .merge(feature_agg_median, on="class_label")
        .merge(feature_agg_max, on="class_label")
    )
    logging.info("Mean Max Distribution of Proline")
    print(_df)


def create_arbitary_ration_df(df: pd.DataFrame):
    data = dict(
        class_label=df["class_label"],
        proline_alcohol_ratio=df["proline"]/df["alcohol"],
        flavanoids_color_ration=df["flavanoids"]/df["color_intensity"],
        od_hue_ration=df["od280/od315_of_diluted_wines"]/df["hue"],
    )
    _df = pd.DataFrame(data=data)
    logging.info("Ration data frame is Ceated")
    print(_df.head())


if __name__ == "__main__":
    wine_df = read_csv_into_df(DATASTORE, "wine.data")
    describe_top_features(wine_df)
    get_proline_agg_df(wine_df)
    create_arbitary_ration_df(wine_df)
