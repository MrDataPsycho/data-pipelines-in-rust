use polars::prelude::*;
use polars::datatypes::DataType::{Int64, Float64};
use std::path::{Path, PathBuf};
use std::time::Instant;
use log::info;

fn read_csv_into_df(path: PathBuf) -> Result<DataFrame> {
    let schema = Schema::from(vec![
        Field::new("class_label", Int64),
        Field::new("alcohol", Float64),
        Field::new("malic_acid", Float64),
        Field::new("ash", Float64),
        Field::new("alcalinity_of_ash", Float64),
        Field::new("magnesium", Float64),
        Field::new("total_phenols", Float64),
        Field::new("flavanoids", Float64),
        Field::new("nonflavanoid_phenols", Float64),
        Field::new("color_intensity", Float64),
        Field::new("hue", Float64),
        Field::new("od280/od315_of_diluted_wines", Float64),
        Field::new("proline", Float64),
    ]);
    CsvReader::from_path(path)?.has_header(false).with_schema(&schema).finish()
}


fn describe_top_features(df: &DataFrame){
    let top_feature_vec = vec![
        "class_label",
        "proline", 
        "flavanoids", 
        "color_intensity", 
        "od280/od315_of_diluted_wines", 
        "alcohol"
    ];
    let _df = df.select(top_feature_vec).unwrap();
    info!("Basic Statistics");
    println!("{}", &_df.describe(None));
}

fn get_proline_agg_df(df: &DataFrame){
    let group_colname = ["class_label"];
    let _df = df
        .groupby(group_colname)
        .unwrap()
        .agg(&[("proline", &["mean", "median", "max"])])
        .unwrap();
    info!("Mean Max Distribution of Proline");
    println!("{}", _df)
}


// fn create_arbitary_ration_df(df: &DataFrame){
//     let selection_list = [
//         "class_label", "proline", "alcohol", "flavanoids", 
//         "color_intensity", "od280/od315_of_diluted_wines", "hue"
//     ];
//     let mut _df = df.select(selection_list).unwrap();
//     _df.with_column(["proline", _df.column("")]);
// }


    

fn main() {
    env_logger::init();
    let start = Instant::now();
    let curr_path = Path::new("main.rs").parent();
    let file_path = curr_path.unwrap().join("datastore").join("wine.data");
    let result = read_csv_into_df(file_path);
    match result {
        Ok(wine_df) => {
            describe_top_features(&wine_df);
            get_proline_agg_df(&wine_df);
        },  // println!("{:?}", content.head(Some(10)))
        Err(error) => panic!("Problem reading file: {:?}", error),
    }
    let duration = start.elapsed();
    info!("Time elapsed in execution is: {:?}", duration);
    info!("Time elapsed in execution (seconds) is: {:?}", duration.as_secs());
}
