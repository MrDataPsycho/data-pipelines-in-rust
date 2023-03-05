use polars::{prelude::*};
use log::info;
use env_logger;


fn main() {
    env_logger::init();
    let df = read_diabetes_dataset();
    run_pipeline(df);
}


fn read_diabetes_dataset() -> DataFrame{
    let path = "data/raw/diabetes.csv";
    let df = LazyCsvReader::new(path.to_string()).finish().expect("Can not rea the Data.");
    info!("Here is a glimps of the data!");
    println!("{:?}", df.clone().limit(3).collect());
    df.collect().unwrap()
}


fn filter_zero_values(df: DataFrame) -> DataFrame {
    let result = df
        .clone()
        .lazy()
        .filter(col("Glucose").gt(0))
        .filter(col("BloodPressure").gt(0))
        .filter(col("SkinThickness").gt(0))
        .filter(col("Insulin").gt(0))
        .filter(col("BMI").gt(0))
        .filter(col("Age").gt(0))
        .filter(col("*").is_not_null())
        ;
    info!("Filters zero applied to lazy frame!");
    result.collect().unwrap()
}

fn select_relevant_columns(df: DataFrame) -> DataFrame {
    let col_list = [
        "Pregnancies",
        "Glucose", 
        "BloodPressure", 
        "SkinThickness", 
        "Insulin", 
        "BMI", 
        "Age",
        "Outcome"
    ];
    info!("Only relevant columns are selected!");
    df.select(col_list).unwrap()
}

fn impute_zero_with_mean(df: DataFrame, col_name: &str) -> DataFrame {
    let musk = df.column(col_name).unwrap().gt(0).unwrap(); 
    let col_mean = df
        .column(col_name)
        .unwrap()
        .filter(&musk)
        .unwrap()
        .mean()
        .unwrap();


    let predicate = when(col(col_name).lt_eq(0.0))
        .then(lit(col_mean))
        .otherwise(col(col_name))
        .alias(col_name);
    let result = df.lazy().with_column(predicate);
    info!("Imputed zero value for column {}", col_name);
    result.collect().unwrap()

}

fn apply_imputation(df: DataFrame) -> DataFrame {
    let df = impute_zero_with_mean(df, "Glucose");
    let df = impute_zero_with_mean(df, "BloodPressure");
    let df = impute_zero_with_mean(df, "SkinThickness");
    let df = impute_zero_with_mean(df, "Insulin");
    let df = impute_zero_with_mean(df, "BMI");
    let df = impute_zero_with_mean(df, "Age");
    info!("Imputation applied for all columns");
    df
}

// fn apply_filter(df: DataFrame) -> DataFrame{
//     let result = df.lazy()
//         .filter(col("Pregnancies").is_not_null())
//         .filter(col("Outcome").is_not_null())
//         .collect()
//         .unwrap();
//     info!("Any non-null value is removed from the data set by pregnancies, outcome columns.");
//     result
// }

fn run_pipeline(df: DataFrame) {
    let write_path = "data/processed/diabetes.csv";
    let mut file = std::fs::File::create(write_path).unwrap();
    info!("Row count before processing. {:?}", df.shape());
    let  df = select_relevant_columns(df);
    let df = apply_imputation(df);
    // let df = apply_filter(df);
    // info!("Here is a glimps of the data!");
    // println!("{:?}", df.clone().lazy().limit(3).collect());
    let mut df = filter_zero_values(df);
    // let df2: DataFrame = df.describe(None);
    // println!("{:?}", df2);
    info!("Row count after processing. {:?}", df.shape());
    info!("Column schema changed to {:?}", df.get_column_names());
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
    info!("File written successfully into {}", write_path);
}

// Temporary Code
    // let schema = Schema::new(vec![
    //     Field::new("Pregnancies", DataType::Int32, false),
    //     Field::new("Glucose", DataType::Int32, false),
    //     Field::new("BloodPressure", DataType::Int32, false),
    //     Field::new("SkinThickness", DataType::Int32, false),
    //     Field::new("Insulin", DataType::Int32, false),
    //     Field::new("BMI", DataType::Float32, false),
    //     Field::new("DiabetesPedigreeFunction", DataType::Float32, false),
    //     Field::new("Age", DataType::Int32, false),
    //     Field::new("Outcome", DataType::Int8, false),
    // ]);
    // let csv_read_optoins = CsvReadOptions{schema: Some(&schema), ..Default::default()};
    // let ctx = SessionContext::new();
    // let df = ctx.read_csv(path.to_string(), csv_read_optoins).await.unwrap();
    // info!("File read successfully into {}", path);
    // df