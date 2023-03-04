use polars::prelude::*;

fn main() -> PolarsResult<()> {
    file_path = "datalayers/landing/Toys_and_Games_5.ndjson";
    // let schema = Schema::from(vec![
    //     Field::new("overall", DataType::Float64),
    //     Field::new("vote", DataType::Utf8),
    //     Field::new("verified", DataType::Boolean),
    //     Field::new("reviewTime", DataType::UInt8),
    //     Field::new("reviwerID", DataType::Utf8),
    //     Field::new("asin", DataType::Utf8),
    //     Field::new("style", DataType::Utf8),
    //     Field::new("reviewerName", DataType::Utf8),
    //     Field::new("reviewText", DataType::Utf8),
    //     Field::new("helpful", DataType::List(Box::new(DataType::Int32))),
    //     Field::new("summary", DataType::Utf8),
    //     Field::new("unixReviewTime", DataType::Int64),
    // ]);
    // let df = match LazyJsonLineReader::new(file_path.into())
    //     .with_schema(schema)
    //     .finish() {
    //     Ok(lf) => lf,
    //     Err(e) => panic!("Error: {}", e),
    // }
    // .collect();
    // println!("{:?}", df);
    println!("File to read {?}", file_path);
    Ok(())
}
