use polars::prelude::*;


fn main() -> PolarsResult<()> {
    let schema = Schema::from(vec![
        Field::new("asin", DataType::Utf8),
        Field::new("image", DataType::Utf8),
        Field::new("overall", DataType::Float64),
        Field::new("reviewText", DataType::Utf8),
        Field::new("reviewTime", DataType::Utf8),
        Field::new("reviwerID", DataType::Utf8),
        Field::new("reviewerName", DataType::Utf8),
        Field::new("style", DataType::Utf8),
        Field::new("summary", DataType::Utf8),
        Field::new("unixReviewTime", DataType::Int64),
        Field::new("verified", DataType::Boolean),
        Field::new("vote",  DataType::Float64)
    ]);
    let file_path = "datalayers/landing/Toys_and_Games_5.json";
    let df = match LazyJsonLineReader::new(file_path.into())
        .with_schema(schema)
        .finish() {
        Ok(lf) => lf,
        Err(e) => panic!("Error: {}", e),
    }
    .limit(5)
    .collect();
    println!("{:?}", df);
    Ok(())
}