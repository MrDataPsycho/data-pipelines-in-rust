use std::ops::Add;

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // let file_path = "datalayers/landing/Toys_and_Games_5.json";
    let file_path = "datalayers/landing/test_file.json";
    // let selected_columns = vec!["asin", "vote", "verified", "unixReviewTime", "reviewText"];
    
    let mut ctx = SessionContext::new();
    let df = ctx.read_json(file_path, NdJsonReadOptions::default()).await?;
    // let df = df.select_columns(&selected_columns)?.limit(None, Some(10))?;
    let df = df.select(vec![col("a"), col("c"), col("a").add(col("c")).alias("d")])?;
    // let result = df.collect().await?;
    // let pretty_results = datafusion::arrow::util::pretty::pretty_format_batches(&result)?;
    // println!("{:?}", pretty_results.to_string());
    df.show().await?;
    Ok(())
}


// let schema = Arc::new(Schema::new(vec![
//     Field::new("asin", DataType::Utf8, false),
//     Field::new("vote", DataType::Int32, true),
//     Field::new("verified", DataType::Boolean, false),
//     Field::new("unixReviewTime", DataType::Timestamp(TimeUnit::Millisecond, None), false),
//     Field::new("reviewText", DataType::UInt8, true),
// ]));