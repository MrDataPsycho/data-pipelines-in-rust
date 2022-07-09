use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::logical_plan::to_timestamp_seconds;
use datafusion::prelude::date_part;
use datafusion::scalar::ScalarValue;
use log::info;
use env_logger;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    // let file_path = "datalayers/landing/Toys_and_Games_5.json";
    let file_path = "datalayers/landing/test_file.json";
    let df = read_data(file_path.to_string()).await?;
    df.show().await?;
    Ok(())
}

async fn read_data(path: String) -> datafusion::error::Result<Arc<DataFrame>> {
    let mut ctx = SessionContext::new();
    let selected_columns = vec!["asin", "vote", "verified", "unixReviewTime", "reviewTime", "reviewText"];
    let df_ = ctx
        .read_json(path, NdJsonReadOptions::default())
        .await?;
    let df_ = df_.select_columns(&selected_columns)?;

    let processed_columns = vec![
        col("asin"), 
        coalesce(vec![col("vote"), lit(0)]).alias("vote"),
        col("verified"),
        to_timestamp_seconds(col("unixReviewTime")).alias("reviewed_at"),
        coalesce(vec![col("reviewText"), lit("")]).alias("review_text"),
        length(coalesce(vec![col("reviewText"), lit("")])).alias("review_text_len"),
        date_part(Expr::Literal(ScalarValue::Utf8(Some("year".to_string()))), to_timestamp_seconds(col("unixReviewTime"))).alias("reviewed_year"),
        date_part(Expr::Literal(ScalarValue::Utf8(Some("month".to_string()))), to_timestamp_seconds(col("unixReviewTime"))).alias("reviewed_month")
        ];
    let df_ = df_.select(processed_columns)?;
    info!("Showing DataFrame schema.");
    println!("{:?}", df_.schema());
    Ok(df_)
}

