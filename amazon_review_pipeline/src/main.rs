use std::sync::Arc;

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // let file_path = "datalayers/landing/Toys_and_Games_5.json";
    let file_path = "datalayers/landing/test_file.json";
    let df_ = read_data(file_path.to_string()).await?;
    df_.show().await?;
    Ok(())
}

async fn read_data(path: String) -> datafusion::error::Result<Arc<DataFrame>> {
    let mut ctx = SessionContext::new();
    let selected_columns = vec!["asin", "vote", "verified", "unixReviewTime", "reviewText"];
    let df_ = ctx.read_json(path, NdJsonReadOptions::default()).await?;
    let df_ = df_.select_columns(&selected_columns)?;
    Ok(df_)
}