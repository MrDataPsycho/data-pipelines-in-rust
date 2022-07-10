use datafusion::logical_plan::{to_timestamp_seconds, when};
use datafusion::prelude::date_part;
use datafusion::prelude::*;
use env_logger;
use log::info;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    let start = Instant::now();
    let file_path = "datalayers/landing/Toys_and_Games_5.json";
    // let file_path = "datalayers/landing/test_file.json";
    let df = read_data(file_path.to_string()).await?;
    let processed_df = add_processed_columns(&df).await?;
    let insights_df = prepare_aggregated_insights(&processed_df).await?;
    processed_df
        .write_csv("datalayers/analytics/toys_n_game")
        .await?;
    info!("Data Written successfully in analytics layer!");
    insights_df
        .write_csv("datalayers/insights/toys_n_game")
        .await?;
    info!("Data Written successfully in analytics layer!");
    // processed_df.limit(None, Some(4))?.show().await?;
    // insights_df.limit(None, Some(4))?.show().await?;
    let duration = start.elapsed();
    info!{"Pipeline executed successfully!"}
    info!("Pipeline Execution time: {:?}", duration);
    Ok(())
}

async fn read_data(path: String) -> datafusion::error::Result<Arc<DataFrame>> {
    let mut ctx = SessionContext::new();
    let selected_columns = vec![
        "asin",
        "vote",
        "verified",
        "unixReviewTime",
        "reviewTime",
        "reviewText",
    ];
    let df_ = ctx.read_json(path, NdJsonReadOptions::default()).await?;
    let df_ = df_.select_columns(&selected_columns)?;

    let processed_columns = vec![
        col("asin"),
        coalesce(vec![col("vote"), lit("0")]).alias("vote"),
        col("verified"),
        to_timestamp_seconds(col("unixReviewTime")).alias("reviewed_at"),
        coalesce(vec![col("reviewText"), lit("")]).alias("review_text"),
        length(coalesce(vec![col("reviewText"), lit("")])).alias("review_text_len"),
        date_part(lit("year"), to_timestamp_seconds(col("unixReviewTime"))).alias("reviewed_year"),
        date_part(lit("month"), to_timestamp_seconds(col("unixReviewTime")))
            .alias("reviewed_month"),
    ];
    let df_ = df_.select(processed_columns)?;
    info!("Data loading plan created successfully!");
    Ok(df_)
}

async fn add_processed_columns(df: &Arc<DataFrame>) -> datafusion::error::Result<Arc<DataFrame>> {
    let text_len_category = when(col("review_text_len").gt_eq(lit(200)), lit("long"))
        .when(
            col("review_text_len")
                .gt(lit(10))
                .and(col("review_text_len").lt(lit(200))),
            lit("medium"),
        )
        .when(
            col("review_text_len")
                .gt(lit(1))
                .and(col("review_text_len").lt_eq(lit(10))),
            lit("short"),
        )
        .otherwise(lit("invalid"))?;

    // let is_voted = when(col("vote").eq(lit(0)), lit("no")).otherwise(lit("yes"))?;

    let selected_col = vec![
        col("asin"),
        // col("vote"),
        // is_voted.alias("is_voted"),
        col("verified"),
        col("reviewed_at"),
        col("review_text"),
        col("review_text_len"),
        text_len_category.alias("review_text_ctg"),
        col("reviewed_year"),
        col("reviewed_month"),
    ];
    let _df = df.select(selected_col)?;
    info!("Plan for processed layer created successfully!");
    _df.repartition(Partitioning::RoundRobinBatch(20))
}

async fn prepare_aggregated_insights(
    df: &Arc<DataFrame>,
) -> datafusion::error::Result<Arc<DataFrame>> {
    let _df = df
        .filter(col("review_text_len").gt(lit(0)))?
        .aggregate(
            vec![col("asin"), col("reviewed_year"), col("reviewed_month")],
            vec![
                count(col("asin")).alias("total_review"),
                // sum(col("vote")).alias("total_vote"),
            ],
        )?
        .sort(vec![
            col("reviewed_year").sort(true, false),
            col("reviewed_month").sort(true, false),
        ])?;
    info!("Plan for Aggregate Layer created successfully!");
    _df.repartition(Partitioning::Hash(vec![col("reviewed_year")], 12))
}
