from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr


CDC_SOURCE_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_cdc"


@dp.table
def bronze_customers_cdc_st():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(CDC_SOURCE_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
            col("_metadata.file_name").alias("source_file"),
            col("_metadata.file_modification_time").alias("file_modified_ts"),
        )
    )


@dp.table
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE')")
def silver_customers_cdc_st():
    return (
        spark.readStream.table("workspace.retailpulse_project.bronze_customers_cdc_st")
        .select(
            "customer_id",
            "name",
            "city",
            "operation",
            "updated_at",
            "source_file",
            "file_modified_ts",
            "ingestion_ts",
        )
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )


dp.create_streaming_table("silver_customers_scd1")

dp.create_auto_cdc_flow(
    target="silver_customers_scd1",
    source="silver_customers_cdc_st",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    stored_as_scd_type=1,
)


dp.create_streaming_table("silver_customers_scd2")

dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="silver_customers_cdc_st",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    stored_as_scd_type=2,
)
