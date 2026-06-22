from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum


CUSTOMERS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_stream/customers"
ORDERS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_stream/orders"
PRODUCTS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_stream/products"


@dp.table
def bronze_customers_st():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(CUSTOMERS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
            col("_metadata.file_name").alias("source_file"),
            col("_metadata.file_modification_time").alias("file_modified_ts"),
        )
    )


@dp.table
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL")
def silver_customers_st():
    return (
        spark.readStream.table("workspace.retailpulse_project.bronze_customers_st")
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumnRenamed("name", "customer_name")
    )


@dp.table
def bronze_orders_st():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(ORDERS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
            col("_metadata.file_name").alias("source_file"),
            col("_metadata.file_modification_time").alias("file_modified_ts"),
        )
    )


@dp.table
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
def silver_orders_st():
    return (
        spark.readStream.table("workspace.retailpulse_project.bronze_orders_st")
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("order_date", col("order_date").cast("date"))
    )


@dp.table
def bronze_products_st():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(PRODUCTS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
            col("_metadata.file_name").alias("source_file"),
            col("_metadata.file_modification_time").alias("file_modified_ts"),
        )
    )


@dp.table
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_price", "price >= 0")
def silver_products_st():
    return (
        spark.readStream.table("workspace.retailpulse_project.bronze_products_st")
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("price", col("price").cast("double"))
    )


@dp.materialized_view
def silver_sales_enriched_st_mv():
    customers_df = spark.read.table(
        "workspace.retailpulse_project.silver_customers_st"
    ).drop("_rescued_data", "source_file", "file_modified_ts", "ingestion_ts")

    orders_df = spark.read.table("workspace.retailpulse_project.silver_orders_st").drop(
        "_rescued_data", "source_file", "file_modified_ts", "ingestion_ts"
    )

    products_df = spark.read.table(
        "workspace.retailpulse_project.silver_products_st"
    ).drop("_rescued_data", "source_file", "file_modified_ts", "ingestion_ts")

    return (
        orders_df.join(customers_df, "customer_id")
        .join(products_df, "product_id")
        .withColumn("revenue", col("quantity") * col("price"))
    )


@dp.materialized_view
def gold_city_revenue_st_mv():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched_st_mv")
        .groupBy("city")
        .agg(sum("revenue").alias("total_revenue"))
    )


@dp.materialized_view
def gold_product_revenue_st_mv():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched_st_mv")
        .groupBy("product_name")
        .agg(sum("revenue").alias("total_revenue"))
    )


@dp.materialized_view
def gold_top_customers_st_mv():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched_st_mv")
        .groupBy("customer_name")
        .agg(sum("revenue").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )
