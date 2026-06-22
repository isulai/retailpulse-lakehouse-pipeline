from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum


CUSTOMERS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_batch/customers"
ORDERS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_batch/orders"
PRODUCTS_PATH = "/Volumes/workspace/retailpulse_project/retailpulse_batch/products"


@dp.table
def bronze_customers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(CUSTOMERS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
        )
    )


@dp.table
def bronze_orders():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(ORDERS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
        )
    )


@dp.table
def bronze_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(PRODUCTS_PATH)
        .select(
            "*",
            current_timestamp().alias("ingestion_ts"),
        )
    )


@dp.table
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_name", "name IS NOT NULL")
def silver_customers():
    return (
        spark.read.table("workspace.retailpulse_project.bronze_customers")
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumnRenamed("name", "customer_name")
    )


@dp.table
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
def silver_orders():
    return (
        spark.read.table("workspace.retailpulse_project.bronze_orders")
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("order_date", col("order_date").cast("date"))
    )


@dp.table
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_price", "price >= 0")
def silver_products():
    return (
        spark.read.table("workspace.retailpulse_project.bronze_products")
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("price", col("price").cast("double"))
    )


@dp.materialized_view
def silver_sales_enriched():
    customers_df = spark.read.table("workspace.retailpulse_project.silver_customers")
    orders_df = spark.read.table("workspace.retailpulse_project.silver_orders")
    products_df = spark.read.table("workspace.retailpulse_project.silver_products")

    return (
        orders_df.join(customers_df, "customer_id")
        .join(products_df, "product_id")
        .withColumn("revenue", col("quantity") * col("price"))
    )


@dp.materialized_view
def gold_city_revenue():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched")
        .groupBy("city")
        .agg(sum("revenue").alias("total_revenue"))
    )


@dp.materialized_view
def gold_product_revenue():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched")
        .groupBy("product_name")
        .agg(sum("revenue").alias("total_revenue"))
    )


@dp.materialized_view
def gold_top_customers():
    return (
        spark.read.table("workspace.retailpulse_project.silver_sales_enriched")
        .groupBy("customer_name")
        .agg(sum("revenue").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )
