# Architecture

RetailPulse follows a medallion architecture using Bronze, Silver, and Gold layers. The project includes batch, streaming, and CDC patterns so the same retail domain can be explored through multiple Databricks pipeline styles.

## End-to-End Lakehouse

```mermaid
flowchart LR
    A["Raw CSV files"] --> B["Unity Catalog Volumes"]
    B --> C["Auto Loader or batch read"]
    C --> D["Bronze Delta tables"]
    D --> E["Silver standardized tables"]
    E --> F["Gold aggregate tables"]
    F --> G["Databricks SQL"]
    F --> H["Analytics consumers"]
```

## Batch Pipeline

```mermaid
flowchart TB
    subgraph Bronze["Bronze"]
        B1["bronze_customers"]
        B2["bronze_orders"]
        B3["bronze_products"]
    end

    subgraph Silver["Silver"]
        S1["silver_customers"]
        S2["silver_orders"]
        S3["silver_products"]
        S4["silver_sales_enriched"]
    end

    subgraph Gold["Gold"]
        G1["gold_city_revenue"]
        G2["gold_product_revenue"]
        G3["gold_top_customers"]
    end

    B1 --> S1 --> S4
    B2 --> S2 --> S4
    B3 --> S3 --> S4
    S4 --> G1
    S4 --> G2
    S4 --> G3
```

## Streaming Pipeline

```mermaid
flowchart LR
    A["CSV landing paths"] --> B["Auto Loader"]
    B --> C["Bronze streaming tables"]
    C --> D["Silver streaming tables"]
    D --> E["Sales enriched materialized view"]
    E --> F["Gold revenue materialized views"]
```

## CDC Pipeline

```mermaid
flowchart LR
    A["customers_cdc.csv"] --> B["Bronze CDC stream"]
    B --> C["Silver CDC stream"]
    C --> D["create_auto_cdc_flow"]
    D --> E["SCD Type 1 target"]
    D --> F["SCD Type 2 target"]
```

## Data Quality Strategy

- Bronze keeps data close to the original source with ingestion metadata.
- Silver applies schema standardization, casts, validation, and quality expectations.
- Gold stores business-facing aggregations ready for BI and dashboarding.

## Operational Metadata

Streaming and CDC ingestion add metadata columns for observability:

- `ingestion_ts`
- `source_file`
- `file_modified_ts`
