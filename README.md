# learn_codex

## Databricks Bronze Lakeflow Declarative Pipeline

This repository includes a Lakeflow Declarative Pipeline source file that reads
Databricks sample retail CSV files from `/databricks-datasets` and writes them
to Bronze streaming tables.

### Pipeline source

- `pipelines_bronze_orders.py`

### Databricks usage

1. In Databricks, create a new **Lakeflow Declarative Pipeline**.
2. Add `pipelines_bronze_orders.py` as a Python source file.
3. Choose the target catalog and schema where the Bronze tables should be
   published.
4. Run the pipeline.

The pipeline creates these managed streaming tables with `quality = bronze`
table metadata:

- `bronze_sample_sales_orders`, which reads from:

  ```text
  /databricks-datasets/retail-org/sales_orders/
  ```

- `bronze_sample_customers`, which reads from:

  ```text
  /databricks-datasets/retail-org/customers/
  ```

Both Bronze tables preserve the sample dataset columns and add ingestion
metadata columns:

- `_bronze_ingested_at`
- `_bronze_source_file`
