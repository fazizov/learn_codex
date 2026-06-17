# learn_codex

## Databricks Bronze Lakeflow Declarative Pipeline

This repository includes a Lakeflow Declarative Pipeline source file that reads
Databricks sample sales-order CSV files from `/databricks-datasets` and writes
them to a Bronze streaming table.

### Pipeline source

- `pipelines_bronze_orders.py`

### Databricks usage

1. In Databricks, create a new **Lakeflow Declarative Pipeline**.
2. Add `pipelines_bronze_orders.py` as a Python source file.
3. Choose the target catalog and schema where the Bronze table should be
   published.
4. Run the pipeline.

The pipeline creates the managed streaming table
`bronze_sample_sales_orders` with `quality = bronze` table metadata. It uses
Auto Loader to incrementally read CSV files from:

```text
/databricks-datasets/retail-org/sales_orders/
```

The Bronze table preserves the sample dataset columns and adds ingestion
metadata columns:

- `_bronze_ingested_at`
- `_bronze_source_file`
