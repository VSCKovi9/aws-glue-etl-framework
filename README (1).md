# aws-glue-etl-framework

A production-grade, reusable ETL framework built on AWS Glue and PySpark. Designed to eliminate boilerplate across data pipelines by providing parameterized job configuration, modular transformations, schema validation, and structured observability out of the box.

Built from real-world patterns used in enterprise data engineering for clients including World Bank and Eli Lilly.

---

## Features

- **Parameterized jobs** — drive any pipeline from a JSON config, zero code changes to onboard a new source
- **Modular PySpark transforms** — cleanse, deduplicate, type-cast, and enrich as composable functions
- **Schema validation** — detects drift between expected and actual DataFrames, alerts via SNS
- **Data quality checks** — null rate, uniqueness, row count reconciliation built into every job
- **Structured logging** — JSON-formatted CloudWatch logs queryable with Logs Insights
- **Job state tracking** — DynamoDB-backed checkpoints for idempotent reruns
- **CI/CD ready** — GitHub Actions workflows for automated testing and Glue job deployment
- **Infrastructure as code** — Terraform templates for all AWS resources

---

## Architecture

```
Source Systems
     │
     ▼
[ Raw Layer ]        s3://bucket/raw/{source}/{date}/
     │               Unmodified source data — Parquet / JSON / CSV
     │
     ▼  (raw_ingestion_job.py)
[ Staging Layer ]    s3://bucket/staging/{source}/{date}/
     │               Cleansed, validated, type-cast — Parquet + partitioned
     │
     ▼  (staging_transform_job.py)
[ Curated Layer ]    Amazon Redshift
                     Analytics-ready star schema + materialized views
```

---

## Project Structure

```
aws-glue-etl-framework/
├── configs/
│   ├── base_config.json
│   ├── jobs/
│   │   ├── raw_to_staging.json
│   │   └── staging_to_curated.json
│   └── schemas/
│       ├── customers.json
│       └── transactions.json
├── src/
│   ├── glue_context.py
│   ├── config_loader.py
│   ├── transforms/
│   │   ├── cleanser.py
│   │   ├── deduplicator.py
│   │   ├── type_caster.py
│   │   └── partitioner.py
│   ├── validators/
│   │   ├── schema_validator.py
│   │   └── quality_checks.py
│   ├── connectors/
│   │   ├── s3_connector.py
│   │   ├── redshift_connector.py
│   │   └── api_connector.py
│   └── observability/
│       ├── logger.py
│       ├── metrics.py
│       └── job_tracker.py
├── jobs/
│   ├── raw_ingestion_job.py
│   ├── staging_transform_job.py
│   └── curated_load_job.py
├── tests/
│   ├── conftest.py
│   └── unit/
│       ├── test_cleanser.py
│       ├── test_schema_validator.py
│       └── test_quality_checks.py
├── infrastructure/
│   ├── glue_jobs.tf
│   └── iam_roles.tf
├── scripts/
│   └── deploy_job.sh
├── requirements.txt
└── .gitignore
```

---

## Quick Start

### Prerequisites

- Python 3.9+
- AWS CLI configured (`aws configure`)
- Terraform >= 1.3 (for infrastructure provisioning)

### Install dependencies

```bash
git clone https://github.com/VSCKovi9/aws-glue-etl-framework.git
cd aws-glue-etl-framework
pip install -r requirements.txt
```

### Run tests locally

```bash
pytest tests/unit/ -v
```

### Deploy a Glue job to AWS

```bash
bash scripts/deploy_job.sh --job staging_transform_job --env dev
```

---

## Job Configuration

Every pipeline is driven by a JSON config file. No code changes needed to onboard a new data source:

```json
{
  "job_name": "customer_raw_to_staging",
  "source": {
    "type": "s3",
    "path": "s3://my-bucket/raw/customers/",
    "format": "parquet"
  },
  "target": {
    "type": "s3",
    "path": "s3://my-bucket/staging/customers/",
    "format": "parquet",
    "partition_by": ["year", "month"]
  },
  "schema": "configs/schemas/customers.json",
  "quality_checks": {
    "null_threshold": 0.02,
    "uniqueness_keys": ["customer_id"],
    "row_count_min": 1000
  },
  "transforms": ["cleanse", "deduplicate", "cast"],
  "notifications": {
    "sns_topic_arn": "arn:aws:sns:us-east-1:123456789:etl-alerts"
  }
}
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| ETL Runtime | AWS Glue 4.0 |
| Processing | PySpark 3.3 |
| Storage | Amazon S3 |
| Warehouse | Amazon Redshift |
| Logging | AWS CloudWatch |
| State Tracking | AWS DynamoDB |
| Alerting | AWS SNS |
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Testing | pytest + moto |

---

## Author

**Venkat Sai Charan Kovi**
Cloud Data Engineer — AWS | PySpark | Redshift | Snowflake

[LinkedIn](https://linkedin.com/in/venkat-sai-charan-kovi-a1b851385) · [Email](mailto:kovisaicharan99@gmail.com)

---

## License

MIT License — see [LICENSE](LICENSE) for details.
