"""
staging_transform_job.py
AWS Glue job: Raw S3 -> Staging S3
 
Reads raw data, applies cleansing and validation transforms,
runs data quality checks, and writes partitioned Parquet to the staging layer.
 
Usage (deployed to AWS Glue):
    Triggered via Glue console, EventBridge schedule, or Step Functions.
    Job parameters passed via --config_path argument.
 
Local usage (for development):
    python jobs/staging_transform_job.py --config_path configs/jobs/raw_to_staging.json
"""
 
import sys
import argparse
import logging
 
# AWS Glue imports — available in the Glue runtime environment
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    GLUE_RUNTIME = True
except ImportError:
    # Running locally without Glue — use standard PySpark
    from pyspark.sql import SparkSession
    GLUE_RUNTIME = False
 
from src.config_loader import ConfigLoader
from src.transforms.cleanser import Cleanser
from src.transforms.deduplicator import Deduplicator
from src.validators.schema_validator import SchemaValidator
from src.validators.quality_checks import QualityChecker
from src.connectors.s3_connector import S3Connector
from src.observability.logger import ETLLogger
from src.observability.job_tracker import JobTracker
 
logging.basicConfig(level=logging.INFO)
 
 
def main():
    # ------------------------------------------------------------------ #
    # 1. Bootstrap Spark / Glue context
    # ------------------------------------------------------------------ #
    if GLUE_RUNTIME:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_path"])
        sc = SparkContext()
        glue_ctx = GlueContext(sc)
        spark = glue_ctx.spark_session
        job = Job(glue_ctx)
        job.init(args["JOB_NAME"], args)
        run_id = args["JOB_RUN_ID"]
        config_path = args["config_path"]
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config_path", required=True)
        local_args = parser.parse_args()
        spark = SparkSession.builder.appName("staging_transform_job").getOrCreate()
        run_id = "local-run"
        config_path = local_args.config_path
 
    # ------------------------------------------------------------------ #
    # 2. Load job configuration
    # ------------------------------------------------------------------ #
    config = ConfigLoader(config_path).load()
    job_name = config["job_name"]
 
    etl_log = ETLLogger(job_name=job_name, run_id=run_id)
    tracker = JobTracker(
        table_name="etl_job_tracker",
        job_name=job_name,
        run_id=run_id
    )
 
    tracker.start()
    etl_log.info("job_started", config_path=config_path)
 
    try:
        # -------------------------------------------------------------- #
        # 3. Read raw data from S3
        # -------------------------------------------------------------- #
        s3 = S3Connector(spark)
        source_cfg = config["source"]
 
        df = s3.read(
            path=source_cfg["path"],
            fmt=source_cfg.get("format", "parquet")
        )
        rows_read = df.count()
        etl_log.info("source_read", path=source_cfg["path"], rows=rows_read)
 
        # -------------------------------------------------------------- #
        # 4. Schema validation
        # -------------------------------------------------------------- #
        if "schema" in config:
            validator = SchemaValidator(
                expected_schema_path=config["schema"],
                sns_topic_arn=config.get("notifications", {}).get("sns_topic_arn")
            )
            result = validator.validate(df)
            if not result.is_valid:
                etl_log.warning("schema_drift_detected", report=result.drift_report)
 
        # -------------------------------------------------------------- #
        # 5. Apply transforms
        # -------------------------------------------------------------- #
        transforms = config.get("transforms", ["cleanse"])
 
        if "cleanse" in transforms:
            df = Cleanser.trim_whitespace(df)
            df = Cleanser.standardize_nulls(df)
            df = Cleanser.normalize_column_names(df)
            etl_log.info("cleanse_complete")
 
        if "deduplicate" in transforms:
            dedup_cfg = config.get("dedup", {})
            if dedup_cfg.get("partition_by") and dedup_cfg.get("order_by"):
                df = Deduplicator.window_dedup(
                    df,
                    partition_by=dedup_cfg["partition_by"],
                    order_by=dedup_cfg["order_by"]
                )
            else:
                df = Cleanser.remove_duplicates(df)
            etl_log.info("dedup_complete")
 
        # -------------------------------------------------------------- #
        # 6. Data quality checks
        # -------------------------------------------------------------- #
        if "quality_checks" in config:
            checker = QualityChecker(config["quality_checks"])
            qc_result = checker.run(df)
            etl_log.info("quality_checks_complete", passed=qc_result.passed)
 
            if not qc_result.passed:
                raise ValueError(f"Data quality checks failed:\n{qc_result.summary()}")
 
        # -------------------------------------------------------------- #
        # 7. Write to staging layer
        # -------------------------------------------------------------- #
        target_cfg = config["target"]
        rows_out = df.count()
 
        s3.write(
            df=df,
            path=target_cfg["path"],
            fmt=target_cfg.get("format", "parquet"),
            mode=target_cfg.get("mode", "overwrite"),
            partition_by=target_cfg.get("partition_by")
        )
 
        etl_log.info("target_write_complete", path=target_cfg["path"], rows=rows_out)
        tracker.succeed(rows_read=rows_read, rows_written=rows_out)
        etl_log.info("job_complete", rows_read=rows_read, rows_written=rows_out)
 
    except Exception as e:
        etl_log.error("job_failed", error=str(e))
        tracker.fail(error_message=str(e))
        raise
 
    finally:
        if GLUE_RUNTIME:
            job.commit()
 
 
if __name__ == "__main__":
    main()
