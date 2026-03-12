"""
schema_validator.py
Validates a PySpark DataFrame against an expected schema definition.
Detects new columns, dropped columns, and type mismatches (schema drift).
Publishes an SNS alert if drift is detected and a topic ARN is configured.
"""
 
import json
import logging
import boto3
from dataclasses import dataclass, field
from typing import List, Optional
from pyspark.sql import DataFrame
 
logger = logging.getLogger(__name__)
 
 
@dataclass
class ValidationResult:
    is_valid: bool
    new_columns: List[str] = field(default_factory=list)
    missing_columns: List[str] = field(default_factory=list)
    type_mismatches: List[dict] = field(default_factory=list)
 
    @property
    def drift_report(self) -> str:
        lines = ["Schema Drift Report", "=" * 40]
        if self.new_columns:
            lines.append(f"New columns (not in schema): {self.new_columns}")
        if self.missing_columns:
            lines.append(f"Missing columns (expected but absent): {self.missing_columns}")
        if self.type_mismatches:
            for m in self.type_mismatches:
                lines.append(
                    f"Type mismatch on '{m['column']}': "
                    f"expected {m['expected']}, got {m['actual']}"
                )
        if self.is_valid:
            lines.append("Result: PASSED - No drift detected")
        else:
            lines.append("Result: FAILED - Schema drift detected")
        return "\n".join(lines)
 
 
class SchemaValidator:
    """
    Validates a DataFrame against a JSON schema definition.
 
    Schema JSON format:
    {
        "fields": [
            {"name": "customer_id", "type": "string", "nullable": false},
            {"name": "created_at",  "type": "timestamp", "nullable": true}
        ]
    }
    """
 
    # Maps JSON schema type names to Spark type strings
    TYPE_MAP = {
        "string": "StringType",
        "integer": "IntegerType",
        "long": "LongType",
        "double": "DoubleType",
        "float": "FloatType",
        "boolean": "BooleanType",
        "timestamp": "TimestampType",
        "date": "DateType",
    }
 
    def __init__(
        self,
        expected_schema_path: str,
        sns_topic_arn: Optional[str] = None,
        region: str = "us-east-1"
    ):
        self.sns_topic_arn = sns_topic_arn
        self.region = region
 
        with open(expected_schema_path, "r") as f:
            schema_def = json.load(f)
 
        self.expected_fields = {
            field["name"]: field for field in schema_def.get("fields", [])
        }
 
    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Compare DataFrame schema against the expected schema.
        Returns a ValidationResult with any drift details.
        """
        actual_fields = {f.name: f for f in df.schema.fields}
 
        expected_names = set(self.expected_fields.keys())
        actual_names = set(actual_fields.keys())
 
        new_columns = list(actual_names - expected_names)
        missing_columns = list(expected_names - actual_names)
        type_mismatches = []
 
        for col_name in expected_names & actual_names:
            expected_type = self.TYPE_MAP.get(
                self.expected_fields[col_name]["type"], ""
            )
            actual_type = type(actual_fields[col_name].dataType).__name__
 
            if expected_type and expected_type != actual_type:
                type_mismatches.append({
                    "column": col_name,
                    "expected": expected_type,
                    "actual": actual_type
                })
 
        is_valid = not (new_columns or missing_columns or type_mismatches)
        result = ValidationResult(
            is_valid=is_valid,
            new_columns=new_columns,
            missing_columns=missing_columns,
            type_mismatches=type_mismatches
        )
 
        if not is_valid:
            logger.warning(result.drift_report)
            if self.sns_topic_arn:
                self._publish_alert(result)
        else:
            logger.info("Schema validation passed — no drift detected")
 
        return result
 
    def _publish_alert(self, result: ValidationResult) -> None:
        """Publish a schema drift alert to an SNS topic."""
        try:
            sns = boto3.client("sns", region_name=self.region)
            sns.publish(
                TopicArn=self.sns_topic_arn,
                Subject="[ETL ALERT] Schema drift detected",
                Message=result.drift_report
            )
            logger.info(f"Schema drift alert published to SNS: {self.sns_topic_arn}")
        except Exception as e:
            logger.error(f"Failed to publish SNS alert: {e}")
