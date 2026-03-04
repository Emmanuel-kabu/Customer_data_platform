"""
Data Quality Ingestion Validator
================================
Shift-left gate that sits between data generation and MinIO upload.

Responsibilities:
    1. Run all DQ rules against generated data BEFORE upload
    2. Route clean data to raw-data bucket
    3. Route quarantined data to quarantine-data bucket
    4. Generate DQ reports as JSON to processed-data bucket
    5. Block uploads if batch-level failures detected

Usage:
    validator = IngestionValidator()
    results = validator.validate_and_upload(
        customers=customers_data,
        products=products_data,
        sales=sales_data,
    )
"""

import os
import csv
import json
import logging
from datetime import datetime
from io import StringIO, BytesIO
from typing import Dict, List, Optional, Any

from minio import Minio

from dq_rules_engine import DataQualityRulesEngine, ValidationResult

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_pass_2026")
RAW_BUCKET = "raw-data"
QUARANTINE_BUCKET = "quarantine-data"
REPORTS_BUCKET = "processed-data"


class IngestionValidator:
    """
    Validates data at ingestion time and routes to appropriate storage.

    Clean data → raw-data/
    Quarantined data → quarantine-data/
    DQ reports → processed-data/dq-reports/
    """

    def __init__(
        self,
        rules_path: Optional[str] = None,
        minio_endpoint: str = MINIO_ENDPOINT,
        minio_access_key: str = MINIO_ACCESS_KEY,
        minio_secret_key: str = MINIO_SECRET_KEY,
    ):
        self.engine = DataQualityRulesEngine(rules_path)
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ensure_buckets()

    def _ensure_buckets(self):
        """Create required buckets if they don't exist."""
        for bucket in [RAW_BUCKET, QUARANTINE_BUCKET, REPORTS_BUCKET]:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")

    def validate_and_upload(
        self,
        customers: List[Dict],
        products: List[Dict],
        sales: List[Dict],
        halt_on_any_failure: bool = False,
    ) -> Dict[str, Any]:
        """
        Validate all entities and upload to appropriate buckets.

        Args:
            customers: Customer records
            products: Product records
            sales: Sales records
            halt_on_any_failure: If True, don't upload anything if ANY entity fails

        Returns:
            Complete validation report dictionary
        """
        logger.info("=" * 70)
        logger.info("DATA QUALITY INGESTION VALIDATION — SHIFT LEFT GATE")
        logger.info("=" * 70)

        # Register reference data for referential integrity checks
        self.engine.register_reference_data("customers", customers)
        self.engine.register_reference_data("products", products)

        # Validate each entity
        results = {}
        entities = {
            "customers": customers,
            "products": products,
            "sales": sales,
        }

        all_valid = True
        for entity_name, data in entities.items():
            logger.info(f"\n{'─' * 50}")
            logger.info(f"Validating: {entity_name} ({len(data)} rows)")
            logger.info(f"{'─' * 50}")

            result = self.engine.validate(entity_name, data)
            results[entity_name] = result

            if result.batch_halted or result.quarantine_rate > 0:
                all_valid = False

        # Check if we should halt everything
        any_halted = any(r.batch_halted for r in results.values())
        if halt_on_any_failure and not all_valid:
            logger.error("[DQ] halt_on_any_failure=True and validation issues found. No uploads.")
            report = self._build_report(results, uploaded=False)
            self._upload_report(report)
            return report

        if any_halted:
            logger.error("[DQ] One or more entities had CRITICAL failures. Partial upload only.")

        # Upload clean data and quarantined data
        upload_stats = {}
        for entity_name, result in results.items():
            stats = self._process_entity_result(entity_name, result)
            upload_stats[entity_name] = stats

        # Generate and upload report
        report = self._build_report(results, uploaded=True, upload_stats=upload_stats)
        self._upload_report(report)

        self._log_final_summary(report)
        return report

    def validate_single_entity(
        self, entity_name: str, data: List[Dict]
    ) -> ValidationResult:
        """Validate a single entity without uploading. Useful for pre-checks."""
        return self.engine.validate(entity_name, data)

    # --------------------------------------------------------
    # Entity result processing
    # --------------------------------------------------------

    def _process_entity_result(
        self, entity_name: str, result: ValidationResult
    ) -> Dict:
        """Upload clean and quarantined data for a single entity."""
        stats = {
            "clean_uploaded": 0,
            "quarantined_uploaded": 0,
            "skipped": False,
        }

        if result.batch_halted:
            logger.warning(f"[DQ] {entity_name}: Batch halted — skipping upload")
            stats["skipped"] = True
            # Still quarantine everything so we can inspect
            if result.quarantined_rows or len(self._get_original_data(result)) > 0:
                all_rows = self._get_original_data(result)
                self._upload_csv(
                    QUARANTINE_BUCKET,
                    f"{entity_name}/halted_{entity_name}_{self.timestamp}.csv",
                    all_rows,
                )
                stats["quarantined_uploaded"] = len(all_rows)
            return stats

        # Upload clean rows
        if result.clean_rows:
            # Strip internal DQ metadata before upload
            clean_for_upload = self._strip_dq_metadata(result.clean_rows)
            self._upload_csv(
                RAW_BUCKET,
                f"{entity_name}/{entity_name}_{self.timestamp}.csv",
                clean_for_upload,
            )
            stats["clean_uploaded"] = len(clean_for_upload)

        # Upload quarantined rows
        if result.quarantined_rows:
            self._upload_csv(
                QUARANTINE_BUCKET,
                f"{entity_name}/quarantined_{entity_name}_{self.timestamp}.csv",
                result.quarantined_rows,  # Keep DQ metadata for inspection
            )
            stats["quarantined_uploaded"] = len(result.quarantined_rows)

        return stats

    # --------------------------------------------------------
    # Report generation
    # --------------------------------------------------------

    def _build_report(
        self,
        results: Dict[str, ValidationResult],
        uploaded: bool = True,
        upload_stats: Optional[Dict] = None,
    ) -> Dict:
        """Build comprehensive DQ report."""
        report = {
            "report_id": f"dq-report-{self.timestamp}",
            "timestamp": datetime.now().isoformat(),
            "version": "2.0",
            "approach": "shift-left",
            "uploaded": uploaded,
            "overall_status": "PASS",
            "entities": {},
        }

        total_clean = 0
        total_quarantined = 0
        total_rows = 0

        for entity_name, result in results.items():
            summary = result.get_summary()
            entity_report = {
                "summary": summary,
            }

            if result.profile:
                entity_report["profile"] = result.profile

            if upload_stats and entity_name in upload_stats:
                entity_report["upload_stats"] = upload_stats[entity_name]

            # Add top violations
            violation_details = []
            seen_rules = set()
            for v in result.violations:
                if v.rule_name not in seen_rules:
                    violation_details.append({
                        "rule": v.rule_name,
                        "type": v.rule_type,
                        "severity": v.severity.value,
                        "description": v.description,
                        "count": sum(
                            1 for vv in result.violations if vv.rule_name == v.rule_name
                        ),
                        "sample_value": str(v.actual_value)[:200] if v.actual_value else None,
                    })
                    seen_rules.add(v.rule_name)
            entity_report["violation_details"] = violation_details

            report["entities"][entity_name] = entity_report
            total_clean += summary["clean_rows"]
            total_quarantined += summary["quarantined_rows"]
            total_rows += summary["total_rows"]

            if summary["batch_halted"]:
                report["overall_status"] = "FAIL"
            elif summary["quarantined_rows"] > 0 and report["overall_status"] != "FAIL":
                report["overall_status"] = "WARN"

        report["totals"] = {
            "total_rows": total_rows,
            "total_clean": total_clean,
            "total_quarantined": total_quarantined,
            "overall_pass_rate": round(
                (total_clean / total_rows * 100) if total_rows > 0 else 100.0, 2
            ),
        }

        return report

    def _upload_report(self, report: Dict):
        """Upload DQ report as JSON to processed-data bucket."""
        report_json = json.dumps(report, indent=2, default=str)
        object_name = f"dq-reports/dq_report_{self.timestamp}.json"
        self._upload_content(REPORTS_BUCKET, object_name, report_json, "application/json")
        logger.info(f"DQ report uploaded: {REPORTS_BUCKET}/{object_name}")

    # --------------------------------------------------------
    # MinIO upload helpers
    # --------------------------------------------------------

    def _upload_csv(self, bucket: str, object_name: str, data: List[Dict]):
        """Upload list of dicts as CSV to MinIO."""
        if not data:
            return

        output = StringIO()
        # Get all possible keys (some rows may have extra DQ metadata)
        all_keys = []
        for row in data:
            for key in row.keys():
                if key not in all_keys:
                    all_keys.append(key)

        writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

        csv_content = output.getvalue()
        self._upload_content(bucket, object_name, csv_content, "text/csv")

    def _upload_content(
        self, bucket: str, object_name: str, content: str, content_type: str
    ):
        """Upload string content to MinIO."""
        data = content.encode("utf-8")
        self.minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        logger.info(f"Uploaded {bucket}/{object_name} ({len(data)} bytes)")

    # --------------------------------------------------------
    # Utility methods
    # --------------------------------------------------------

    @staticmethod
    def _strip_dq_metadata(rows: List[Dict]) -> List[Dict]:
        """Remove internal DQ fields (prefixed with _) from rows."""
        cleaned = []
        for row in rows:
            cleaned.append({k: v for k, v in row.items() if not k.startswith("_")})
        return cleaned

    @staticmethod
    def _get_original_data(result: ValidationResult) -> List[Dict]:
        """Get all original rows from a result (clean + quarantined)."""
        all_rows = []
        for row in result.clean_rows + result.quarantined_rows:
            all_rows.append({k: v for k, v in row.items() if not k.startswith("_")})
        return all_rows

    def _log_final_summary(self, report: Dict):
        """Log final human-readable summary."""
        logger.info("\n" + "=" * 70)
        logger.info("DATA QUALITY VALIDATION — FINAL SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Report ID:      {report['report_id']}")
        logger.info(f"Overall Status: {report['overall_status']}")
        logger.info(f"Total Rows:     {report['totals']['total_rows']}")
        logger.info(f"Clean Rows:     {report['totals']['total_clean']}")
        logger.info(f"Quarantined:    {report['totals']['total_quarantined']}")
        logger.info(f"Pass Rate:      {report['totals']['overall_pass_rate']}%")

        for entity, details in report["entities"].items():
            s = details["summary"]
            logger.info(f"\n  {entity.upper()}:")
            logger.info(f"    Rows: {s['total_rows']} | Clean: {s['clean_rows']} | "
                        f"Quarantined: {s['quarantined_rows']} | Pass: {s['pass_rate']}%")
            if details.get("violation_details"):
                logger.info("    Violations:")
                for v in details["violation_details"][:5]:
                    logger.info(f"      - [{v['severity']}] {v['rule']}: {v['count']} occurrences")

        logger.info("=" * 70)
