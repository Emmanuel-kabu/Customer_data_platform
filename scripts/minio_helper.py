"""
MinIO Helper - Utilities for interacting with MinIO object storage.
"""

import os
import logging
from io import BytesIO
from typing import List

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOClient:
    """Wrapper around MinIO client with CDP-specific operations."""

    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        secure: bool = False
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minio_admin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_pass_2026")

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure
        )
        logger.info(f"MinIO client initialized: {self.endpoint}")

    def ensure_bucket(self, bucket_name: str):
        """Create bucket if it doesn't exist."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")

    def list_objects(self, bucket: str, prefix: str = "", recursive: bool = True) -> List[str]:
        """List all objects in a bucket with optional prefix filter."""
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=recursive)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Failed to list objects in {bucket}/{prefix}: {e}")
            return []

    def list_csv_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List only CSV files in a bucket."""
        all_objects = self.list_objects(bucket, prefix)
        return [obj for obj in all_objects if obj.lower().endswith(".csv")]

    def download_file_content(self, bucket: str, object_name: str) -> str:
        """Download file content as string."""
        try:
            response = self.client.get_object(bucket, object_name)
            content = response.read().decode("utf-8")
            response.close()
            response.release_conn()
            logger.info(f"Downloaded {bucket}/{object_name} ({len(content)} bytes)")
            return content
        except S3Error as e:
            logger.error(f"Failed to download {bucket}/{object_name}: {e}")
            raise

    def upload_file_content(self, bucket: str, object_name: str, content: str,
                            content_type: str = "text/csv"):
        """Upload string content to MinIO."""
        try:
            data = content.encode("utf-8")
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=BytesIO(data),
                length=len(data),
                content_type=content_type
            )
            logger.info(f"Uploaded {bucket}/{object_name} ({len(data)} bytes)")
        except S3Error as e:
            logger.error(f"Failed to upload {bucket}/{object_name}: {e}")
            raise

    def move_to_archive(self, source_bucket: str, object_name: str,
                        archive_bucket: str = "archive-data"):
        """Move processed file to archive bucket."""
        try:
            self.ensure_bucket(archive_bucket)

            # Copy to archive
            from minio.commonconfig import CopySource
            self.client.copy_object(
                archive_bucket,
                object_name,
                CopySource(source_bucket, object_name)
            )

            # Remove from source
            self.client.remove_object(source_bucket, object_name)
            logger.info(f"Archived {source_bucket}/{object_name} -> {archive_bucket}/{object_name}")

        except S3Error as e:
            logger.error(f"Failed to archive {object_name}: {e}")
            raise

    def get_file_metadata(self, bucket: str, object_name: str) -> dict:
        """Get metadata for an object."""
        try:
            stat = self.client.stat_object(bucket, object_name)
            return {
                "object_name": stat.object_name,
                "size": stat.size,
                "last_modified": str(stat.last_modified),
                "content_type": stat.content_type,
                "etag": stat.etag,
            }
        except S3Error as e:
            logger.error(f"Failed to get metadata for {bucket}/{object_name}: {e}")
            return {}
