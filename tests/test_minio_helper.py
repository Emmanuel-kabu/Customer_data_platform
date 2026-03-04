"""
Tests for MinIO helper module.
Tests: client initialization, bucket operations, file listing, upload/download.
"""

import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


class TestMinIOClientInit:
    """Tests for MinIO client initialization."""

    @patch("minio_helper.Minio")
    def test_initializes_with_env_vars(self, mock_minio, mock_env_vars):
        """Should initialize using environment variables."""
        from minio_helper import MinIOClient
        MinIOClient()
        mock_minio.assert_called_once_with(
            "localhost:9000",
            access_key="test_user",
            secret_key="test_password",
            secure=False
        )

    @patch("minio_helper.Minio")
    def test_initializes_with_custom_params(self, mock_minio):
        """Should accept custom connection parameters."""
        from minio_helper import MinIOClient
        MinIOClient(
            endpoint="custom:9000",
            access_key="custom_key",
            secret_key="custom_secret"
        )
        mock_minio.assert_called_once_with(
            "custom:9000",
            access_key="custom_key",
            secret_key="custom_secret",
            secure=False
        )


class TestMinIOBucketOps:
    """Tests for MinIO bucket operations."""

    @patch("minio_helper.Minio")
    def test_ensure_bucket_creates_if_missing(self, mock_minio, mock_env_vars):
        """Should create bucket if it doesn't exist."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value
        mock_instance.bucket_exists.return_value = False

        client = MinIOClient()
        client.ensure_bucket("test-bucket")

        mock_instance.make_bucket.assert_called_once_with("test-bucket")

    @patch("minio_helper.Minio")
    def test_ensure_bucket_skips_if_exists(self, mock_minio, mock_env_vars):
        """Should not create bucket if it already exists."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value
        mock_instance.bucket_exists.return_value = True

        client = MinIOClient()
        client.ensure_bucket("test-bucket")

        mock_instance.make_bucket.assert_not_called()


class TestMinIOFileOps:
    """Tests for MinIO file operations."""

    @patch("minio_helper.Minio")
    def test_list_objects(self, mock_minio, mock_env_vars):
        """Should list all objects in a bucket."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value

        mock_obj1 = MagicMock()
        mock_obj1.object_name = "file1.csv"
        mock_obj2 = MagicMock()
        mock_obj2.object_name = "file2.csv"
        mock_instance.list_objects.return_value = [mock_obj1, mock_obj2]

        client = MinIOClient()
        result = client.list_objects("raw-data", prefix="sales/")

        assert len(result) == 2
        assert "file1.csv" in result

    @patch("minio_helper.Minio")
    def test_list_csv_files_filters_correctly(self, mock_minio, mock_env_vars):
        """Should return only CSV files."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value

        mock_csv = MagicMock()
        mock_csv.object_name = "data.csv"
        mock_json = MagicMock()
        mock_json.object_name = "config.json"
        mock_instance.list_objects.return_value = [mock_csv, mock_json]

        client = MinIOClient()
        result = client.list_csv_files("raw-data")

        assert len(result) == 1
        assert result[0] == "data.csv"

    @patch("minio_helper.Minio")
    def test_download_file_content(self, mock_minio, mock_env_vars):
        """Should download and return file content as string."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value

        mock_response = MagicMock()
        mock_response.read.return_value = b"col1,col2\nval1,val2"
        mock_instance.get_object.return_value = mock_response

        client = MinIOClient()
        content = client.download_file_content("raw-data", "test.csv")

        assert content == "col1,col2\nval1,val2"
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

    @patch("minio_helper.Minio")
    def test_upload_file_content(self, mock_minio, mock_env_vars):
        """Should upload string content to MinIO."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value

        client = MinIOClient()
        client.upload_file_content("raw-data", "test.csv", "col1,col2\nval1,val2")

        mock_instance.put_object.assert_called_once()
        call_args = mock_instance.put_object.call_args
        assert call_args.kwargs["bucket_name"] == "raw-data" or call_args[1].get("bucket_name") == "raw-data"

    @patch("minio_helper.Minio")
    def test_list_objects_handles_error(self, mock_minio, mock_env_vars):
        """Should return empty list on error."""
        from minio_helper import MinIOClient
        from minio.error import S3Error
        mock_instance = mock_minio.return_value
        mock_instance.list_objects.side_effect = S3Error(
            "NoSuchBucket", "Bucket not found", "", "", "", ""
        )

        client = MinIOClient()
        result = client.list_objects("nonexistent")
        assert result == []

    @patch("minio_helper.Minio")
    def test_get_file_metadata(self, mock_minio, mock_env_vars):
        """Should return file metadata dictionary."""
        from minio_helper import MinIOClient
        mock_instance = mock_minio.return_value

        mock_stat = MagicMock()
        mock_stat.object_name = "test.csv"
        mock_stat.size = 1024
        mock_stat.last_modified = "2025-01-01"
        mock_stat.content_type = "text/csv"
        mock_stat.etag = "abc123"
        mock_instance.stat_object.return_value = mock_stat

        client = MinIOClient()
        metadata = client.get_file_metadata("raw-data", "test.csv")

        assert metadata["object_name"] == "test.csv"
        assert metadata["size"] == 1024
        assert metadata["content_type"] == "text/csv"
