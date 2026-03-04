"""Environment-backed settings for Metabase provisioning."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MetabaseSettings:
    metabase_url: str
    admin_email: str
    admin_password: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    db_name: str
    root_collection_name: str
    question_catalog_path: str


def load_settings() -> MetabaseSettings:
    return MetabaseSettings(
        metabase_url=os.getenv("METABASE_URL", "http://localhost:3001").rstrip("/"),
        admin_email=os.getenv("METABASE_ADMIN_EMAIL", "admin@cdp.local"),
        admin_password=os.getenv("METABASE_ADMIN_PASSWORD", "CDPAdmin2026!"),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "customer_data_platform"),
        postgres_user=os.getenv("POSTGRES_USER", "cdp_admin"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "cdp_secure_pass_2026"),
        db_name=os.getenv("METABASE_DB_NAME", "CDP Warehouse"),
        root_collection_name=os.getenv("METABASE_ROOT_COLLECTION", "CDP Business Intelligence"),
        question_catalog_path=os.getenv("BUSINESS_QUESTION_CATALOG", "config/business_questions.yml"),
    )
