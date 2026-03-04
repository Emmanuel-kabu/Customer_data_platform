"""
Apply or update analytics SQL models in PostgreSQL.

This script is idempotent and safe to run repeatedly.
"""

from pathlib import Path

from db_helper import get_db_connection


def apply_models(sql_file: str | None = None) -> dict:
    sql_path = Path(sql_file) if sql_file else Path(__file__).parent / "sql" / "analytics_models.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"Analytics model SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        conn.commit()
        return {"status": "success", "sql_file": str(sql_path)}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    result = apply_models()
    print(f"Applied analytics models successfully: {result['sql_file']}")
