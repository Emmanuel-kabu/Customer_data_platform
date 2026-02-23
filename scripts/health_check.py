"""
Health Check Script
===================
Checks connectivity and health of all CDP services.
Can be run standalone or as part of monitoring.
"""

import os
import sys
import socket
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def check_port(host: str, port: int, timeout: int = 5) -> bool:
    """Check if a TCP port is reachable."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def check_postgres(host: str = "localhost", port: int = 5432) -> dict:
    """Check PostgreSQL connectivity."""
    reachable = check_port(host, port)
    result = {"service": "PostgreSQL", "host": host, "port": port, "reachable": reachable}

    if reachable:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=host, port=port,
                dbname=os.getenv("POSTGRES_DB", "customer_data_platform"),
                user=os.getenv("POSTGRES_USER", "cdp_admin"),
                password=os.getenv("POSTGRES_PASSWORD", "cdp_secure_pass_2026"),
                connect_timeout=5
            )
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            result["status"] = "healthy"
            result["version"] = version
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
    else:
        result["status"] = "unreachable"

    return result


def check_minio(host: str = "localhost", port: int = 9000) -> dict:
    """Check MinIO connectivity."""
    reachable = check_port(host, port)
    result = {"service": "MinIO", "host": host, "port": port, "reachable": reachable}

    if reachable:
        try:
            import requests
            resp = requests.get(f"http://{host}:{port}/minio/health/live", timeout=5)
            result["status"] = "healthy" if resp.status_code == 200 else "unhealthy"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
    else:
        result["status"] = "unreachable"

    return result


def check_airflow(host: str = "localhost", port: int = 8080) -> dict:
    """Check Airflow webserver connectivity."""
    reachable = check_port(host, port)
    result = {"service": "Airflow", "host": host, "port": port, "reachable": reachable}

    if reachable:
        try:
            import requests
            resp = requests.get(f"http://{host}:{port}/health", timeout=10)
            if resp.status_code == 200:
                health = resp.json()
                result["status"] = "healthy"
                result["scheduler"] = health.get("scheduler", {}).get("status", "unknown")
            else:
                result["status"] = "unhealthy"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
    else:
        result["status"] = "unreachable"

    return result


def check_metabase(host: str = "localhost", port: int = 3000) -> dict:
    """Check Metabase connectivity."""
    reachable = check_port(host, port)
    result = {"service": "Metabase", "host": host, "port": port, "reachable": reachable}

    if reachable:
        try:
            import requests
            resp = requests.get(f"http://{host}:{port}/api/health", timeout=10)
            if resp.status_code == 200:
                health = resp.json()
                result["status"] = health.get("status", "unknown")
            else:
                result["status"] = "unhealthy"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
    else:
        result["status"] = "unreachable"

    return result


def run_all_checks() -> dict:
    """Run health checks on all services."""
    logger.info("=" * 50)
    logger.info("CDP Health Check")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 50)

    checks = [
        check_postgres(),
        check_minio(),
        check_airflow(),
        check_metabase(),
    ]

    all_healthy = True
    for check in checks:
        status = check.get("status", "unknown")
        icon = "✅" if status == "healthy" else "❌"
        logger.info(f"{icon} {check['service']}: {status} ({check['host']}:{check['port']})")
        if status != "healthy":
            all_healthy = False
            if "error" in check:
                logger.info(f"   Error: {check['error']}")

    logger.info("=" * 50)
    logger.info(f"Overall: {'ALL HEALTHY' if all_healthy else 'ISSUES DETECTED'}")

    return {
        "timestamp": datetime.now().isoformat(),
        "all_healthy": all_healthy,
        "checks": checks
    }


if __name__ == "__main__":
    result = run_all_checks()
    sys.exit(0 if result["all_healthy"] else 1)
