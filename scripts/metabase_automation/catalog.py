"""Business question catalog loader and basic validation helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import yaml


def load_question_catalog(path: str) -> Dict[str, dict]:
    catalog_path = Path(path)
    if not catalog_path.exists():
        raise FileNotFoundError(f"Business question catalog not found: {catalog_path}")

    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    questions = payload.get("questions", [])
    by_id = {q["id"]: q for q in questions if isinstance(q, dict) and q.get("id")}
    return {
        "version": payload.get("version", "unknown"),
        "updated_at": payload.get("updated_at", ""),
        "questions": questions,
        "by_id": by_id,
    }


def missing_question_ids(known_ids: Dict[str, dict], referenced_ids: List[str]) -> List[str]:
    return [qid for qid in referenced_ids if qid not in known_ids]
