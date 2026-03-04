"""Low-level Metabase API client."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .settings import MetabaseSettings

logger = logging.getLogger(__name__)


class MetabaseClient:
    def __init__(self, settings: MetabaseSettings):
        self.settings = settings
        self.session_id: Optional[str] = None

    def wait_for_ready(self, retries: int = 60, delay_seconds: int = 10) -> None:
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(f"{self.settings.metabase_url}/api/health", timeout=5)
                if response.status_code == 200 and response.json().get("status") == "ok":
                    logger.info("Metabase is healthy")
                    return
            except Exception:
                pass
            logger.info("Waiting for Metabase... (%s/%s)", attempt, retries)
            time.sleep(delay_seconds)
        raise TimeoutError("Metabase did not become ready in time")

    def _api(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
    ) -> Optional[Dict[str, Any]]:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.session_id:
            headers["X-Metabase-Session"] = self.session_id
        response = requests.request(
            method,
            f"{self.settings.metabase_url}{path}",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} failed: {response.status_code} - {response.text[:400]}")
        text = (response.text or "").strip()
        return response.json() if text else None

    @staticmethod
    def _as_list(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):  # type: ignore[unreachable]
            return payload
        if isinstance(payload.get("data"), list):
            return payload["data"]  # type: ignore[index]
        if isinstance(payload.get("items"), list):
            return payload["items"]  # type: ignore[index]
        return []

    def setup_if_needed(self) -> None:
        props = self._api("GET", "/api/session/properties")
        if not props:
            raise RuntimeError("Unable to load Metabase session properties")

        setup_token = props.get("setup-token")
        has_user_setup = props.get("has-user-setup")
        if has_user_setup or not setup_token:
            return

        payload = {
            "token": setup_token,
            "prefs": {"site_name": "Customer Data Platform", "site_locale": "en", "allow_tracking": False},
            "user": {
                "first_name": "CDP",
                "last_name": "Admin",
                "email": self.settings.admin_email,
                "password": self.settings.admin_password,
                "site_name": "Customer Data Platform",
            },
            "database": {
                "engine": "postgres",
                "name": self.settings.db_name,
                "details": {
                    "host": self.settings.postgres_host,
                    "port": self.settings.postgres_port,
                    "dbname": self.settings.postgres_db,
                    "user": self.settings.postgres_user,
                    "password": self.settings.postgres_password,
                    "ssl": False,
                    "schema-filters-type": "inclusion",
                    "schema-filters-patterns": ["warehouse", "analytics", "audit"],
                },
            },
        }
        self._api("POST", "/api/setup", payload=payload, timeout=45)
        logger.info("Metabase first-time setup complete")

    def login(self) -> None:
        payload = {"username": self.settings.admin_email, "password": self.settings.admin_password}
        response = self._api("POST", "/api/session", payload=payload)
        if not response or not response.get("id"):
            raise RuntimeError("Metabase login did not return session ID")
        self.session_id = response["id"]
        logger.info("Logged into Metabase")

    def ensure_database(self) -> int:
        databases = self._as_list(self._api("GET", "/api/database"))
        for db in databases:
            if db.get("name") == self.settings.db_name:
                return int(db["id"])

        payload = {
            "engine": "postgres",
            "name": self.settings.db_name,
            "details": {
                "host": self.settings.postgres_host,
                "port": self.settings.postgres_port,
                "dbname": self.settings.postgres_db,
                "user": self.settings.postgres_user,
                "password": self.settings.postgres_password,
                "ssl": False,
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": ["warehouse", "analytics", "audit"],
            },
        }
        created = self._api("POST", "/api/database", payload=payload)
        if not created or not created.get("id"):
            raise RuntimeError("Failed to create Metabase database connection")
        return int(created["id"])

    def sync_schema(self, database_id: int) -> None:
        self._api("POST", f"/api/database/{database_id}/sync_schema")
        logger.info("Triggered Metabase schema sync for database id=%s", database_id)

    def build_field_index(self, database_id: int) -> Dict[str, int]:
        metadata = self._api("GET", f"/api/database/{database_id}/metadata") or {}
        index: Dict[str, int] = {}
        for table in metadata.get("tables", []):
            schema = table.get("schema")
            table_name = table.get("name")
            if not schema or not table_name:
                continue
            for field in table.get("fields", []):
                fname = field.get("name")
                fid = field.get("id")
                if fname is None or fid is None:
                    continue
                index[f"{schema}.{table_name}.{fname}"] = int(fid)
        return index

    def ensure_collection_path(self, parts: List[str]) -> int:
        parent_id: Optional[int] = None
        for part in parts:
            parent_id = self._ensure_collection(part, parent_id)
        if parent_id is None:
            raise RuntimeError("Failed to resolve collection path")
        return parent_id

    def _ensure_collection(self, name: str, parent_id: Optional[int]) -> int:
        collections = self._as_list(self._api("GET", "/api/collection"))
        for item in collections:
            current_parent = item.get("location")
            target_parent_token = "/" if parent_id is None else f"/{parent_id}/"
            if item.get("name") == name and current_parent == target_parent_token:
                return int(item["id"])

        payload: Dict[str, Any] = {"name": name}
        if parent_id is not None:
            payload["parent_id"] = parent_id
        created = self._api("POST", "/api/collection", payload=payload)
        if not created or not created.get("id"):
            raise RuntimeError(f"Failed to create collection: {name}")
        return int(created["id"])

    def upsert_card(
        self,
        *,
        name: str,
        description: str,
        query: str,
        display: str,
        database_id: int,
        collection_id: int,
        template_tags: Dict[str, dict],
        field_index: Optional[Dict[str, int]] = None,
    ) -> int:
        cards = self._as_list(self._api("GET", "/api/card?f=all"))
        existing = None
        for card in cards:
            if card.get("name") == name and int(card.get("collection_id") or 0) == collection_id:
                existing = card
                break

        resolved_tags = self._resolve_template_tags(template_tags, field_index or {})

        payload = {
            "name": name,
            "description": description,
            "collection_id": collection_id,
            "dataset_query": {
                "type": "native",
                "native": {"query": query.strip(), "template-tags": resolved_tags},
                "database": database_id,
            },
            "display": display,
            "visualization_settings": {},
        }

        if existing:
            card_id = int(existing["id"])
            self._api("PUT", f"/api/card/{card_id}", payload=payload)
            return card_id

        created = self._api("POST", "/api/card", payload=payload)
        if not created or not created.get("id"):
            raise RuntimeError(f"Failed to create card: {name}")
        return int(created["id"])

    def _resolve_template_tags(self, template_tags: Dict[str, dict], field_index: Dict[str, int]) -> Dict[str, dict]:
        resolved: Dict[str, dict] = {}
        for tag_name, tag in template_tags.items():
            if tag.get("type") == "dimension" and isinstance(tag.get("dimension"), str):
                field_ref = tag["dimension"]
                field_id = field_index.get(field_ref)
                if field_id is None:
                    raise RuntimeError(f"Field filter reference not found in Metabase metadata: {field_ref}")
                copied = dict(tag)
                copied["dimension"] = ["field", field_id, None]
                resolved[tag_name] = copied
            else:
                resolved[tag_name] = tag
        return resolved

    def ensure_dashboard(self, *, name: str, description: str, collection_id: int) -> int:
        dashboards = self._as_list(self._api("GET", "/api/dashboard"))
        for dash in dashboards:
            if dash.get("name") == name:
                dashboard_id = int(dash["id"])
                self._api(
                    "PUT",
                    f"/api/dashboard/{dashboard_id}",
                    payload={"name": name, "description": description, "collection_id": collection_id},
                )
                return dashboard_id

        created = self._api(
            "POST",
            "/api/dashboard",
            payload={"name": name, "description": description, "collection_id": collection_id},
        )
        if not created or not created.get("id"):
            raise RuntimeError(f"Failed to create dashboard: {name}")
        return int(created["id"])

    def set_dashboard_parameters(self, dashboard_id: int, parameters: List[dict], name: str, description: str) -> None:
        self._api(
            "PUT",
            f"/api/dashboard/{dashboard_id}",
            payload={"name": name, "description": description, "parameters": parameters},
        )

    def ensure_dashboard_cards(
        self,
        dashboard_id: int,
        card_specs: List[Tuple[int, Dict[str, int], List[Tuple[str, str]]]],
    ) -> None:
        details = self._api("GET", f"/api/dashboard/{dashboard_id}") or {}
        existing_dashcards = details.get("dashcards", []) or []
        existing_card_ids = {int(item["card_id"]) for item in existing_dashcards if item.get("card_id") is not None}

        cards_payload: List[Dict[str, Any]] = []
        for item in existing_dashcards:
            cards_payload.append(
                {
                    "id": int(item["id"]),
                    "card_id": int(item["card_id"]),
                    "row": int(item["row"]),
                    "col": int(item["col"]),
                    "size_x": int(item["size_x"]),
                    "size_y": int(item["size_y"]),
                    "series": item.get("series") or [],
                    "parameter_mappings": item.get("parameter_mappings") or [],
                }
            )

        temp_id = -1
        for card_id, layout, mappings in card_specs:
            if card_id in existing_card_ids:
                continue

            new_card = {
                "id": temp_id,
                "card_id": card_id,
                "row": layout["row"],
                "col": layout["col"],
                "size_x": layout["sizeX"],
                "size_y": layout["sizeY"],
                "parameter_mappings": [
                    {"parameter_id": parameter_id, "card_id": card_id, "target": ["variable", ["template-tag", tag]]}
                    for parameter_id, tag in mappings
                ],
            }
            cards_payload.append(new_card)
            temp_id -= 1

        if cards_payload:
            self._api("PUT", f"/api/dashboard/{dashboard_id}/cards", payload={"cards": cards_payload})

    def repair_all_parameter_ids(self) -> Dict[str, int]:
        """
        Repair invalid/missing parameter IDs across all dashboards and cards.
        Returns a small summary count for visibility.
        """
        dashboards_fixed = 0
        cards_fixed = 0

        dashboards = self._as_list(self._api("GET", "/api/dashboard"))
        for dashboard in dashboards:
            dashboard_id = int(dashboard["id"])
            detail = self._api("GET", f"/api/dashboard/{dashboard_id}") or {}

            parameters = detail.get("parameters") or []
            normalized_parameters = []
            changed_dashboard = False

            for idx, param in enumerate(parameters):
                if not isinstance(param, dict):
                    normalized_parameters.append(param)
                    continue

                copied = dict(param)
                pid = copied.get("id")
                if not isinstance(pid, str) or not pid.strip():
                    slug = copied.get("slug") or copied.get("name") or f"param_{idx + 1}"
                    copied["id"] = str(slug).strip().lower().replace(" ", "_")
                    changed_dashboard = True
                normalized_parameters.append(copied)

            if changed_dashboard:
                self._api(
                    "PUT",
                    f"/api/dashboard/{dashboard_id}",
                    payload={
                        "name": detail.get("name"),
                        "description": detail.get("description"),
                        "parameters": normalized_parameters,
                    },
                )
                dashboards_fixed += 1

            for dashcard in detail.get("dashcards") or []:
                card = dashcard.get("card") or {}
                card_id = card.get("id")
                if not card_id:
                    continue

                card_full = self._api("GET", f"/api/card/{card_id}") or {}
                dataset_query = card_full.get("dataset_query") or {}
                changed_card = self._normalize_template_tag_ids(dataset_query)
                if not changed_card:
                    continue

                self._api(
                    "PUT",
                    f"/api/card/{card_id}",
                    payload={
                        "name": card_full.get("name"),
                        "description": card_full.get("description"),
                        "collection_id": card_full.get("collection_id"),
                        "dataset_query": dataset_query,
                        "display": card_full.get("display"),
                        "visualization_settings": card_full.get("visualization_settings") or {},
                    },
                )
                cards_fixed += 1

        return {"dashboards_fixed": dashboards_fixed, "cards_fixed": cards_fixed}

    def _normalize_template_tag_ids(self, dataset_query: Dict[str, Any]) -> bool:
        changed = False

        def _fix_tags(tags: Dict[str, Any]) -> bool:
            local_changed = False
            for key, value in tags.items():
                if not isinstance(value, dict):
                    continue
                tag_id = value.get("id")
                if not isinstance(tag_id, str) or not tag_id.strip():
                    value["id"] = str(key)
                    local_changed = True
                tag_name = value.get("name")
                if not isinstance(tag_name, str) or not tag_name.strip():
                    value["name"] = str(key)
                    local_changed = True
            return local_changed

        native = dataset_query.get("native")
        if isinstance(native, dict):
            tags = native.get("template-tags")
            if isinstance(tags, dict) and _fix_tags(tags):
                changed = True

        stages = dataset_query.get("stages")
        if isinstance(stages, list):
            for stage in stages:
                if not isinstance(stage, dict):
                    continue
                tags = stage.get("template-tags")
                if isinstance(tags, dict) and _fix_tags(tags):
                    changed = True

        return changed
