"""High-level Metabase provisioning workflow."""

from __future__ import annotations

import logging
from typing import List

from .catalog import load_question_catalog, missing_question_ids
from .client import MetabaseClient
from .settings import load_settings
from .specs import DASHBOARD_SPECS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _validate_question_references(question_ids: dict) -> None:
    referenced: List[str] = []
    for dashboard in DASHBOARD_SPECS:
        for card in dashboard.cards:
            referenced.extend(card.question_ids)

    missing = missing_question_ids(question_ids, referenced)
    if missing:
        raise ValueError(f"Dashboard specs reference missing question IDs: {', '.join(sorted(set(missing)))}")


def provision_metabase_assets() -> None:
    settings = load_settings()
    catalog = load_question_catalog(settings.question_catalog_path)
    _validate_question_references(catalog["by_id"])

    client = MetabaseClient(settings)
    client.wait_for_ready()
    client.setup_if_needed()
    client.login()

    database_id = client.ensure_database()
    client.sync_schema(database_id)
    field_index = client.build_field_index(database_id)

    root_collection_id = client.ensure_collection_path([settings.root_collection_name])
    logger.info("Using root collection id=%s", root_collection_id)

    for dashboard in DASHBOARD_SPECS:
        collection_path = [settings.root_collection_name] + dashboard.collection_path
        collection_id = client.ensure_collection_path(collection_path)

        dashboard_id = client.ensure_dashboard(
            name=dashboard.name,
            description=dashboard.description,
            collection_id=collection_id,
        )
        client.set_dashboard_parameters(
            dashboard_id=dashboard_id,
            parameters=dashboard.parameters,
            name=dashboard.name,
            description=dashboard.description,
        )

        prepared_cards = []
        for card in dashboard.cards:
            card_id = client.upsert_card(
                name=card.name,
                description=card.description,
                query=card.query,
                display=card.display,
                database_id=database_id,
                collection_id=collection_id,
                template_tags=card.template_tags,
                field_index=field_index,
            )
            prepared_cards.append((card_id, card.layout, card.mappings))

        client.ensure_dashboard_cards(dashboard_id, prepared_cards)
        logger.info("Provisioned dashboard: %s", dashboard.name)

    repair_summary = client.repair_all_parameter_ids()
    logger.info(
        "Global Metabase parameter repair complete: dashboards_fixed=%s cards_fixed=%s",
        repair_summary["dashboards_fixed"],
        repair_summary["cards_fixed"],
    )

    logger.info(
        "Metabase provisioning complete. Catalog version=%s updated_at=%s",
        catalog.get("version"),
        catalog.get("updated_at"),
    )
