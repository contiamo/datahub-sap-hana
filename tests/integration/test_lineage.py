import pytest
import yaml
from datahub.ingestion.api.common import PipelineContext

from datahub_sap_hana.ingestion import HanaSource

@pytest.fixture
def ctx():
    return PipelineContext(run_id="hana-test")

@pytest.fixture
def config():
    return {
        "username": "HOTEL",
        "password": "Localdev1",
        "database": "HXE",
        "host_port": "localhost:39041",
        "include_view_lineage": True,
        "include_column_lineage": True,
        "schema_pattern": {
            "allow": [
                "HOTEL"
            ],
            "ignoreCase": True
        },
        "profile_pattern": {
            "allow": [
                "HOTEL"
            ]
        }
    }

@pytest.mark.db
def test_get_view_definitions(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    view_definitions = list(source.get_column_lineage_view_definitions(conn))

    assert len(view_definitions) == 5
    assert [view.name for view in view_definitions] == [
        "available_rooms_by_hotel",
        "flat_hotel_rooms",
        "highest_price",
        "reservation_count",
        "total_rooms_price",
    ]

    assert all([view.sql for view in view_definitions])
    assert all([view.schema == "hotel" for view in view_definitions])


@pytest.mark.db
def test_get_column_lineage(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    lineages = list(source.get_column_view_lineage_elements(conn))

    assert len(lineages) == 5
    assert [x[0].name for x in lineages] == [
        "available_rooms_by_hotel",
        "flat_hotel_rooms",
        "highest_price",
        "reservation_count",
        "total_rooms_price",
    ]


@pytest.mark.db
def test_create_column_lineage(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    lineages = list(source.get_column_view_lineage_elements(conn))

    # TODO
    assert len(lineages) == 5

@pytest.mark.db
def test_get_column_lineage_workunits(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    workunits = list(source._get_column_lineage_workunits(conn))

    #TODO
    assert len(workunits) == 5
    