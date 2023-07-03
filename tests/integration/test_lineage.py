import pytest
from datahub.ingestion.api.common import PipelineContext
from serde.json import to_json
from sqlalchemy import inspect

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
        "schema_pattern": {"allow": ["HOTEL"], "ignoreCase": True},
        "profile_pattern": {"allow": ["HOTEL"]},
    }


@pytest.mark.db
def test_get_view_definitions(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()
    inspector = inspect(conn)

    view_definitions = list(source.get_column_lineage_view_definitions(inspector))

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
    inspector = inspect(conn)

    lineages = list(source.get_column_view_lineage_elements(inspector))

    assert len(lineages) == 5
    assert [x[0].name for x in lineages] == [
        "available_rooms_by_hotel",
        "flat_hotel_rooms",
        "highest_price",
        "reservation_count",
        "total_rooms_price",
    ]

    # SELECT
    # "H"."HNO" ,
    # "H"."NAME" ,
    # "H"."ADDRESS" ,
    # "H"."CITY" ,
    # "H"."STATE" ,
    # "H"."ZIP" ,
    # "R"."TYPE" ,
    # "R"."PRICE" ,
    # "R"."FREE"
    #  FROM HOTEL.ROOM AS R
    # LEFT JOIN HOTEL.HOTEL AS H
    #   ON H.HNO=R.HNO'

    flat_hotel_rooms = lineages[1]
    column_lineage = flat_hotel_rooms[1]
    assert len(column_lineage) == 9, f"found: {to_json(column_lineage)}"
    assert column_lineage[0][0].name == "hno"
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "flat_hotel_rooms"
    assert column_lineage[1][0].name == "name"
    assert column_lineage[2][0].name == "address"
    assert column_lineage[3][0].name == "city"
    assert column_lineage[4][0].name == "state"
    assert column_lineage[5][0].name == "zip"
    assert column_lineage[6][0].name == "type"
    assert column_lineage[7][0].name == "price"
    assert column_lineage[8][0].name == "free"

    upstream_field_names = [
        "hno",
        "name",
        "address",
        "city",
        "state",
        "zip",
        "type",
        "price",
        "free",
    ]
    upstreams = [x[1][0].name for x in column_lineage]
    assert upstreams == upstream_field_names, f"{upstreams}"

    # SELECT
    #   H.NAME,
    #   H.CITY,
    #   R.TYPE,
    #   COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE
    # FROM
    #   HOTEL.ROOM AS R
    # LEFT JOIN
    #   HOTEL.HOTEL AS H
    # ON H.HNO = R.HNO
    # GROUP BY
    #   H.NAME,
    #   H.CITY,
    #   R.TYPE,
    #   R.PRICE'

    total_rooms_price = lineages[4]
    column_lineage = total_rooms_price[1]
    assert column_lineage[0][0].name == "name"
    assert column_lineage[1][0].name == "city"
    assert column_lineage[2][0].name == "type"
    assert column_lineage[3][0].name == "total_room_price"
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "total_rooms_price"

    upstream_field_names = ["name", "city", "type", "price"]
    upstreams = [x[1][0].name for x in column_lineage]
    assert upstreams == upstream_field_names, f"{upstreams}"
