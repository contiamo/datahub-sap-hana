from pathlib import Path

import pytest
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.sql_config import make_sqlalchemy_uri
from serde.json import to_json
from sqlalchemy import create_engine, inspect

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

    view_definitions = list(
        source.get_column_lineage_view_definitions(inspector))

    assert len(view_definitions) == 9
    assert [view.name for view in view_definitions] == [
        "available_rooms_by_hotel",
        "flat_hotel_rooms",
        "guests",
        "highest_price",
        "latest_maintenance",
        "ranked_rooms",
        "reservation_count",
        "total_rooms_price",
        "unary_rooms",
    ]

    assert all([view.sql for view in view_definitions])
    assert all([view.schema == "hotel" for view in view_definitions])


@pytest.mark.db
def test_get_column_lineage(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()
    inspector = inspect(conn)

    lineages = list(source.get_column_view_lineage_elements(inspector))

    assert len(lineages) == 9
    assert [x[0].name for x in lineages] == [
        "available_rooms_by_hotel",
        "flat_hotel_rooms",
        "guests",
        "highest_price",
        "latest_maintenance",
        "ranked_rooms",
        "reservation_count",
        "total_rooms_price",
        "unary_rooms",
    ], f"{[x[0].name for x in lineages]}"

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

    # test for a view with a *
    flat_hotel_rooms = lineages[1]
    column_lineage = flat_hotel_rooms[1]
    assert len(column_lineage) == 9, f"found: {to_json(column_lineage)}"
    assert column_lineage[0][0].name == "hno"
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "flat_hotel_rooms"
    assert column_lineage[1][0].name == "name", f"{column_lineage[1][0].name }"
    assert column_lineage[2][0].name == "address", f"{column_lineage[2][0].name }"
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
    assert upstreams == upstream_field_names, f"here: {upstreams}"

    # Now test the more complicated view "latest_maintenance", which has 3 tables + window functions
    #
    #   SELECT
    #   H.NAME,
    #   R.TYPE,
    #   R.PRICE,
    #   COUNT(R.TYPE) OVER (PARTITION BY H.NAME, R.TYPE) AS TOTAL_ROOM_COUNT,
    #   SUM(R.PRICE) OVER (PARTITION BY H.NAME) AS TOTAL_PRICE,
    #   ROW_NUMBER() OVER (PARTITION BY H.NAME ORDER BY R.PRICE DESC) AS RANK,
    #   MAX(M.DESCRIPTION) OVER (PARTITION BY H.NAME) AS LATEST_MAINTENANCE
    # FROM
    #   HOTEL.HOTEL AS H
    #   INNER JOIN HOTEL.ROOM AS R ON H.HNO = R.HNO
    #   LEFT JOIN HOTEL.RESERVATION AS RES ON R.HNO = RES.HNO AND R.TYPE = RES.TYPE
    #   LEFT JOIN HOTEL.MAINTENANCE AS M ON H.HNO = M.HNO
    # WHERE
    #   RES.RESNO IS NULL
    # GROUP BY
    #   H.NAME, R.TYPE, R.PRICE, M.DESCRIPTION
    # ORDER BY
    #   H.NAME, R.TYPE;

    latest_maintenance = lineages[4]
    column_lineage = latest_maintenance[1]

    # check that the downstream (target) columns are correct
    expected_downstreams = [
        "name",
        "type",
        "price",
        "total_room_count",
        "total_price",
        "rank",
        "latest_maintenance",
    ]
    downstreams = [x[0].name for x in column_lineage]
    assert downstreams == expected_downstreams, f"check: {downstreams}"

    # check that the view schema and name are correct
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "latest_maintenance"

    # now check that each of these columns has the correct upstream (source) columns
    upstream_field_names = [
        ["name"],  # name of hotel
        ["type"],  # type of room
        ["price"],  # price
        ["name", "type"],  # name of hotel and type of room
        ["name", "price"],  # name of hotel and price
        ["name", "price"],  # name of hotel and price
        ["description", "name"],  # maintenance description and name of hotel
    ]
    # note that we sort the upstreams because the order is not guaranteed by sqlglot
    upstreams = [sorted([source.name for source in x[1]])
                 for x in column_lineage]

    assert upstreams == upstream_field_names, f"found: {upstreams}"

    # test for view with subselect, joined with a view, and windown func

    #     SELECT
    #     H.NAME,
    #     R.TYPE,
    #     R.PRICE,
    #     R.FREE,
    #     (SELECT COUNT(*) FROM HOTEL.RESERVATION AS RES WHERE RES.HNO = H.HNO AND RES.TYPE = R.TYPE) AS NUM_RESERVATIONS,
    #     ROW_NUMBER() OVER (PARTITION BY H.NAME ORDER BY R.PRICE DESC) AS RN
    #   FROM
    #     HOTEL.HOTEL AS H
    #     JOIN HOTEL.ROOM AS R ON R.HNO = H.HNO
    #     JOIN HOTEL.CUSTOMER AS C ON C.CNO = R.HNO
    #     JOIN HOTEL.RESERVATION AS RES ON RES.HNO = H.HNO AND RES.TYPE = R.TYPE
    #     JOIN HOTEL.TOTAL_ROOMS_PRICE AS TRP ON TRP.NAME = H.NAME AND TRP.TYPE = R.TYPE
    #   WHERE
    #     C.NAME = 'Smith'
    #   ORDER BY
    #     H.NAME ASC, R.TYPE ASC;

    guests = lineages[2]
    column_lineage = guests[1]

    # check that the downstream (target) columns are correct
    expected_downstreams = [
        "name",
        "type",
        "price",
        "free",
        "num_reservations",
        "rn",
    ]
    downstreams = [x[0].name for x in column_lineage]
    assert downstreams == expected_downstreams, f"check: {downstreams}"

    # check that the view schema and name are correct
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "guests"

    # now check that each of these columns has the correct upstream (source) columns
    upstream_field_names = [
        ["name"],  # name of hotel
        ["type"],  # type of room
        ["price"],  # price of room
        ["free"],  # status of room
        # columms used in subselect to get num_reservations column
        ["hno", "hno", "type", "type"],
        ["name", "price"],  # cols from view total_room_price to get rn column
    ]
    # note that we sort the upstreams because the order is not guaranteed by sqlglot
    upstreams = [sorted([source.name for source in x[1]])
                 for x in column_lineage]

    assert upstreams == upstream_field_names, f"found: {upstreams}"


@pytest.mark.db
def test_cross_schema(testdata: Path, ctx):
    scheme = "hana"  # make this into dictionary
    username = "system"
    password = "HXEHana1"
    at = "localhost:39041"
    db = "HXE"

    # create dict for config
    config_dict = {
        "username": "system",
        "password": "HXEHana1",
        "host_port": "localhost:39041",
        "database": "HXE",
        "include_view_lineage": "true",
        "include_column_lineage": "true",
        "schema_pattern": AllowDenyPattern(allow=["RESERVATIONS"]),
    }

    test_schema_file = testdata / "test_cross_schema.sql"
    test_schema = test_schema_file.read_text()

    uri = make_sqlalchemy_uri(scheme, username, password, at, db)
    engine = create_engine(uri)
    conn = engine.connect()

    schema_results = conn.execute(
        "SELECT * FROM SCHEMAS WHERE SCHEMA_NAME = 'RESERVATIONS'"
    )
    print(schema_results)

    if schema_results.returns_rows:
        conn.execute("DROP SCHEMA RESERVATIONS CASCADE")

    conn.execute("CREATE SCHEMA RESERVATIONS")
    conn.execute(test_schema)

    assert any(
        "RESERVATIONS" in item[0] for item in schema_results
    ), f"{schema_results}"

    # source class
    source = HanaSource.create(config_dict, ctx)
    source.get_db_connection()
    inspector = inspect(conn)

    sql = 'SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE'
    view_definitions = source.get_column_lineage_view_definitions(inspector)

    for views in view_definitions:
        assert views.name == "test_cross_schema", f"{views.name}"  # pass
        assert views.schema == "reservations"  # pass
        assert views.sql == sql, f"{views.sql}"  # pass

    # notes: view from reservation schema is detected before creation of column lineage. When get_column_view_lineage elements is
    # executed, there is a Key error

    # create the dictionary for config
    # create the source class
    # test for the get_column_view_lineage
    # test for the downstream nd upstream columns
    # check for behavior of diff schemas (hotel, reservations)
