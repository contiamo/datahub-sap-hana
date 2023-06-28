import pytest
from datahub.ingestion.api.common import PipelineContext
from serde.json import to_json

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

    # SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE"
    #  FROM HOTEL.ROOM AS R\n
    # LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO'

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

    # SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\n
    # FROM\n  HOTEL.ROOM AS R\n
    # LEFT JOIN\n  HOTEL.HOTEL AS H\n
    # ON H.HNO = R.HNO\n
    # GROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE'

    total_rooms_price = lineages[4]
    column_lineage = total_rooms_price[1]
    assert column_lineage[0][0].name == "name"
    assert column_lineage[1][0].name == "city"
    assert column_lineage[2][0].name == "type"
    assert column_lineage[3][0].name == "total_room_price"
    assert column_lineage[0][0].dataset.schema == "hotel"
    assert column_lineage[0][0].dataset.name == "total_rooms_price"

    upstream_field_names = [
        "name",
        "city",
        "type",
        "price"]
    upstreams = [x[1][0].name for x in column_lineage]
    assert upstreams == upstream_field_names, f"{upstreams}"


"""

@pytest.mark.db
def test_create_column_lineage(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    lineages = list(source.get_column_view_lineage_elements(
        conn))

    lineage_elements = source.get_column_view_lineage_elements(conn)

    flat_hotel_rooms = (View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO'), [(DownstreamLineageField(name='name', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free_rm_count', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='hno', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='hno', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='name', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(
        name='address', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='address', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='city', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='city', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='state', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='state', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='zip', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='zip', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='price', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='price', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))])])

    total_room_price = (View(schema='hotel', name='total_rooms_price', sql='SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE'),
                        [(DownstreamLineageField(name='name', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free_rm_count', dataset=View(schema='hotel', name='available_rooms_by_hotel', sql='SELECT\n  H.NAME,\n  R.TYPE,\n  R.FREE,\n  COUNT(R.FREE) AS FREE_RM_COUNT\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  R.TYPE, \n  R.FREE')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='hno', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='hno', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='name', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='address', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='address', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='city', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='city', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='state', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='state', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='zip', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='zip', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='price', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='price', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='free', dataset=View(schema='hotel', name='flat_hotel_rooms', sql='SELECT "H"."HNO" , "H"."NAME" , "H"."ADDRESS" , "H"."CITY" , "H"."STATE" , "H"."ZIP" , "R"."TYPE" , "R"."PRICE" , "R"."FREE" FROM HOTEL.ROOM AS R\n  LEFT JOIN HOTEL.HOTEL AS H \n  ON H.HNO=R.HNO')), [UpstreamLineageField(name='free', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(
                            name='name', dataset=View(schema='hotel', name='highest_price', sql='WITH "MAX_PRICE_CTE" AS (SELECT MAX(PRICE) AS MAX_PRICE\n  FROM HOTEL.ROOM) SELECT H.NAME, R.TYPE, R.PRICE\nFROM HOTEL.HOTEL AS H\nJOIN HOTEL.ROOM AS R ON R.HNO = H.HNO\nWHERE R.PRICE = (\n  SELECT MAX_PRICE\n  FROM MAX_PRICE_CTE\n)')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='highest_price', sql='WITH "MAX_PRICE_CTE" AS (SELECT MAX(PRICE) AS MAX_PRICE\n  FROM HOTEL.ROOM) SELECT H.NAME, R.TYPE, R.PRICE\nFROM HOTEL.HOTEL AS H\nJOIN HOTEL.ROOM AS R ON R.HNO = H.HNO\nWHERE R.PRICE = (\n  SELECT MAX_PRICE\n  FROM MAX_PRICE_CTE\n)')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='price', dataset=View(schema='hotel', name='highest_price', sql='WITH "MAX_PRICE_CTE" AS (SELECT MAX(PRICE) AS MAX_PRICE\n  FROM HOTEL.ROOM) SELECT H.NAME, R.TYPE, R.PRICE\nFROM HOTEL.HOTEL AS H\nJOIN HOTEL.ROOM AS R ON R.HNO = H.HNO\nWHERE R.PRICE = (\n  SELECT MAX_PRICE\n  FROM MAX_PRICE_CTE\n)')), [UpstreamLineageField(name='price', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='hotel name', dataset=View(schema='hotel', name='reservation_count', sql='SELECT\n  H.NAME AS "HOTEL NAME",\n  R.TYPE AS "ROOM TYPE",\n  COUNT(R.TYPE) AS "NUMBER OF RESERVATIONS",\n  AVG(R.PRICE) AS "AVERAGE ROOM PRICE"\nFROM\n  HOTEL.RESERVATION AS RES\n  JOIN HOTEL.ROOM AS R ON R.HNO = RES.HNO AND R.TYPE = RES.TYPE\n  JOIN HOTEL.HOTEL AS H ON H.HNO = RES.HNO\n  JOIN HOTEL.CUSTOMER AS C ON C.CNO = RES.CNO\nGROUP BY\n  H.NAME,\n  R.TYPE\nHAVING\n  COUNT(R.TYPE) > 5')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='room type', dataset=View(schema='hotel', name='reservation_count', sql='SELECT\n  H.NAME AS "HOTEL NAME",\n  R.TYPE AS "ROOM TYPE",\n  COUNT(R.TYPE) AS "NUMBER OF RESERVATIONS",\n  AVG(R.PRICE) AS "AVERAGE ROOM PRICE"\nFROM\n  HOTEL.RESERVATION AS RES\n  JOIN HOTEL.ROOM AS R ON R.HNO = RES.HNO AND R.TYPE = RES.TYPE\n  JOIN HOTEL.HOTEL AS H ON H.HNO = RES.HNO\n  JOIN HOTEL.CUSTOMER AS C ON C.CNO = RES.CNO\nGROUP BY\n  H.NAME,\n  R.TYPE\nHAVING\n  COUNT(R.TYPE) > 5')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='number of reservations', dataset=View(schema='hotel', name='reservation_count', sql='SELECT\n  H.NAME AS "HOTEL NAME",\n  R.TYPE AS "ROOM TYPE",\n  COUNT(R.TYPE) AS "NUMBER OF RESERVATIONS",\n  AVG(R.PRICE) AS "AVERAGE ROOM PRICE"\nFROM\n  HOTEL.RESERVATION AS RES\n  JOIN HOTEL.ROOM AS R ON R.HNO = RES.HNO AND R.TYPE = RES.TYPE\n  JOIN HOTEL.HOTEL AS H ON H.HNO = RES.HNO\n  JOIN HOTEL.CUSTOMER AS C ON C.CNO = RES.CNO\nGROUP BY\n  H.NAME,\n  R.TYPE\nHAVING\n  COUNT(R.TYPE) > 5')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='average room price', dataset=View(schema='hotel', name='reservation_count', sql='SELECT\n  H.NAME AS "HOTEL NAME",\n  R.TYPE AS "ROOM TYPE",\n  COUNT(R.TYPE) AS "NUMBER OF RESERVATIONS",\n  AVG(R.PRICE) AS "AVERAGE ROOM PRICE"\nFROM\n  HOTEL.RESERVATION AS RES\n  JOIN HOTEL.ROOM AS R ON R.HNO = RES.HNO AND R.TYPE = RES.TYPE\n  JOIN HOTEL.HOTEL AS H ON H.HNO = RES.HNO\n  JOIN HOTEL.CUSTOMER AS C ON C.CNO = RES.CNO\nGROUP BY\n  H.NAME,\n  R.TYPE\nHAVING\n  COUNT(R.TYPE) > 5')), [UpstreamLineageField(name='price', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='name', dataset=View(schema='hotel', name='total_rooms_price', sql='SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE')), [UpstreamLineageField(name='name', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='city', dataset=View(schema='hotel', name='total_rooms_price', sql='SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE')), [UpstreamLineageField(name='city', dataset=Table(schema='hotel', name='hotel'))]), (DownstreamLineageField(name='type', dataset=View(schema='hotel', name='total_rooms_price', sql='SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE')), [UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))]), (DownstreamLineageField(name='total_room_price', dataset=View(schema='hotel', name='total_rooms_price', sql='SELECT\n  H.NAME,\n  H.CITY,\n  R.TYPE,\n  COUNT(R.TYPE) * (R.PRICE) AS TOTAL_ROOM_PRICE\nFROM\n  HOTEL.ROOM AS R\n  LEFT JOIN\n  HOTEL.HOTEL AS H\n  ON H.HNO = R.HNO\nGROUP BY \n  H.NAME,\n  H.CITY,\n  R.TYPE, \n  R.PRICE')), [UpstreamLineageField(name='price', dataset=Table(schema='hotel', name='room')), UpstreamLineageField(name='type', dataset=Table(schema='hotel', name='room'))])])

    total_room_price_upstream_urns = {'urn:li:dataset:(urn:li:dataPlatform:hana,HXE.hotel.hotel,PROD)',
                                      'urn:li:dataset:(urn:li:dataPlatform:hana,HXE.hotel.room,PROD)'}
    total_room_price_downstream_urns = "urn:li:dataset:(urn:li:dataPlatform:hana,HXE.hotel.total_rooms_price,PROD)"

    flat_hotel_rooms_upstream_urns = {'urn:li:dataset:(urn:li:dataPlatform:hana,HXE.hotel.hotel,PROD)',
                                      'urn:li:dataset:(urn:li:dataPlatform:hana,HXE.hotel.room,PROD)'}

    assert len(lineages) == 5

    for ix, element in lineage_elements:
        if ix == 1:
            assert element == flat_hotel_rooms

        if ix == 4:
            assert element == total_room_price

    for _, col_lineages in lineage_elements:
        assert (len(col_lineages)) == 24

    finegrainedLineages = source.build_fine_grained_lineage(conn)
    for ix, finegrainedLineage in enumerate(finegrainedLineages):
        if ix == 4:
            assert finegrainedLineage[1] == total_room_price_upstream_urns
            # there should be 2 columns in the upstreams urns
            assert len(finegrainedLineage[1]) == 2
            assert finegrainedLineage[2] == total_room_price_downstream_urns
            assert len(finegrainedLineage[0]) == 50

        if ix == 1:
            assert len(finegrainedLineage[0]) == 9
            assert finegrainedLineage[1] == flat_hotel_rooms_upstream_urns


@pytest.mark.db
def test_get_column_lineage_workunits(config, ctx):
    source = HanaSource.create(config, ctx)
    conn = source.get_db_connection()

    workunits = list(source._get_column_lineage_workunits(conn))

    assert len(workunits) == 5
"""
