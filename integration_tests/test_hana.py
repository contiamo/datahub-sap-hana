import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from datahub_sap_hana.ingestion import HanaConfig


@pytest.fixture(scope="module")
def engine():
    config = HanaConfig(
        username="system",
        password="HXEHana1",
        host_port="localhost:39017",
        scheme="hana",
    )
    engine = create_engine(config.get_sql_alchemy_url())
    return engine


def test_connection_engine(engine):
    try:
        with engine.connect() as conn:
            conn.execute("""SELECT * FROM SCHEMAS LIMIT 1""")
    except OperationalError as e:
        pytest.fail(f"Could not connect to database: {e}")


def test_connection_db(engine):
    with engine.connect() as conn:
        assert conn is not None
