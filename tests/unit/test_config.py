from datahub_sap_hana.ingestion import HanaConfig

scheme = "test_table"
table = "logs"


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


def test_database_alias_takes_precendence():
    config = HanaConfig.parse_obj(
        {
            **_base_config(),
            "database_alias": "ops_database",
            "database": "hana",
        }
    )
    assert config.get_identifier(scheme, table) == "ops_database.test_table.logs"


def test_database_in_identifier():
    config = HanaConfig.parse_obj({**_base_config(), "database": "test_db"})
    assert config.get_identifier(scheme, table) == "test_db.test_table.logs"
