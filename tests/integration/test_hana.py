import pytest
import yaml
from datahub.ingestion.api.common import PipelineContext
from sqlalchemy import create_engine

from datahub_sap_hana.ingestion import HanaSource
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd


@pytest.fixture
def hana_source():
    with open("tests/integration/data/hana_to_file.yml") as f:
        config_file = yaml.safe_load(f)
        config_dict = config_file["source"]["config"]
        ctx = PipelineContext(run_id="hana-test")
        hana_source = HanaSource.create(config_dict, ctx)
        return hana_source


query = """SELECT 
    BASE_OBJECT_NAME as source_table, 
    BASE_SCHEMA_NAME as source_schema,
    DEPENDENT_OBJECT_NAME as dependent_view, 
    DEPENDENT_SCHEMA_NAME as dependent_schema
  from SYS.OBJECT_DEPENDENCIES 
WHERE 
  DEPENDENT_OBJECT_TYPE = 'TABLE'
  OR DEPENDENT_OBJECT_TYPE = 'VIEW'
  AND BASE_SCHEMA_NAME NOT LIKE '%SYS%'
  AND DEPENDENT_SCHEMA_NAME NOT LIKE '%SYS%' LIMIT 1
  """


def test_connection_with_query(hana_source: HanaSource):
    engine = hana_source.config.get_sql_alchemy_url()
    engine = create_engine(engine)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        assert result == [("HOTEL", "HOTEL", "ROOM", "HOTEL")]
        assert len(result) > 0


def test_hana_ingest(pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/data"

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "hana_to_file.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], test_resources_dir
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=test_resources_dir / "hana_mces.json",
        golden_path=test_resources_dir / "hana_mces_golden.json",
    )
