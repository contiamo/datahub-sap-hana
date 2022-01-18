import time

import pytest

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.integration
def test_mssql_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hana"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "testhanadb"
    ) as docker_services:
        time.sleep(50)
        wait_for_port(docker_services, "testhanadb", 39044)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "hana_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "-c", f"{config_file}"], tmp_path=tmp_path, check_result=True
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "hana_mces.json",
            golden_path=test_resources_dir / "hana_mces_golden.json",
        )
