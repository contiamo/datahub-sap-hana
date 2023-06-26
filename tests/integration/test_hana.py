# This is copied and slightly modified from the acryl-datahub project
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# The original source code can be found at https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/integration/hana/test_hana.py
# And the original Apache 2 license is available at https://github.com/acryldata/datahub/blob/master/LICENSE

import pytest
import yaml
from datahub.ingestion.api.common import PipelineContext

from datahub_sap_hana.ingestion import HanaSource
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd


@pytest.fixture
def hana_source():
    with open("tests/integration/data/hana_to_file_lineage_disabled.yml") as f:
        config_file = yaml.safe_load(f)
        config_dict = config_file["source"]["config"]
        ctx = PipelineContext(run_id="hana-test")
        hana_source = HanaSource.create(config_dict, ctx)
        return hana_source, conn


@pytest.mark.integration
def test_integration_hana_ingest(pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/data"
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "hana_to_file_default.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c",
            f"{config_file}"], test_resources_dir
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=test_resources_dir / "hana_mces_with_lineage_output.json",
        golden_path=test_resources_dir / "hana_mces_golden_with_lineage.json",
    )


# test for when the field include_view_lineage: false
@pytest.mark.integration
def test_integration_hana_ingest_lineage_disabled(pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/data"
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir /
                   "hana_to_file_lineage_disabled.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c",
            f"{config_file}"], test_resources_dir
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=test_resources_dir / "hana_mces_lineage_disabled_output.json",
        golden_path=test_resources_dir / "hana_mces_golden_lineage_disabled.json",
    )
