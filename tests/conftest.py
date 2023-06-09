import logging
import os
import pytest

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.putenv("DATAHUB_DEBUG", "1")

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")


def pytest_addoption(parser):
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )
    parser.addoption("--copy-output-files", action="store_true", default=False)


def pytest_configure(config):
    # Automatically skip integration tests if no mark is specified
    markexpr = config.getoption("markexpr", None)
    if not markexpr:
        print("No markexpr specified, skipping integration tests")
        markexpr = "not integration"

    config.option.markexpr = markexpr


def pytest_collection_modifyitems(items, config):
    # Automatically mark tests as integration test if it starts with `test_integration`
    for item in items:
        if "test_integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
