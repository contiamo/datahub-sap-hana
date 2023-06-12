# This is copied and slightly modified from the acryl-datahub project
# Copyright 2015 LinkedIn Corp. All rights reserved.
# The original source code can be found at https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/test_helpers/fs_helpers.py
# And the original Apache 2 license is available at https://github.com/acryldata/datahub/blob/master/LICENSE


import contextlib
import os
import pathlib
from typing import Iterator


@contextlib.contextmanager
def isolated_filesystem(temp_dir: pathlib.Path) -> Iterator[None]:
    cwd = os.getcwd()

    os.chdir(temp_dir)
    try:
        yield
    finally:
        os.chdir(cwd)
