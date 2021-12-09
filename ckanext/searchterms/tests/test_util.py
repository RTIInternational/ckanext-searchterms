"""Tests for util.py."""
import pytest
import os

import pathlib
from ckanext.searchterms.util import get_resource_file_path
from ckanext.searchterms.plugin import file_exists

def test_file_exists(app):
    """ Test the utility function `file_exists` """

    resource_id = "abcdefmytest"

    # Delete file if it exists from previous test run
    fpath = get_resource_file_path(resource_id)
    if os.path.exists(fpath):
        os.remove(fpath)

    # Verify file does not exist
    resource_id = "abcdefmytest"
    result = file_exists(resource_id, delay=0)
    assert result == False

    # Create test file at path
    dirpath = fpath.rsplit("/", 1)[0]
    pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)
    testFile = open(fpath, "w")
    testFile.write("mytest")
    testFile.close()

    # Verify file does exist
    result2 = file_exists(resource_id, delay=0)
    assert result2 == True
