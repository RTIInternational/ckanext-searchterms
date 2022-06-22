"""Tests for plugin.py."""
import pytest
import pandas as pd

from ckan.tests import factories

from ckanext.searchterms.implementations import (
    is_eligible,
    # get_searchterms,
)
from ckanext.searchterms.jobs import check_search_terms_resource


@pytest.mark.ckan_config("ckan.plugins", "searchterms")
@pytest.mark.usefixtures("with_plugins")
def test_searchterms_by_itself(app):
    """
    Verify that searchterms get_eligibility_func() raises an error without an extension
    that implements the interface
    """
    resource = None
    with pytest.raises(Exception) as excinfo:
        is_eligible(resource)
    assert "No plugin implementing ISearchterms was found." in str(excinfo.value)


@pytest.mark.ckan_config("ckan.plugins", "searchterms searchterms_mock_plugin")
@pytest.mark.usefixtures("with_plugins")
def test_mock_plugin_is_eligible(app):
    """
    Verify that searchterms get_searchterms_func() retrieves a custom get_searchterms()
    method from a plugin that extends the ISearchterms interface
    """
    resource = None
    assert is_eligible(resource) is True


@pytest.mark.ckan_config("ckan.plugins", "searchterms searchterms_mock_plugin")
@pytest.mark.usefixtures("with_plugins")
def test_searchterms_are_saved(app):
    """
    Verify end-to-end resource's searchterms are saved as a new resource
    """

    dataset = factories.Dataset()
    resource = factories.Resource(package_id=dataset["id"])
    searchterms_df = check_search_terms_resource(resource)
    assert searchterms_df["Term_1"].tolist() == ["Apple", "Orange"]
