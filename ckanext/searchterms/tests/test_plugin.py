"""Tests for plugin.py."""
import pytest

from ckanext.searchterms.implementations import (
    is_eligible,
    # calculate_stats,
)


@pytest.mark.ckan_config("ckan.plugins", "searchterms")
@pytest.mark.usefixtures("with_plugins")
def test_searchterms_by_itself(app):
    """
    Verify that summarystats get_eligibility_func() raises an error without an extension
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
    Verify that summarystats get_eligibility_func() retrieves a custom is_eligible()
    method from a plugin that extends the ISearchterms interface
    """
    resource = None
    assert is_eligible(resource) is True
