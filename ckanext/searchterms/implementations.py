import ckan.plugins as p
from ckanext.searchterms.interfaces import ISearchterms

"""
These methods are getters for the actual implementations in the consuming plugin.

Plugins should add their implementations using the interface
`ckanext.searchterms.interfaces.ISearchterms`
"""


def is_eligible(resource):
    """
    Calls a function, `is_eligible(resource) -> boolean`
    that is used to determine if search terms should be calculated for the
    given resource
    """

    eligibility_func = None
    for plugin in p.PluginImplementations(ISearchterms):
        if hasattr(plugin, "is_eligible_for_searchterms"):
            eligibility_func = plugin.is_eligible_for_searchterms

    if eligibility_func is None:
        raise Exception("No plugin implementing ISearchterms was found.")

    return eligibility_func(resource)


def get_terms(resource, dataset, existing_terms=None):
    """
    Calls a function, `get_searchterms(resource, dataset, existing_terms) -> DataFrame`
    that returns a dataframe of search terms
    """

    searchterms_func = None
    for plugin in p.PluginImplementations(ISearchterms):
        if hasattr(plugin, "get_searchterms"):
            searchterms_func = plugin.get_searchterms

    if searchterms_func is None:
        raise Exception("No plugin implementing ISearchterms was found.")

    return searchterms_func(resource, dataset, existing_terms)
