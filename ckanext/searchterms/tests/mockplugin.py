import ckan.plugins as p
from ckanext.searchterms.interfaces import ISearchterms
import pandas as pd


class SearchtermsMockPlugin(p.SingletonPlugin):
    """
    This plugin is
    - made available as `searchterms_mock_plugin` by the entrypoints list in setup.py
    - included in the plugin list in test_plugin.py
    It demonstrates how to use the Searchterms plugin.
    """

    p.implements(ISearchterms)

    def is_eligible_for_searchterms(self, resource):
        return True

    def calculate_stats(self, resource):
        return pd.DataFrame(["Dummy", "List"])

    # More methods to be added later
