from ckan.plugins.interfaces import Interface
import pandas as pd


class ISearchterms(Interface):
    """
    Interface to define custom search terms usage
    """

    def is_eligible_for_searchterms(self, resource):
        u"""
        Returns a boolean that is used to determine if search terms should be
        generated for the given resource.
        """
        return False

    def get_searchterms(self, resource, dataset, existing_terms):
        u"""
        Returns a pandas.DataFrame with columns containing terms.
        Multiple columns denotes synonyms.
        """
        return pd.DataFrame(["Dummy", "List"])
