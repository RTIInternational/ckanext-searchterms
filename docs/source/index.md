[ckanext-searchterms documentation master file]: <> (This is a comment, it will not be included)

# ckanext-searchterms


```{toctree}
---
maxdepth: 2
---

```

[GitHub](https://github.com/RTIInternational/ckanext-searchterms)

This CKAN extension allows plugins to create search terms for a dataset that meets certain criteria. These searchterms are then uploaded to the dataset and indexed by Solr.

## Requirements

This plugin is compatible with CKAN 2.9 or later.

## Installation

```
pip install -e "git+https://github.com/RTIInternational/ckanext-searchterms.git#egg=ckanext-searchterms"
```

## Usage

This extension is not standalone but meant to be extended by your own CKAN plugin using the two provided interfaces.


**Example searchterms usage in a plugin**

```
from ckanext.searchterms.interfaces import ISearchterms

class MyPlugin(plugins.SingletonPlugin):
    plugins.implements(ISearchterms)

    def is_eligible_for_searchterms(self, resource):
        u"""
        Returns a boolean that is used to determine if search terms should be
        generated for the given resource.
        """
        # Some criteria
        if dataset.get("data_type") == "math":
            return True
        else:
            return False

    def get_searchterms(self, resource, dataset, existing_terms):
        u"""
        Returns a pandas.DataFrame with columns containing terms.
        Multiple columns denotes synonyms.
        """
        return pd.DataFrame(["Dummy", "List"])
```

When a dataset's resource is created or updated, searchterms will call `is_eligible` to see if it should `get_searchterms` and update.

## Schema

The searchterms plugin will set the field `searchterms_error` on the resource if there is an error. This field must be added to your dataset schema if you want it available on the resource.

```
{
    "resource_fields": [
        {
            "field_name": "searchterms_error",
            "validators": "ignore_missing"
        }
    ]
}
```

## What sort of search terms might be generated?

A simple example could be a dataset containing information about a set of people and their favorite foods. Using this plugin, you could implement a `is_eligible` function that checks if the dataset does indeed contain such data, then implement a `get_searchterms` function to parse the data for search terms, e.g. `["apples", "oranges"]`. When the user searches for `apples`, the datasets containing `apples` will be returned.
