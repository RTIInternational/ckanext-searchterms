import os
import itertools
import logging
import pandas as pd
import uuid
from werkzeug.datastructures import FileStorage
import ckan.plugins.toolkit as tk
from .constants import SearchtermsParsingError
from .implementations import is_eligible, get_terms
from .util import (
    BLANK,
    SEARCHTERMS_ERROR,
    TERMS_RSRC_NAME,
    TRUE,
    get_resource_file_path,
    site_user_context,
)

log = logging.getLogger(__name__)

# If resource is eligible, add terms job to worker queue
def enqueue_terms_job(resource):
    # Check package_id exists to make sure it's not a package
    if (
        resource.get("name") != TERMS_RSRC_NAME
        and resource.get("package_id", False)
        and is_eligible(resource)
    ):
        tk.enqueue_job(
            check_search_terms_resource,
            [resource],
            rq_kwargs={"timeout": 21600},
            queue="searchterms",
        )


def enqueue_terms_update_on_delete_job(resource):
    if is_eligible(resource):
        tk.enqueue_job(
            update_search_terms_on_delete,
            [resource],
            rq_kwargs={"timeout": 21600},
            queue="searchterms",
        )


def get_existing_search_terms_df_from_csv(pkg):
    search_terms_df = None
    search_terms_resource_id = None
    for rsc in pkg.get("resources"):
        if rsc.get("name") == TERMS_RSRC_NAME:
            log.debug("Found existing searchterms resource")
            filepath = get_resource_file_path(rsc.get("id"))
            search_terms_resource_id = rsc.get("id")
            try:
                search_terms_df = pd.read_csv(filepath, sep="\t")
                # Check for old schema
                if "found_in_1" in search_terms_df.columns:
                    log.info("Old schema detected; deleting old search terms resource")
                    tk.get_action("resource_delete")(site_user_context(), rsc)
                    return None, None
            except UnicodeDecodeError:
                log.error(
                    "Existing searchterms resource is non-unicode. Deleting the resource."
                )
                tk.get_action("resource_delete")(site_user_context(), rsc)
            except FileNotFoundError:
                err_msg = (
                    "Search terms resource does not have a corresponding file to load "
                    "(File does not exist: {0}). "
                    "Deleting the resource."
                )
                log.error(err_msg.format(filepath))
                tk.get_action("resource_delete")(site_user_context(), rsc)
    return search_terms_df, search_terms_resource_id


def check_search_terms_resource(resource, resource_was_updated=False):
    """
    Check for existing searchterms, update if it exists, otherwise create it
    """

    dataset = tk.get_action("package_show")(
        site_user_context(), {"id": resource.get("package_id")}
    )
    rsrc_id = resource.get("id")
    rsrc_col = "rsrc-{}".format(rsrc_id)

    searchterms_df = None
    # Get the search terms resource as a DataFrame, if it exists
    log.info(f"Generating search terms for resource {resource.get('name')}")
    searchterms_df, _ = get_existing_search_terms_df_from_csv(dataset)
    try:
        new_terms_df = get_terms(resource, dataset, searchterms_df)
        new_terms_df = create_initial_searchterms(rsrc_col, new_terms_df)
    except SearchtermsParsingError as e:
        return add_error(dataset, str(e))
    if searchterms_df is not None:
        if resource_was_updated and rsrc_col in searchterms_df.columns:
            searchterms_df = remove_resource_from_search_terms(rsrc_col, searchterms_df)
        searchterms_df = update_searchterms(rsrc_col, new_terms_df, searchterms_df)
        delete_existing_search_terms(resource)
    else:
        searchterms_df = new_terms_df
    save_file(searchterms_df, dataset.get("id"))
    return searchterms_df


def create_initial_searchterms(rsrc_col, new_terms_df):
    """ """
    log.info("Converting output to search terms file")
    new_terms_df[rsrc_col] = "True"
    return new_terms_df


def update_searchterms(rsrc_col, new_terms_df, searchterms_df):
    """
    Merges new searchterms DataFrame into existing searchterms DataFrame.

    1) If these terms are already in the old searchterms DataFrame, _update_ them
    3) If there are new terms not existing in the old searchterms DataFrame, _append_ them
    """
    log.info("Merging new searchterms with old searchterms")

    new_terms_identifiers = get_identifiercols(new_terms_df)
    searchterms_identifiers = get_identifiercols(searchterms_df)
    ## merge lists of identifiers and deduplicate
    shared_identifiers = [
        column for column in searchterms_identifiers if column in new_terms_identifiers
    ]
    merged = searchterms_df.merge(
        new_terms_df, how="outer", on=shared_identifiers, suffixes=(None, "_drop")
    )
    dropcols = [
        column for column in merged.columns.values.tolist() if ("_drop" in column)
    ]
    merged.drop(columns=dropcols, inplace=True)
    merged.drop_duplicates(inplace=True)
    merged.fillna(value="", inplace=True)
    return merged


# This method drops the column for a given resource ID from the searchterms table
# But it first loops through all rows of the searchterms table, removing any if unused by other resources
def remove_resource_from_search_terms(rsrc_col, searchterms_df):
    log.info("Update detected - removing old search terms for this resource")
    # remove old column
    searchterms_df.drop(columns=[rsrc_col], inplace=True)
    # drop any rows that should no longer exist because that column is gone
    rsrc_cols = [
        column for column in searchterms_df.columns.values.tolist() if "rsrc" in column
    ]
    searchterms_df.dropna(axis="rows", how="all", subset=[rsrc_cols], inplace=True)
    return searchterms_df


def update_search_terms_on_delete(resource):
    log.debug("Updating searchterms because resource was deleted")
    dataset = tk.get_action("package_show")(
        site_user_context(), {"id": resource.get("package_id")}
    )

    searchterms_df, searchterms_id = get_existing_search_terms_df_from_csv(dataset)
    if searchterms_df is not None:
        searchterms_df = remove_resource_from_search_terms(
            resource.get("id"), searchterms_df
        )
        # Delete the old search_terms file
        tk.get_action("resource_delete")(site_user_context(), {"id": searchterms_id})

    # Upload the new search_terms file (that has removed the old resource ID)
    save_file(searchterms_df, dataset.get("id"))
    return searchterms_df


def save_file(searchterms_df, dataset_id):
    # Write tmp file
    log.info("Writing temporary searchterms file to disk")
    tsv_filename = os.path.join("/tmp", "searchterms-{}".format(uuid.uuid4())) + ".tsv"
    if "index" in searchterms_df.columns.values:
        searchterms_df.drop(columns=["index"], inplace=True)
    searchterms_df.fillna("", inplace=True)
    searchterms_df.to_csv(tsv_filename, sep="\t", index=False, na_rep="")

    # Upload tmp file to CKAN
    upload_to_ckan(tsv_filename, TERMS_RSRC_NAME, dataset_id)

    # Remove tmp file
    os.remove(tsv_filename)


def upload_to_ckan(filepath, name, dataset_id):
    log.info("Uploading search terms file from disk to ckan")
    with open(filepath, "rb") as file:
        resource_metadata = {
            "name": name,
            "resource_file_type": "",
            "package_id": dataset_id,
            "upload": FileStorage(file),
        }
        # TODO: move the file to its destination on the filesystem instead of uploading to the server
        # Possibly by implementing get_resource_uploader()
        rsrc = tk.get_action("resource_create")(site_user_context(), resource_metadata)
        log.info(
            "Created search terms resource {} for dataset {}".format(
                rsrc.get("id"), dataset_id
            )
        )

    updatedPackage = tk.get_action("package_show")(
        site_user_context(), {"id": dataset_id}
    )
    updatedPackage[SEARCHTERMS_ERROR] = BLANK
    final = tk.get_action("package_update")(site_user_context(), updatedPackage)
    return final


def delete_existing_search_terms(resource):
    pkg = tk.get_action("package_show")(
        site_user_context(), {"id": resource.get("package_id")}
    )
    for res in pkg.get("resources"):
        if res.get("name") == TERMS_RSRC_NAME:
            tk.get_action("resource_delete")(site_user_context(), res)


def add_error(dataset, e):
    errormessage = "Unable to process your data file for search. Error: {}".format(e)
    dataset[SEARCHTERMS_ERROR] = errormessage
    tk.get_action("package_update")(site_user_context(), dataset)


def get_termcols(dataframe):
    return [column for column in dataframe.columns.values.tolist() if "Term" in column]


def get_identifiercols(dataframe):
    return [
        column
        for column in dataframe.columns.values.tolist()
        if "rsrc" not in column and "Term" not in column
    ]


def get_rsrccols(dataframe):
    return [column for column in dataframe.columns.values.tolist() if "rsrc" in column]
