import os
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
    searchterms_df, _ = get_existing_search_terms_df_from_csv(dataset)
    log.debug("searchterms_df = {}".format(searchterms_df))

    # Parse the resource for new search terms
    try:
        new_terms_df, key = get_terms(resource, dataset, searchterms_df)
    except SearchtermsParsingError as e:
        return add_error(dataset, str(e))
    if searchterms_df is not None:
        # Update existing searchterms DataFrame.
        # If the resource was updated and already exists in the searchterms DataFrame, remove it
        if resource_was_updated and rsrc_col in searchterms_df.columns:
            searchterms_df = remove_resource_from_search_terms(rsrc_id, searchterms_df)

        searchterms_df = update_searchterms(rsrc_col, new_terms_df, key, searchterms_df)
        delete_existing_search_terms(resource)
    else:
        # Add a new column to the terms DataFrame and initialize all rows to TRUE
        new_terms_df[rsrc_col] = TRUE
        searchterms_df = new_terms_df

    save_file(searchterms_df, dataset.get("id"))
    return searchterms_df


def update_searchterms(rsrc_col, new_terms_df, key, searchterms_df):
    """
    Merges new searchterms DataFrame into existing searchterms DataFrame.

    1) If these terms are already in the old searchterms DataFrame, _update_ them
    3) If there are new terms not existing in the old searchterms DataFrame, _append_ them
    """
    log.debug("Updating searchterms")

    # Add a new column to the searchterms DataFrame and initialize all rows to BLANK
    searchterms_df[rsrc_col] = BLANK

    # If there are existing terms we need to update rows for, do so
    existing_terms_df = new_terms_df[
        new_terms_df[key].str.lower().isin(searchterms_df[key].str.lower())
    ]
    if not existing_terms_df.empty:
        for index, row in searchterms_df.iterrows():
            if row[key].lower() in existing_terms_df[key].str.lower().values:
                searchterms_df.at[index, rsrc_col] = TRUE
    # If there are new terms we need to add to our table, do so
    new_unique_terms_df = new_terms_df[
        ~new_terms_df[key].str.lower().isin(searchterms_df[key].str.lower())
    ].copy(deep=True)
    if not new_unique_terms_df.empty:
        new_unique_terms_df[rsrc_col] = TRUE
        searchterms_df = searchterms_df.append(
            new_unique_terms_df, ignore_index=True, sort=False
        )
    return searchterms_df


# This method drops the column for a given resource ID from the searchterms table
# But it first loops through all rows of the searchterms table, removing any if unused by other resources
def remove_resource_from_search_terms(resource_id, search_terms_df):
    log.debug("Removing resource from searchterms")
    rsrc_col_to_delete = "rsrc-{}".format(resource_id)
    other_rsrc_cols = [
        col
        for col in search_terms_df
        if col.startswith("rsrc-") and col != rsrc_col_to_delete
    ]
    rows_to_drop = []
    # checks for empty rows and and creates a list of empty row indices to delete (so we're not modifying what we're iterating over)
    for index, row in search_terms_df.iterrows():
        gene_tag_exists = False
        for col in other_rsrc_cols:
            if row[col] == TRUE:
                gene_tag_exists = True
                break
        if not gene_tag_exists:
            rows_to_drop.append(index)
    search_terms_df.drop(rows_to_drop, inplace=True)
    search_terms_df.drop(rsrc_col_to_delete, axis="columns", inplace=True)
    search_terms_df.reset_index(inplace=True)
    return search_terms_df


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
    with open(filepath, "rb") as file:
        resource_metadata = {
            "name": name,
            "resource_file_type": "",
            "package_id": dataset_id,
            "upload": FileStorage(file),
        }
        # TODO: move the file to its destination on the filesystem instead of uploading to the server
        # Possibly by implementing get_resource_uploader()
        tk.get_action("resource_create")(site_user_context(), resource_metadata)

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
