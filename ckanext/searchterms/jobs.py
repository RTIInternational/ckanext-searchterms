import os
import logging
import pandas as pd
import uuid
import datetime
import json
from werkzeug.datastructures import FileStorage
import ckan.plugins.toolkit as tk
import ckan.plugins as p
import ckan.model as model
from .constants import SearchtermsParsingError
from .implementations import is_eligible, get_terms
from .util import (
    BLANK,
    SEARCHTERMS_ERROR,
    TERMS_RSRC_NAME,
    get_resource_file_path,
    site_user_context,
)

log = logging.getLogger(__name__)


# If resource is eligible, add terms job to worker queue
def enqueue_terms_job(resource, resource_was_updated=False):
    # Check package_id exists to make sure it's not a package
    if (
        resource.get("name") == TERMS_RSRC_NAME
        or not resource.get("package_id", False)
        or not is_eligible(resource)
    ):
        return

    res_id = resource.get("id")
    task = {
        "entity_id": res_id,
        "entity_type": "resource",
        "task_type": "searchterms",
        "last_updated": str(datetime.datetime.utcnow()),
        "state": "submitting",
        "key": "searchterms",
        "value": "{}",
        "error": "{}",
    }
    p.toolkit.get_action("task_status_update")(
        {"session": model.meta.create_local_session(), "ignore_auth": True}, task
    )
    try:
        job = tk.enqueue_job(
            check_search_terms_resource,
            [resource, resource_was_updated],
            rq_kwargs={"timeout": 21600},
            queue="searchterms",
        )
    except Exception:
        log.exception("Unable to queue searchterms res_id=%s", res_id)

    package_id = resource.get("package_id")
    value = json.dumps(
        {"job_id": job.id, "package_id": package_id, "resource_id": res_id}
    )

    task["value"] = value
    task["state"] = "pending"
    task["last_updated"] = str(datetime.datetime.utcnow())

    p.toolkit.get_action("task_status_update")(
        {"session": model.meta.create_local_session(), "ignore_auth": True}, task
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
                search_terms_df = pd.read_csv(filepath, sep="\t", dtype=str)
                # Check for old schema
                if "found_in_1" in search_terms_df.columns:
                    log.info("Old schema detected; deleting old search terms resource")
                    tk.get_action("package_revise")(
                        site_user_context(),
                        {
                            "match__id": pkg.get("id"),
                            "filter": ["-resources__{}".format(rsc.get("id"))],
                        },
                    )
                    return None, None
            except UnicodeDecodeError:
                log.error(
                    "Existing searchterms resource is non-unicode. Deleting the resource."
                )
                tk.get_action("package_revise")(
                    site_user_context(),
                    {
                        "match__id": pkg.get("id"),
                        "filter": ["-resources__{}".format(rsc.get("id"))],
                    },
                )
            except FileNotFoundError:
                err_msg = (
                    "Search terms resource does not have a corresponding file to load "
                    "(File does not exist: {0}). "
                    "Deleting the resource."
                )
                log.error(err_msg.format(filepath))
                tk.get_action("package_revise")(
                    site_user_context(),
                    {
                        "match__id": pkg.get("id"),
                        "filter": ["-resources__{}".format(rsc.get("id"))],
                    },
                )
    return search_terms_df, search_terms_resource_id


def check_search_terms_resource(resource, resource_was_updated=False):
    """
    Check for existing searchterms, update if it exists, otherwise create it
    """
    # Retrieve the existing task_status for this resource's searchterms job
    res_id = resource.get("id")
    existing_task = p.toolkit.get_action("task_status_show")(
        site_user_context(),
        {"entity_id": res_id, "task_type": "searchterms", "key": "searchterms"},
    )

    # Update the task_status to 'running'
    task = {
        "id": existing_task.get("id"),
        "entity_id": res_id,
        "entity_type": "resource",
        "task_type": "searchterms",
        "last_updated": str(datetime.datetime.utcnow()),
        "state": "running",
        "key": "searchterms",
        "value": existing_task.get("value", ""),
        "error": "{}",
    }
    p.toolkit.get_action("task_status_update")(
        {"session": model.meta.create_local_session(), "ignore_auth": True}, task
    )

    log = logging.getLogger(__name__)

    try:
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
            add_error(resource, str(e))
            raise Exception(
                f"Error parsing search terms for resource {resource.get('name')}"
            )
        if searchterms_df is not None:
            if resource_was_updated and rsrc_col in searchterms_df.columns:
                searchterms_df = remove_resource_from_search_terms(
                    rsrc_col, searchterms_df
                )
            searchterms_df = update_searchterms(rsrc_col, new_terms_df, searchterms_df)
            delete_existing_search_terms(resource)
            new_column_order = [
                *get_identifiercols(searchterms_df),
                *get_termcols(searchterms_df),
                *get_rsrccols(searchterms_df),
                "search_index",
            ]
            searchterms_df = searchterms_df[new_column_order]
        else:
            searchterms_df = new_terms_df
        searchterms_df = add_search_index_to_search_terms(searchterms_df)
        save_file(searchterms_df, dataset.get("id"))

        task["state"] = "complete"
        task["error"] = "{}"
        p.toolkit.get_action("task_status_update")(
            {"session": model.meta.create_local_session(), "ignore_auth": True}, task
        )
        return searchterms_df
    except Exception as e:
        task["state"] = "error"
        task["error"] = str(e)
        log.error("searchterms error: {0}".format(str(e)))
        p.toolkit.get_action("task_status_update")(
            {"session": model.meta.create_local_session(), "ignore_auth": True}, task
        )


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
    rsrc_id = f"rsrc-{rsrc_col}" if "rsrc" not in rsrc_col else rsrc_col
    # remove old column
    searchterms_df.drop(columns=[rsrc_id], inplace=True)
    # drop any rows that should no longer exist because that column is gone
    rsrc_cols = get_rsrccols(searchterms_df)
    searchterms_df.dropna(how="all", subset=rsrc_cols, inplace=True)
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
        tk.get_action("package_revise")(
            site_user_context(),
            {
                "match__id": resource.get("package_id"),
                "filter": ["-resources__{}".format(searchterms_id)],
            },
        )
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
        }
        # TODO: move the file to its destination on the filesystem instead of uploading to the server
        # Possibly by implementing get_resource_uploader()
        pkg = tk.get_action("package_revise")(
            site_user_context(),
            {
                "match__id": dataset_id,
                "update__resources__extend": [resource_metadata],
                "update__resources__-1__upload": FileStorage(file),
                "update": {SEARCHTERMS_ERROR: BLANK},
            },
        )
        log.info(
            "Created search terms resource for {} for dataset {}".format(
                name, dataset_id
            )
        )
    return pkg


def enqueue_xloader_searchterms(dataset_id):
    tk.enqueue_job(
        xloader_searchterms,
        [dataset_id],
        rq_kwargs={"timeout": 21600},
        queue="searchterms",
    )


def xloader_searchterms(dataset_id):
    pkg = tk.get_action("package_show")(site_user_context(), {"id": dataset_id})
    # Manually submit searchterms to xloader to make a preview available
    for resource in pkg.get("resources", []):
        if (
            resource.get("name") == TERMS_RSRC_NAME
            and resource.get("state") == "active"
            and not resource.get("datastore_active")
        ):
            resource_id = resource.get("id")
            tk.get_action("xloader_submit")(
                site_user_context(),
                {"resource_id": resource_id, "ignore_hash": True},
            )
            log.info(
                "Enqueued xloader job for search terms resource {} for dataset {}".format(
                    resource_id, dataset_id
                )
            )


def delete_existing_search_terms(resource):
    pkg = tk.get_action("package_show")(
        site_user_context(), {"id": resource.get("package_id")}
    )
    # Get all searchterm resources in package
    resources = [
        res for res in pkg.get("resources") if res.get("name") == TERMS_RSRC_NAME
    ]
    # Build filter arguments for removing resources with package_revise
    resource_filters = []
    for resource in resources:
        log.info(
            "Deleting existing searchterms resource file with resource id: {}".format(
                resource.get("id")
            )
        )
        resource_filters.append("-resources__{}".format(resource.get("id")))
    tk.get_action("package_revise")(
        site_user_context(),
        {
            "match__id": pkg.get("id"),
            "filter": resource_filters,
        },
    )


def add_search_index_to_search_terms(searchterms_df):
    """
    adds a || separated string column to each row as a 'search_index' for use in the explorer tool
    """
    if "search_index" in searchterms_df.columns:
        searchterms_df.drop(columns=["search_index"], inplace=True)
    indexable_cols = get_termcols(searchterms_df) + get_identifiercols(searchterms_df)
    searchterms_df["search_index"] = searchterms_df[indexable_cols].agg(
        "||".join, axis=1
    )
    return searchterms_df


def add_error(resource, e):
    errormessage = "Unable to process your resource for search. Error: {}".format(e)
    resource[SEARCHTERMS_ERROR] = errormessage
    res_id = resource.get("id")
    tk.get_action("package_revise")(
        site_user_context(),
        {
            "match__id": resource.get("package_id"),
            f"update__resources__{res_id}": resource,
        },
    )


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
