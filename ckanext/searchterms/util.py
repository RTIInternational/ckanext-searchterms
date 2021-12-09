import os
import time
import logging

from ckan import model
import ckan.plugins.toolkit as tk

SEARCHTERMS_ERROR = "searchterms_error"
TERMS_RSRC_NAME = "Search Terms"
TRUE = True
BLANK = ""

log = logging.getLogger(__name__)


def get_resource_file_path(id):
    dir1 = id[0:3]
    dir2 = id[3:6]
    base = os.environ["CKAN_STORAGE_PATH"]
    return base + "/resources/" + dir1 + "/" + dir2 + "/" + id[6:]


def site_user_context():
    user = tk.get_action("get_site_user")({"model": model, "ignore_auth": True}, {})
    return {"ignore_auth": True, "user": user["name"], "auth_user_obj": None}


def file_exists(resource_id, num_retries=3, delay=0.5):
    # Sometimes after the file was uploaded to CKAN, it takes a second for it to exist
    for try_num in range(0, num_retries):
        if os.path.exists(get_resource_file_path(resource_id)):
            log.info("File exists")
            return True
        elif try_num + 1 != num_retries:
            log.info(
                "Retrying file_exists({}) (retry {})".format(resource_id, try_num + 1)
            )
            time.sleep(delay)
    return False
