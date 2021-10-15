import os

from ckan import model
import ckan.plugins.toolkit as tk

SEARCHTERMS_ERROR = "searchterms_error"
TERMS_RSRC_NAME = "Search Terms"
TRUE = True
BLANK = ""


def get_resource_file_path(id):
    dir1 = id[0:3]
    dir2 = id[3:6]
    base = os.environ["CKAN_STORAGE_PATH"]
    return base + "/resources/" + dir1 + "/" + dir2 + "/" + id[6:]


def site_user_context():
    user = tk.get_action("get_site_user")({"model": model, "ignore_auth": True}, {})
    return {"ignore_auth": True, "user": user["name"], "auth_user_obj": None}
