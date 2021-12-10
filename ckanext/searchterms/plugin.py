import math
import json
import logging
from os.path import exists

from ckan import plugins
import ckan.plugins.toolkit as tk
import pandas as pd

from .implementations import is_eligible
from .jobs import (
    check_search_terms_resource,
    enqueue_terms_job,
    enqueue_terms_update_on_delete_job,
)
from .util import (
    TERMS_RSRC_NAME,
    get_resource_file_path,
    site_user_context,
)

log = logging.getLogger(__name__)


def package_has_resource_type(package, resource_type):
    ret_val = False
    for res in package["resources"]:
        if res["resource_file_type"] == resource_type:
            ret_val = True
            break
    return ret_val


class SearchtermsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IClick)

    # IResourceController
    def before_update(self, context, current, resource):
        if resource.get("package_id", False):  # have to make sure it's not a package
            context["file_uploaded"] = False
            if resource.get("upload", False):
                context["file_uploaded"] = True

    def after_create(self, context, resource):
        enqueue_terms_job(resource)

    def after_update(self, context, resource):
        if context.get("file_uploaded"):
            enqueue_terms_job(resource)

    # doesn't actually run for some reason
    def before_delete(self, context, resource, resources):
        enqueue_terms_update_on_delete_job(resource)

    # IPackageController
    def before_index(self, pkg_dict):
        pkg = tk.get_action("package_show")(
            site_user_context(), {"id": pkg_dict.get("id")}
        )
        filteredResources = list(
            filter(
                lambda rsrc: rsrc.get("name") == TERMS_RSRC_NAME,
                pkg.get("resources", []),
            )
        )
        if len(filteredResources):
            fpath = get_resource_file_path(filteredResources[0].get("id"))
            if exists(fpath):
                try:
                    df = pd.read_csv(fpath, sep="\t")
                    data = df.values.flatten().tolist()
                    hundreds = math.ceil(len(data) / float(100))
                    for i in range(int(hundreds)):

                        def upper_bound(proposed, max_len=len(data)):
                            return max_len if proposed > max_len else proposed

                        key = pkg.get("id") + "_search_term_" + str(i)
                        min = int(i * 100)
                        max = upper_bound((i + 1) * 100)
                        data_slice = data[min:max]
                        pkg_dict["extras_" + key] = json.dumps(data_slice)

                except Exception:
                    err_msg = "An error occurred in building the index for package {0}"
                    log.error(err_msg.format(pkg.get("name")))
                    raise
            else:
                # Happens when resource is created, package updated with metadata, but file not uploaded yet
                log.debug("Search terms resource exists but file does not.")

        return pkg_dict

    # IConfigurer
    # Adds templates from the plugin to the CKAN instance
    def update_config(self, config_):
        tk.add_template_directory(config_, "templates")

    #######################################################################
    # IClick                                                              #
    # Command-line interface for submitting dataset processing jobs       #
    #######################################################################
    def get_commands(self):
        from .click import get_commands

        return get_commands()
