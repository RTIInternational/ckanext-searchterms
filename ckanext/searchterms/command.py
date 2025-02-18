import logging
import ckan.model as model
import ckan.plugins.toolkit as toolkit

from .implementations import is_eligible
from .jobs import (
    check_search_terms_resource,
    enqueue_terms_job,
    enqueue_xloader_searchterms,
)
from .util import TERMS_RSRC_NAME, site_user_context

log = logging.getLogger(__name__)


class SearchtermsCmd:
    def __init__(self, fg):
        self.run_in_foreground = fg

    def identify_pkg(self, cmd):
        # try with id
        package = toolkit.get_action("package_show")({"ignore_auth": True}, {"id": cmd})
        # try with name
        if not package:
            package = toolkit.get_action("package_show")(
                {"ignore_auth": True}, {"name": cmd}
            )
        if not package:
            print("No package found by id or name for input " + cmd)
            return False
        else:
            return package

    def submit_all_pkgs(self):
        package_list = toolkit.get_action("package_search")(
            {"model": model, "ignore_auth": True},
            {"include_private": True, "rows": 1000},
        )
        total_pkg_count = 0
        total_res_count = 0
        total_pkg_failed_count = 0
        total_pkg_validated_count = 0
        log.info("Found {0} packages".format(package_list.get("count", "bzzt")))
        for pkg in package_list.get("results", []):
            total_pkg_count += 1
            pkgid = pkg.get("id") + " (" + pkg.get("name") + ")"
            log.info("Validating package " + pkgid)
            enqued = []
            nenqued = []
            try:
                validated = self.identify_pkg(pkg.get("id", ""))
            except Exception:
                log.error("Error validating package " + pkgid)
                validated = False
            if validated:
                log.info("Succesfully validated package and submitted " + pkgid)
                res_count = self.resubmit_pkg(validated)
                total_res_count += res_count if res_count else 0
                total_pkg_validated_count += 1
                enqued.append(pkgid)
            else:
                log.info("Unable to validate package " + pkg.get("id", ""))
                nenqued.append(pkgid)
                total_pkg_failed_count += 1
        log.info(
            "Total {} packages found. {} failed validation and did not submit. Submitted {} packages. {} total searchterm jobs".format(
                total_pkg_count,
                total_pkg_failed_count,
                total_pkg_validated_count,
                total_res_count,
            )
        )

    def resubmit_pkg(self, package):
        pkgid = package.get("id") + " (" + package.get("name") + ")"

        if len(package.get("resources", [])) == 0:
            log.info("No resources found for dataset {}".format(pkgid))
            return

        total_eligible_resources = 0

        for resource in package.get("resources", []):
            if resource.get("name", "") == TERMS_RSRC_NAME:
                log.info(
                    "Deleting old search terms resource for package {0}".format(pkgid)
                )
                toolkit.get_action("package_revise")(
                    site_user_context(),
                    {
                        "match__id": package.get("id"),
                        "filter": ["-resources__{}".format(resource.get("id"))],
                    },
                )

        log.info("Starting search terms job for package {0}".format(pkgid))
        for resource in package.get("resources", []):
            rsrcid = resource.get("id") + " (" + resource.get("name") + ")"
            if is_eligible(resource):
                total_eligible_resources += 1
                if self.run_in_foreground:
                    log.info("Checking search terms for resource " + rsrcid)
                    check_search_terms_resource(resource, resource_was_updated=True)
                else:
                    log.info("Enqueueing search terms job for resource " + rsrcid)
                    enqueue_terms_job(resource, True)
            else:
                log.debug("Skipping search terms job for resource " + rsrcid)
        # a package should only ever have one active searchterms resource file
        enqueue_xloader_searchterms(package.get("id"))
        return total_eligible_resources
