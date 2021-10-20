import logging
import ckan.model as model
import ckan.plugins.toolkit as toolkit

from .implementations import is_eligible
from .jobs import check_search_terms_resource

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
        log.info("Found {0} packages".format(package_list.get("count", "bzzt")))
        for pkg in package_list.get("results", []):
            pkgid = pkg.get("id") + " (" + pkg.get("name") + ")"
            print("Validating package " + pkgid)
            enqued = []
            nenqued = []
            validated = self.identify_pkg(pkg.get("id", ""))
            if validated:
                print("Succesfully validated package and submitted " + pkgid)
                self.resubmit_pkg(validated)
                enqued.append(pkgid)
            else:
                print("Unable to validate package " + pkg.get("id", ""))
                nenqued.append(pkgid)
            print("Enqueued:")
            print(", ".join(enqued))
            print("Not Enqueued:")
            print(", ".join(nenqued))

    def resubmit_pkg(self, package):
        pkgid = package.get("id") + " (" + package.get("name") + ")"
        log.info("Starting search terms job for package {0}".format(pkgid))
        for resource in package.get("resources", []):
            rsrcid = resource.get("id") + " (" + resource.get("name") + ")"
            if is_eligible(resource):
                if self.run_in_foreground:
                    log.info("Checking search terms for resource " + rsrcid)
                    check_search_terms_resource(resource, resource_was_updated=True)
                else:
                    log.info("Enqueueing search terms job for resource " + rsrcid)
                    toolkit.enqueue_job(
                        check_search_terms_resource,
                        [resource, True],
                        rq_kwargs={"timeout": 21600},
                        queue=u"searchterms",
                    )
            else:
                log.debug("Skipping search terms job for resource " + rsrcid)
