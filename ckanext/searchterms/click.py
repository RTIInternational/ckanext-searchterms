# -*- coding: utf-8 -*-
import click
from ckanext.searchterms.command import SearchtermsCmd


@click.group()
def searchterms():
    """searchterms commands
    Usage:
            searchterms submit <dataset-spec>

                    Submit the given datasets' for summary statistics
                    (They are added to the queue for CKAN's task worker.

                    where <dataset-spec> is one of:

                            <dataset-name> - Submit a particular dataset's resources

                            <dataset-id> - Submit a particular dataset's resources

                            all - Submit all datasets' resources to the DataStore
    """
    pass


@searchterms.command()
@click.argument(u"dataset-spec")
@click.option(
    "--fg", is_flag=True, default=False, help="Runs the gene tagger in the foreground"
)
def submit(dataset_spec, fg):
    """
    searchterms submit <dataset-spec>
    """
    cmd = SearchtermsCmd(fg)
    if dataset_spec == "all":
        cmd.submit_all_pkgs()
    else:
        pkg_id = cmd.identify_pkg(dataset_spec)
        if pkg_id:
            cmd.resubmit_pkg(pkg_id)


def get_commands():
    return [searchterms]
