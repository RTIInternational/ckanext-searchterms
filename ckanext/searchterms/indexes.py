import ckan.plugins.toolkit as toolkit
import ckan.model as model
import ckanext.datastore.backend.postgres as datastore_db
import logging

log = logging.getLogger(__name__)


def create_index_for_search_terms_resource(resource):
    """For a given search terms resource, create an index on the "search index" column in the datastore for use in the explorer tool"""
    get_write_engine = datastore_db.get_write_engine
    ## TODO: don't even attempt this if the index exists already rather than relying on IF NOT EXISTS
    log.info(
        f"creating index for the search_terms column for resource {resource.get('id')}"
    )
    create_idx_str = f"CREATE INDEX IF NOT EXISTS \"SEARCH_INDEX_GIN\" ON \"{resource.get('id')}\" USING GIN (\"search_index\" gin_trgm_ops);"
    engine = datastore_db.get_write_engine()
    connection = engine.connect()
    connection.execute(create_idx_str)
    connection.close()


def create_indexes_for_identifier_columns(context, resource):
    """For a given searchable resource, create an index on the "search index" column in the datastore for use in the explorer tool"""
    columns = [
        "Gene",
        "Molecule",
        "InChiKey",
        "database_identifier",
        "Protein",
        "ParticipantID",
    ]
    info = toolkit.get_action("datastore_info")(context, {"id": resource.get("id")})
    log.info(info)
    columns_in_file = info.get("schema").keys()
    columns = [column for column in columns if column in columns_in_file]
    for column in columns:
        log.info(
            f"creating index for the {column} column for resource {resource.get('id')}"
        )
        create_idx_str = f"CREATE INDEX IF NOT EXISTS \"{column.upper()}_GIN_IDX\" ON \"{resource.get('id')}\" USING GIN (\"{column}\" gin_trgm_ops);"
        engine = datastore_db.get_write_engine()
        connection = engine.connect()
        connection.execute(create_idx_str)
        connection.close()
