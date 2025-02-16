import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database


def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam",
        destination="duckdb",
        dataset_name="rfam_data",
        progress="log",
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    # credentials = ConnectionStringCredentials(
    #     "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    # )
    credentials = dlt.secrets.get("sources.sql_database.credentials")

    # To pass the credentials from `secrets.toml`, comment out the above credentials.
    # And the credentials will be automatically read from `secrets.toml`.

    # Configure the source to load a few select tables incrementally
    source_1 = sql_database(credentials).with_resources("family", "clan").parallelize()

    # Add incremental config to the resources. "updated" is a timestamp column in these tables that gets used as a cursor
    source_1.family.apply_hints(incremental=dlt.sources.incremental("updated"))
    source_1.clan.apply_hints(incremental=dlt.sources.incremental("updated"))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(source_1, write_disposition="merge")
    print(info)

    # Load some other tables with replace write disposition. This overwrites the existing tables in destination
    source_2 = (
        sql_database(credentials).with_resources("features", "author").parallelize()
    )
    info = pipeline.run(source_2, write_disposition="replace")
    print(info)

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source_3 = sql_database(credentials).with_resources("genome").parallelize()
    source_3.genome.apply_hints(incremental=dlt.sources.incremental("created"))

    info = pipeline.run(source_3, write_disposition="append")
    print(info)


load_select_tables_from_database()
