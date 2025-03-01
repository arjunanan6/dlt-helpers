import dlt
from dlt.sources.sql_database import sql_database


def load_incrementally():
    """Attempt to load incrementally"""

    # Set up the connection credentials
    # Local db running off of: https://hub.docker.com/r/microsoft/azure-sql-edge
    credentials = "mssql+pyodbc://SA:MyPass%40word@localhost:1433/arjun?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

    source = sql_database(credentials=credentials, schema="dbo").with_resources("replicate_test")
    
    # Configure incremental
    source.replicate_test.apply_hints(
        incremental=dlt.sources.incremental("OnlineSynk")
    )

    # Setup dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="local_test",
        destination="duckdb",
        dataset_name="arjun_test",
        progress="log"
    )

    # Run the pipeline
    pipeline.run(source)


if __name__ == "__main__":
    load_incrementally()
