# Using dlt to fetch a CSV file and an Excel file from an SFTP server, and write it to duckdb

- sFTP server in this instance is running locally, but this could be any sFTP source.
- Fetches a CSV file and loads into duckdb with a built in read_csv() transformer.
- Fetches an Excel file and loads into duckdb with a custom read_excel() transformer.

