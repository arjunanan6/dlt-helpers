from typing import Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_csv


@dlt.transformer(standalone=True)
def read_excel(items: Iterator[FileItemDict], sheet_name: str) -> Iterator[TDataItems]:
    import pandas as pd

    # Iterate through each file item.
    for file_obj in items:
        with file_obj.open() as file:
            # Read from the Excel file and yield its content as dictionary records.
            yield pd.read_excel(file, sheet_name).to_dict(orient="records")


def extract_from_fs():
    """CSV Transformation"""

    source = filesystem(file_glob="/home/foo/upload/*.csv") | read_csv()

    source.apply_hints(write_disposition="replace")

    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        progress="log",
        dataset_name="filesystem_ds",
    )

    pipeline.run(source.with_name("my_csv_table"))


def extract_from_fs_excel():
    """Excel transformation"""

    excel_source = filesystem(file_glob="/home/foo/upload/*.xlsx") | read_excel("Sheet1") # Sheet name in excel.

    excel_source.apply_hints(write_disposition="replace")

    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        dataset_name="filesystem_ds",
        progress="log",
    )

    pipeline.run(excel_source.with_name("my_excel_table"))


if __name__ == "__main__":
    extract_from_fs()
    extract_from_fs_excel()
