import dlt
from dlt.sources.rest_api import rest_api_source


def main():
    # dlt config items

    # dlt.config["data_writer.buffer_max_items"] = 500
    dlt.config["data_writer.file_max_items"] = 500
    dlt.config["extract.workers"] = 5
    dlt.config["normalize.workers"] = 20
    dlt.config["load.workers"] = 20

    jaffle_config = {
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
            "paginator": {
                "type": "header_link",
            },
        },
        "resources": [
            {
                "name": "orders",
                "processing_steps": [
                    {"filter": lambda x: int(x["order_total"]) < 500},
                ],
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "page": 1,
                        "page_size": 1000,
                        "start_date": {
                            "type": "incremental",
                            "cursor_path": "ordered_at",
                            "initial_value": "2017-08-01",
                        },
                    },
                },
            },
            {
                "name": "customers",
                "endpoint": {
                    "path": "customers",
                    "params": {"page": 1, "page_size": 1000},
                },
            },
            {
                "name": "products",
                "endpoint": {
                    "path": "products",
                    "params": {"page": 1, "page_size": 1000},
                },
            },
        ],
    }

    jaffle_src = rest_api_source(jaffle_config)

    jaffle_src.parallelize()

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_pipeline",
        destination="duckdb",
        dataset_name="jaffle_shop",
        progress="log",
    )

    load_info = pipeline.run(jaffle_src)
    print(load_info)
    print(pipeline.last_trace)


if __name__ == "__main__":
    main()
