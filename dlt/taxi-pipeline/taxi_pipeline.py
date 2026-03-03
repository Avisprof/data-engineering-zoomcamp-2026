"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline():
    """Define dlt resources from REST API endpoints."""
    # The source should yield items from the rest_api generator.
    yield from rest_api_resources({
        "client": {
            # base URL of the taxi rides API, pages are appended with ?page=<n>
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # no authentication required; leaving auth out entirely is fine
        },
        "resources": [
            {
                # name of the resource/table in the destination
                "name": "taxi_rides",
                "endpoint": {
                    # root path (empty) since the base URL already returns the list
                    "path": "",
                    # Configure paginator to handle the direct list response
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        # no `total` field is returned by the API, so disable
                        # the default total_path check which otherwise raises
                        # ValueError. pagination will then simply continue until
                        # an empty response comes back.
                        "total_path": None,
                        # each page returns 1 000 records, and the paginator will
                        # stop automatically when a response contains no elements.
                        "stop_after_empty_page": True,
                    },
                    # The API returns records directly as a list
                    "data_selector": "",
                },
            }
        ],
        # you could set `resource_defaults` here if multiple endpoints share settings
    })


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # `drop_sources` removes existing tables and state each run so we are
    # working with a fresh database while debugging.  Remove this in
    # production if you want incremental loads.
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline())

    load_id = load_info.loads_ids[-1]
    m = load_info.metrics[load_id][0]
    print('>>>')
    #print("Resources:", list(m["resource_metrics"].keys()))
    #print("Tables:", list(m["table_metrics"].keys()))
    #print("Load ID:", load_id)
    print('<<<')

    #for resource, rm in m["resource_metrics"].items():
    #    print(f"Resource: {resource}")
    #    print(f"rows extracted: {rm.items_count}")
    #    print()

    #print(load_info)  # noqa: T201
