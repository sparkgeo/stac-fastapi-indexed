from json import dump
from os import makedirs, path
from re import sub
from typing import Final, List

import requests


def s3_join(parts: List[str]) -> str:
    return "/".join([sub("/$", "", sub("^/", "", part)) for part in parts])


base_source_url: Final[str] = "https://planetarycomputer.microsoft.com/api/stac/v1/"
collection_ids: Final[List[str]] = [
    "ecmwf-forecast",
    "modis-17A2HGF-061",
]
json_type: Final[str] = "application/json"
geojson_type: Final[str] = "application/geo+json"
s3_base_url: Final[str] = "s3://tchristian-stac-serverless-data/pc_partial_data"

base_source_path: Final[str] = path.abspath(
    path.join(path.dirname(__file__), "pc_partial_data", "s3")
)
catalog_path: Final[str] = s3_join([s3_base_url, "catalog.json"])


def collection_source_path(collection_id: str) -> str:
    return path.join(base_source_path, "collections", "{}.json".format(collection_id))


def collection_access_path(collection_id: str) -> str:
    return s3_join([s3_base_url, "collections", collection_id])


def collection_items_source_path(collection_id: str) -> str:
    return path.join(base_source_path, "collections", collection_id, "items")


def collection_items_access_path(collection_id: str) -> str:
    return s3_join([s3_base_url, "collections", collection_id, "items"])


def execute():
    with open(path.join(base_source_path, "catalog.json"), "w") as f:
        dump(
            {
                "type": "Catalog",
                "id": "microsoft-pc-local",
                "title": "Microsoft Planetary Computer Local",
                "description": "A filtered and altered local file version of the Planetary Computer catalog",
                "stac_version": "1.0.0",
                "conformsTo": ["https://api.stacspec.org/v1.0.0-rc.1/core"],
                "links": [
                    {
                        "rel": "self",
                        "type": json_type,
                        "href": catalog_path,
                    },
                    {
                        "rel": "root",
                        "type": json_type,
                        "href": catalog_path,
                    },
                ]
                + [
                    {
                        "rel": "child",
                        "type": json_type,
                        "title": collection_id,
                        "href": collection_access_path(collection_id),
                    }
                    for collection_id in collection_ids
                ],
            },
            f,
            indent=2,
        )

    for collection_id in collection_ids:
        print(f"fetching collection {collection_id}")
        collection_response = requests.get(
            f"{base_source_url}/collections/{collection_id}"
        )
        if collection_response.status_code != 200:
            print(
                f"problem fetching {collection_id}: {collection_response.status_code}"
            )
            continue
        try:
            collection = collection_response.json()
        except Exception as e:
            print(f"problem parsing {collection_id}: {e}")
            continue
        collection = {
            **collection,
            **{
                "links": [
                    link
                    for link in collection["links"]
                    if link["rel"]
                    in [
                        "license",
                        "describedBy",
                    ]
                ]
                + [
                    {
                        "rel": "items",
                        "type": geojson_type,
                        "href": collection_items_access_path(collection_id),
                    },
                    {
                        "rel": "parent",
                        "type": json_type,
                        "href": catalog_path,
                    },
                    {
                        "rel": "root",
                        "type": json_type,
                        "href": catalog_path,
                    },
                    {
                        "rel": "self",
                        "type": json_type,
                        "href": collection_access_path(collection_id),
                    },
                ]
            },
        }
        makedirs(collection_items_source_path(collection_id), exist_ok=True)
        with open(collection_source_path(collection_id), "w") as f:
            dump(
                collection,
                f,
                indent=2,
            )

        next_items_url = f"{base_source_url}/collections/{collection_id}/items"
        page_count = 0
        while next_items_url is not None:
            page_count += 1
            print(
                f"fetching items page {page_count} in {collection_id} with {next_items_url}"
            )
            items_response = requests.get(next_items_url)
            if items_response.status_code != 200:
                print(f"problem getting {next_items_url}: {items_response.status_code}")
                break
            try:
                items = items_response.json()
            except Exception as e:
                print(f"problem parsing {next_items_url}: {e}")
                break
            next_link_entries = [
                link
                for link in items["links"]
                if link["rel"] == "next" and link["method"] == "GET"
            ]
            next_items_url = (
                next_link_entries[0]["href"] if len(next_link_entries) == 1 else None
            )
            for feature in items["features"]:
                feature = {
                    **feature,
                    **{
                        "links": [
                            link
                            for link in feature["links"]
                            if link["rel"]
                            in [
                                "cite-as",
                                "via",
                                "preview",
                            ]
                        ]
                        + [
                            {
                                "rel": "collection",
                                "type": json_type,
                                "href": collection_access_path(collection_id),
                            },
                            {
                                "rel": "parent",
                                "type": json_type,
                                "href": collection_access_path(collection_id),
                            },
                            {
                                "rel": "root",
                                "type": json_type,
                                "href": catalog_path,
                            },
                            {
                                "rel": "self",
                                "type": geojson_type,
                                "href": s3_join(
                                    [
                                        collection_items_access_path(collection_id),
                                        "{}.json".format(feature["id"]),
                                    ]
                                ),
                            },
                        ]
                    },
                }

                with open(
                    path.join(
                        collection_items_source_path(collection_id),
                        "{}.json".format(feature["id"]),
                    ),
                    "w",
                ) as f:
                    dump(
                        feature,
                        f,
                        indent=2,
                    )


if __name__ == "__main__":
    execute()
