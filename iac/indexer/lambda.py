from os import environ
from re import match, sub
from typing import Any, Dict, Final, List

import obstore
from stac_index.indexer.index import execute


def handler(event, context) -> Dict[str, Any]:
    manifest_s3_uri = environ["MANIFEST_S3_URI"]
    manifest_parent_prefix_s3_uri = "/".join(manifest_s3_uri.split("/")[:-1]) + "/"
    manifest_path = sub("^s3://[^/]+/(.+)$", r"\1", manifest_s3_uri)
    manifest_parent_path = "/".join(manifest_path.split("/")[:-1]) + "/"
    index_bucket = sub("^s3://([^/]+)/.+$", r"\1", manifest_s3_uri)
    indexer_args: Dict[str, Any] = {"publish_uri": manifest_parent_prefix_s3_uri}
    s3_store = obstore.store.S3Store(bucket=index_bucket)
    try:
        s3_store.head(path=manifest_path)
        indexer_args["manifest_json_uri"] = manifest_s3_uri
    except FileNotFoundError:
        print("manifest not found at {}".format(manifest_path))
        root_catalog_uri = environ.get("ROOT_CATALOG_URI", "")
        if root_catalog_uri == "":
            raise Exception(
                "either ROOT_CATALOG_URI or MANIFEST_S3_URI must be configured"
            )
        indexer_args["root_catalog_uri"] = root_catalog_uri

    execute(**indexer_args)

    index_run_retention_default: Final[int] = 3
    index_files_by_run: Dict[str, List[str]] = {}
    for object_meta in obstore.list(
        store=s3_store, prefix=manifest_parent_path
    ).collect():
        object_path = str(object_meta["path"])
        index_run_prefix = "/".join(object_path.split("/")[:-1]) + "/"
        if match(
            r".*\d{4}\-\d{2}\-\d{2}T\d{2}\.\d{2}\.\d{2}\.\d{6}Z\-[a-z0-9]{32}/",
            index_run_prefix,
        ):
            if index_run_prefix not in index_files_by_run:
                index_files_by_run[index_run_prefix] = []
            index_files_by_run[index_run_prefix].append(object_path)
    index_run_retention = int(
        environ.get("INDEX_RUN_RETENTION", index_run_retention_default)
    )
    if index_run_retention < 1:
        print(
            "refusing to retain <1 index runs, defaulting to {}".format(
                index_run_retention_default
            )
        )
        index_run_retention = index_run_retention_default
    index_run_prefixes_to_delete = sorted(list(index_files_by_run.keys()))[
        :-index_run_retention
    ]
    print(f"need to delete {len(index_run_prefixes_to_delete)} past index run(s)")
    if len(index_run_prefixes_to_delete) > 0:
        for index_run_prefix_to_delete in index_run_prefixes_to_delete:
            print("deleting '{}'".format(index_run_prefix_to_delete))
            obstore.delete(
                store=s3_store, paths=index_files_by_run[index_run_prefix_to_delete]
            )

    return {"message": "indexing complete"}
