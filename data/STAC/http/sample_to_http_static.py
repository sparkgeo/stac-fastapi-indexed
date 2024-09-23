from glob import glob
from json import dump, load
from os import makedirs, path
from shutil import copyfile
from typing import Final, List

source_root_default: Final[str] = path.join(path.dirname(__file__), "sample", "data")
target_root_default: Final[str] = path.join(path.dirname(__file__), "sample-alt")


def main(
    source_root: str,
    target_root: str,
) -> None:
    catalog_in_path = path.join(source_root, "catalog.json")
    with open(catalog_in_path, "r") as f:
        catalog = load(f)

    for child_link_dict in [
        entry for entry in catalog["links"] if entry["rel"] == "child"
    ]:
        collection_in_path = path.join(
            source_root,
            child_link_dict["href"],
        )

        with open(collection_in_path, "r") as f:
            try:
                collection = load(f)
            except Exception:
                print(f"failed to open collection '{collection_in_path}'")
                continue

        collection_item_links: List[str] = []
        for items_link_dict in [
            entry for entry in collection["links"] if entry["rel"] == "items"
        ]:
            for item_in_path in glob(
                path.join(
                    path.join(
                        source_root,
                        items_link_dict["href"],
                    ),
                    "*.json",
                )
            ):
                with open(item_in_path, "r") as f:
                    try:
                        item = load(f)
                    except Exception:
                        print(f"failed to open item '{item_in_path}'")
                        continue
                collection_item_links.append(
                    [link["href"] for link in item["links"] if link["rel"] == "self"][0]
                )
                item_out_path = item_in_path.replace(source_root, target_root)
                makedirs(path.dirname(item_out_path), exist_ok=True)
                copyfile(item_in_path, item_out_path)

        collection_out_path = collection_in_path.replace(source_root, target_root)
        makedirs(path.dirname(collection_out_path), exist_ok=True)
        with open(collection_out_path, "w") as f:
            dump(
                {
                    **collection,
                    "links": [
                        link for link in collection["links"] if link["rel"] != "items"
                    ]
                    + [
                        {
                            "rel": "item",
                            "type": "application/json",
                            "href": item_link,
                        }
                        for item_link in collection_item_links
                    ],
                },
                f,
                indent=2,
            )

    catalog_out_path = path.join(target_root, "catalog.json")
    makedirs(path.dirname(catalog_out_path), exist_ok=True)
    copyfile(catalog_in_path, catalog_out_path)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--source_root",
        type=str,
        help=f"Optional source data root path, defaults to {source_root_default}",
        default=[source_root_default],
        nargs=1,
    )
    parser.add_argument(
        "--target_root",
        type=str,
        help=f"Optional target data root path, defaults to {target_root_default}",
        default=[target_root_default],
        nargs=1,
    )
    args = parser.parse_args()
    main(
        args.source_root[0],
        args.target_root[0],
    )
