from glob import glob
from json import dump, load
from os import makedirs, path
from typing import Final, List

source_root_default: Final[str] = path.join(path.dirname(__file__), "sample", "data")
target_root_default: Final[str] = path.join(path.dirname(__file__), "sample-alt")


def main(
    old_link_prefix: str,
    new_link_prefix: str,
    source_root: str,
    target_root: str,
    single_item_links: bool,
) -> None:
    with open(path.join(source_root, "catalog.json"), "r") as f:
        catalog = load(f)

    for child_link_dict in [
        entry for entry in catalog["links"] if entry["rel"] == "child"
    ]:
        collection_in_path = path.join(
            source_root,
            *str(child_link_dict["href"]).replace(old_link_prefix, "").split("/"),
        )
        if not path.exists(collection_in_path):
            print(f"collection at {collection_in_path} does not exist")
            continue
        with open(collection_in_path, "r") as f:
            try:
                collection = load(f)
            except Exception:
                print(f"failed to open collection '{collection_in_path}'")
                continue

        item_paths: List[str] = []
        for items_link_dict in [
            entry for entry in collection["links"] if entry["rel"] == "items"
        ]:
            for item_in_path in glob(
                path.join(
                    path.join(
                        source_root,
                        *str(items_link_dict["href"])
                        .replace(old_link_prefix, "")
                        .split("/"),
                    ),
                    "*.json",
                )
            ):
                if not path.exists(item_in_path):
                    print(f"item at {item_in_path} does not exist")
                    continue
                item_paths.append(item_in_path)
                with open(item_in_path, "r") as f:
                    try:
                        item = load(f)
                    except Exception:
                        print(f"failed to open item '{item_in_path}'")
                        continue

                item_out_path = item_in_path.replace(source_root, target_root)
                makedirs(path.dirname(item_out_path), exist_ok=True)

                with open(item_out_path, "w") as f:
                    dump(
                        {
                            **item,
                            "links": [
                                {
                                    **link,
                                    "href": str(link["href"]).replace(
                                        old_link_prefix, new_link_prefix
                                    ),
                                }
                                for link in item["links"]
                            ],
                        },
                        f,
                        indent=2,
                    )

        collection_out_path = collection_in_path.replace(source_root, target_root)
        makedirs(path.dirname(collection_out_path), exist_ok=True)
        links_list = [
            {
                **link,
                "href": str(link["href"]).replace(old_link_prefix, new_link_prefix),
            }
            for link in collection["links"]
        ]
        if single_item_links:
            links_list = [link for link in links_list if link["rel"] != "items"] + [
                {
                    "rel": "item",
                    "type": "application/geo+json",
                    "href": str(item_path).replace(old_link_prefix, new_link_prefix),
                }
                for item_path in item_paths
            ]
        with open(collection_out_path, "w") as f:
            dump(
                {
                    **collection,
                    "links": links_list,
                },
                f,
                indent=2,
            )

    catalog_out_path = path.join(target_root, "catalog.json")
    makedirs(path.dirname(catalog_out_path), exist_ok=True)
    with open(catalog_out_path, "w") as f:
        dump(
            {
                **catalog,
                "links": [
                    {
                        **link,
                        "href": str(link["href"]).replace(
                            old_link_prefix, new_link_prefix
                        ),
                    }
                    for link in catalog["links"]
                ],
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "old_link_prefix",
        type=str,
        help="STAC link path prefix to replace",
    )
    parser.add_argument(
        "new_link_prefix",
        type=str,
        help="Replacement for STAC link path prefix",
    )
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
    parser.add_argument(
        "--single_item_links",
        action="store_true",
        default=False,
        help="Optionally replace a collection's 'items' link with individual 'item' links for each item",
    )
    args = parser.parse_args()
    main(
        args.old_link_prefix,
        args.new_link_prefix,
        args.source_root[0],
        args.target_root[0],
        args.single_item_links,
    )
