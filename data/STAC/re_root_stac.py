from glob import glob
from json import dump, load
from os import makedirs, path
from typing import Final

replaceable_path_prefix_default: Final[str] = "/data/"
source_root_default: Final[str] = path.join(path.dirname(__file__), "sample", "data")
target_root_default: Final[str] = path.join(path.dirname(__file__), "sample-alt")


def main(
    new_link_href_root: str,
    source_root: str,
    target_root: str,
    replaceable_path_prefix: str,
) -> None:
    with open(path.join(source_root, "catalog.json"), "r") as f:
        catalog = load(f)

    for child_link_dict in [
        entry for entry in catalog["links"] if entry["rel"] == "child"
    ]:
        collection_in_path = path.join(
            source_root,
            *str(child_link_dict["href"])
            .replace(replaceable_path_prefix, "")
            .split("/"),
        )

        with open(collection_in_path, "r") as f:
            collection = load(f)

        for item_link_dict in [
            entry for entry in collection["links"] if entry["rel"] == "items"
        ]:
            for item_in_path in glob(
                path.join(
                    path.join(
                        source_root,
                        *str(item_link_dict["href"])
                        .replace(replaceable_path_prefix, "")
                        .split("/"),
                    ),
                    "*.json",
                )
            ):
                with open(item_in_path, "r") as f:
                    item = load(f)

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
                                        replaceable_path_prefix, new_link_href_root
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
        with open(collection_out_path, "w") as f:
            dump(
                {
                    **collection,
                    "links": [
                        {
                            **link,
                            "href": str(link["href"]).replace(
                                replaceable_path_prefix, new_link_href_root
                            ),
                        }
                        for link in collection["links"]
                    ],
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
                            replaceable_path_prefix, new_link_href_root
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
        "new_link_href_root",
        type=str,
        help="Replacement string for 'replaceable_path_prefix' in STAC link hrefs",
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
        "--replaceable_path_prefix",
        type=str,
        help=f"Optional path prefix to replace, defaults to '{replaceable_path_prefix_default}'",
        default=[replaceable_path_prefix_default],
        nargs=1,
    )
    args = parser.parse_args()
    main(
        args.new_link_href_root,
        args.source_root[0],
        args.target_root[0],
        args.replaceable_path_prefix[0],
    )
