from glob import glob
from json import dump, load
from os import makedirs, path
from typing import Final

replaceable_path_prefix: Final[str] = "s3://stac/sample/"
source_root: Final[str] = path.join(path.dirname(__file__), "sample", "data")
target_root: Final[str] = path.join(path.dirname(__file__), "sample-alt")


def main(new_link_href_root: str) -> None:
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

    with open(path.join(target_root, "catalog.json"), "w") as f:
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
        help=f"Replacement string for {replaceable_path_prefix} in STAC link hrefs",
    )
    args = parser.parse_args()
    main(args.new_link_href_root)
