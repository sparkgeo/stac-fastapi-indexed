from concurrent.futures import ThreadPoolExecutor
from json import dumps
from os import environ
from statistics import mean
from threading import Lock
from time import time
from typing import Any, Callable, Dict, Final, List

import requests

_iterations: Final[int] = int(environ.get("BENCHMARK_ITERATIONS", 10))
_collection_test_limit: Final[int] = int(
    environ.get("BENCHMARK_COLLECTION_TEST_LIMIT", -1)
)
_max_concurrency: Final[int] = int(environ.get("BENCHMARK_MAX_CONCURRENCY", 1))
_results_lock: Final[Lock] = Lock()


def execute(root_url: str) -> None:
    print(
        f"collection limit: {_collection_test_limit}, iterations: {_iterations}, max concurrency: {_max_concurrency}"
    )
    results: Dict[str, List[float]] = {}
    print("fetching all collection IDs")
    all_collections = requests.get(f"{root_url}/collections").json()["collections"]
    if _collection_test_limit > -1:
        all_collections = all_collections[:_collection_test_limit]
    tests: Dict[str, List[Callable[[], None]]] = {
        "all-collections": create_all_collections_request(root_url),
        "collection-detail": create_collection_detail_requests(
            root_url, all_collections
        ),
        "collection-items-first-page": create_collection_items_requests(
            root_url, all_collections
        ),
        "search-blank": create_blank_search_first_page_requests(root_url),
        "collection-search-blank": create_collection_search_requests(
            root_url, all_collections
        ),
        "collection-search-bbox": create_collection_search_spatial_bbox(
            root_url, all_collections
        ),
    }

    def execute_tests_serially(iteration: int) -> None:
        for test_name, fns in tests.items():
            with _results_lock:
                if test_name not in results:
                    results[test_name] = []
            for fn in fns:
                print(f"...testing {test_name} (iteration {iteration})")
                start = time()
                fn()
                with _results_lock:
                    results[test_name].append(time() - start)

    with ThreadPoolExecutor(max_workers=_max_concurrency) as executor:
        executor.map(execute_tests_serially, [i + 1 for i in range(_iterations)])

    print(
        dumps(
            {test_name: round(mean(times), 2) for test_name, times in results.items()},
            indent=2,
        )
    )


def create_all_collections_request(root_url: str) -> List[Callable[[], None]]:
    def fn():
        requests.get(f"{root_url}/collections")

    return [fn]


def create_collection_detail_requests(
    root_url: str, collections: List[Dict[str, Any]]
) -> List[Callable[[], None]]:
    def create(collection_id: str):
        def fn():
            requests.get(f"{root_url}/collections/{collection_id}")

        return fn

    return [create(collection["id"]) for collection in collections]


def create_collection_items_requests(
    root_url: str, collections: List[Dict[str, Any]]
) -> List[Callable[[], None]]:
    def create(collection_id: str):
        def fn():
            requests.get(f"{root_url}/collections/{collection_id}/items")

        return fn

    return [create(collection["id"]) for collection in collections]


def create_blank_search_first_page_requests(root_url: str) -> List[Callable[[], None]]:
    def fn():
        requests.post(f"{root_url}/search", json={})

    return [fn]


def create_collection_search_requests(
    root_url: str, collections: List[Dict[str, Any]]
) -> List[Callable[[], None]]:
    def create(collection_id: str):
        def fn():
            requests.post(f"{root_url}/search", json={"collections": [collection_id]})

        return fn

    return [create(collection["id"]) for collection in collections]


def create_collection_search_spatial_bbox(
    root_url: str, collections: List[Dict[str, Any]]
) -> List[Callable[[], None]]:
    def create(collection: Dict[str, Any]):
        full_extent: List[float] = collection["extent"]["spatial"]["bbox"][0]
        if len(full_extent) == 6:
            full_extent = [
                full_extent[0],
                full_extent[1],
                full_extent[4],
                full_extent[5],
            ]
        full_x_range = full_extent[2] - full_extent[0]
        full_y_range = full_extent[3] - full_extent[1]
        bbox = [
            full_extent[0] + full_x_range / 10 if full_x_range > 0 else 0,
            full_extent[1] + full_y_range / 10 if full_y_range > 0 else 0,
            full_extent[2] - full_x_range / 10 if full_x_range > 0 else 0,
            full_extent[3] - full_y_range / 10 if full_y_range > 0 else 0,
        ]

        def fn():
            requests.post(
                f"{root_url}/search",
                json={"collections": [collection["id"]], "bbox": bbox},
            )

        return fn

    return [create(collection) for collection in collections]


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "root_url",
        type=str,
        help="Root (catalog response) path for the API, no trailing slash",
    )
    args = parser.parse_args()
    execute(args.root_url)
