from types import SimpleNamespace
from typing import cast
from unittest import mock

import pytest
from fastapi import Request
from stac_fastapi.types.errors import NotFoundError
from stac_index.io.readers.exceptions import UriNotFoundException

from stac_fastapi.indexed.stac.fetcher import (
    get_all_collections,
    get_single_collection,
    get_single_item,
)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.Item")
@mock.patch("stac_fastapi.indexed.stac.fetcher.StacParser")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_item_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_item_with_fetch_success(
    fetchone_mock: mock.AsyncMock,
    fix_item_links_mock: mock.MagicMock,
    stac_parser_mock: mock.MagicMock,
    item_mock: mock.MagicMock,
    get_reader_for_uri_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [None, "matching STAC item uri", None]
    fix_item_links_mock.return_value = "fixed item"
    stac_parser_mock.return_value = SimpleNamespace(
        parse_stac_item=mock.Mock(return_value=[{}])
    )
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock()
    )
    assert (
        await get_single_item(
            collection_id="test collection id",
            item_id="test item id",
            request=cast(Request, SimpleNamespace()),
        )
        == fix_item_links_mock.return_value
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.Item")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_item_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_item_content_success(
    fetchone_mock: mock.AsyncMock,
    fix_item_links_mock: mock.MagicMock,
    item_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [valid_item_json, "irrelevant uri", ""]
    fix_item_links_mock.return_value = "fixed item"
    assert (
        await get_single_item(
            collection_id="test collection id",
            item_id="test item id",
            request=cast(Request, SimpleNamespace()),
        )
        == fix_item_links_mock.return_value
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_item_with_fetch_indexed_but_missing(
    fetchone_mock: mock.AsyncMock,
    get_reader_for_uri_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [None, "matching STAC item uri", None]
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock(side_effect=UriNotFoundException("uri"))
    )
    with pytest.raises(NotFoundError) as e:
        await get_single_item(
            collection_id="test collection id",
            item_id="test item id",
            request=cast(Request, SimpleNamespace()),
        )
    assert "index is outdated" in str(e.value)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_collection_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_collection_with_fetch_success(
    fetchone_mock: mock.AsyncMock,
    fix_collection_links_mock: mock.MagicMock,
    get_reader_for_uri_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [None, "matching STAC collection uri"]
    fix_collection_links_mock.return_value = "fixed collection"
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock(return_value={})
    )
    assert (
        await get_single_collection(
            collection_id="test collection id",
            request=cast(Request, SimpleNamespace()),
        )
        == fix_collection_links_mock.return_value
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.Collection")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_collection_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_collection_content_success(
    fetchone_mock: mock.AsyncMock,
    fix_collection_links_mock: mock.MagicMock,
    collection_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [valid_collection_json, "irrelevant uri"]
    fix_collection_links_mock.return_value = "fixed collection"
    assert (
        await get_single_collection(
            collection_id="test collection id",
            request=cast(Request, SimpleNamespace()),
        )
        == fix_collection_links_mock.return_value
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_collection_with_fetch_indexed_but_missing(
    fetchone_mock: mock.AsyncMock,
    get_reader_for_uri_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [None, "matching STAC collection uri"]
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock(side_effect=UriNotFoundException("uri"))
    )
    with pytest.raises(NotFoundError) as e:
        await get_single_collection(
            collection_id="test collection id",
            request=cast(Request, SimpleNamespace()),
        )
    assert "index is outdated" in str(e.value)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_collection_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchall")
async def test_all_collections_with_fetch_success(
    fetchall_mock: mock.AsyncMock,
    get_reader_for_uri_mock: mock.MagicMock,
    fix_collection_links_mock: mock.MagicMock,
    *args,
) -> None:
    fetchall_mock.return_value = [[None, ""], [None, ""]]
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock(
            side_effect=[
                {"id": "mock collection 1"},
                {"id": "mock collection 2"},
            ]
        )
    )
    fixed_collections_mock_value = [
        SimpleNamespace(id="mock fixed collection 1"),
        SimpleNamespace(id="mock fixed collection 2"),
    ]
    fix_collection_links_mock.side_effect = fixed_collections_mock_value
    result = await get_all_collections(
        request=cast(
            Request,
            SimpleNamespace(),
        )
    )
    assert sorted(result, key=lambda x: x.id) == sorted(
        fixed_collections_mock_value, key=lambda x: x.id
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fix_collection_links")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchall")
async def test_all_collections_with_fetch_partial_indexed_but_missing(
    fetchall_mock: mock.AsyncMock,
    get_reader_for_uri_mock: mock.MagicMock,
    fix_collection_links_mock: mock.MagicMock,
    *args,
) -> None:
    fetchall_mock.return_value = [[None, ""], [None, ""]]
    get_reader_for_uri_mock.return_value = SimpleNamespace(
        load_json_from_uri=mock.AsyncMock(
            side_effect=[
                {"id": "mock collection 1"},
                UriNotFoundException("uri"),
            ]
        )
    )
    fixed_collections_mock_value = [SimpleNamespace(id="mock fixed collection 1")]
    fix_collection_links_mock.side_effect = fixed_collections_mock_value
    result = await get_all_collections(
        request=cast(
            Request,
            SimpleNamespace(),
        )
    )
    assert len(result) == 1
    assert result[0] == fixed_collections_mock_value[0]


valid_item_json = """{
  "id": "valid-item",
  "type": "Feature",
  "collection": "collection",
  "links": [
    {
      "rel": "self",
      "type": "application/geo+json",
      "href": "/data/collections/collection/items/valid-item.json"
    }
  ],
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [
          -94.4934082,
          37.1055746
        ],
        [
          -94.4934082,
          37.0792845
        ],
        [
          -94.4604492,
          37.0792845
        ],
        [
          -94.4604492,
          37.1055746
        ],
        [
          -94.4934082,
          37.1055746
        ]
      ]
    ]
  },
  "properties": {
    "proj:epsg": 3857,
    "orientation": "nadir",
    "height": 2500,
    "width": 2500,
    "datetime": "2000-02-10T00:00:00Z",
    "gsd": 0.5971642834779395
  },
  "assets": {
    "COG": {
      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
      "href": "https://arturo-stac-api-test-data.s3.amazonaws.com/joplin/images/may24C367500e4107500n.tif",
      "title": "NOAA STORM COG"
    }
  },
  "bbox": [
    -94.4934082,
    37.0792845,
    -94.4604492,
    37.1055746
  ],
  "stac_extensions": [
    "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
  ],
  "stac_version": "1.0.0"
}"""

valid_collection_json = """{
  "id": "collection",
  "description": "Based on Joplin collection",
  "stac_version": "1.0.0",
  "license": "public-domain",
  "links": [
    {
      "rel": "license",
      "href": "https://creativecommons.org/licenses/publicdomain/",
      "title": "public domain"
    },
    {
      "rel": "items",
      "type": "application/geo+json",
      "href": "/data/collections/joplin/items"
    }
  ],
  "type": "Collection",
  "extent": {
    "spatial": {
      "bbox": [
        [
          -94.6911621,
          37.0332547,
          -94.402771,
          37.1077651
        ]
      ]
    },
    "temporal": {
      "interval": [
        [
          "2000-02-01T00:00:00Z",
          "2000-02-12T00:00:00Z"
        ]
      ]
    }
  }
}"""
