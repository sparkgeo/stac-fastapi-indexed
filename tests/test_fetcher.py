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
    fetchone_mock.return_value = [None, "matching STAC item uri", ""]
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
async def test_get_single_item_content_success() -> None:
    assert False


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.stac.fetcher.format_query_object_name")
@mock.patch("stac_fastapi.indexed.stac.fetcher.get_reader_for_uri")
@mock.patch("stac_fastapi.indexed.stac.fetcher.fetchone")
async def test_get_single_item_with_fetch_indexed_but_missing(
    fetchone_mock: mock.AsyncMock,
    get_reader_for_uri_mock: mock.MagicMock,
    *args,
) -> None:
    fetchone_mock.return_value = [None, "matching STAC item uri", ""]
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
async def test_get_single_collection_content_success() -> None:
    assert False


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
