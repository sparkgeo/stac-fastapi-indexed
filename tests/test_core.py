from types import SimpleNamespace
from typing import cast
from unittest import mock

import pytest
from common import monkeypatch_settings
from fastapi import Request
from stac_fastapi.types.errors import NotFoundError

core = None


@pytest.fixture(autouse=True)
def setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch_settings(monkeypatch)

    from stac_fastapi.indexed.core import CoreCrudClient

    global core
    core = CoreCrudClient()


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.Item")
@mock.patch("stac_fastapi.indexed.core.StacParser")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fix_item_links")
@mock.patch("stac_fastapi.indexed.core.fetchone")
async def test_get_item_success(
    fetchone_mock: mock.AsyncMock,
    fix_item_links_mock: mock.MagicMock,
    fetch_dict_mock: mock.AsyncMock,
    stac_parser_mock: mock.MagicMock,
    item_mock: mock.MagicMock,
    *args,
) -> None:
    assert core is not None, "init failure"
    fetchone_mock.return_value = ["matching STAC item uri", ""]
    fix_item_links_mock.return_value = "fixed item"
    fetch_dict_mock.return_value = {}
    stac_parser_mock.return_value = SimpleNamespace(
        parse_stac_item=mock.Mock(return_value=[0, 1])
    )
    with mock.patch.object(core, "get_collection"):
        assert (
            await core.get_item(
                item_id="test item id",
                collection_id="test collection id",
                request=cast(Request, SimpleNamespace()),
            )
            == fix_item_links_mock.return_value
        )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fetchone")
async def test_get_item_indexed_but_missing(
    fetchone_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    *args,
) -> None:
    from stac_index.io.readers.exceptions import UriNotFoundException

    assert core is not None, "init failure"
    fetchone_mock.return_value = ["matching STAC item uri", ""]
    fetch_dict_mock.side_effect = UriNotFoundException("uri")
    with mock.patch.object(core, "get_collection"):
        with pytest.raises(NotFoundError) as e:
            await core.get_item(
                item_id="test item id",
                collection_id="test collection id",
                request=cast(Request, SimpleNamespace()),
            )
        assert "index is outdated" in str(e.value)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fix_collection_links")
@mock.patch("stac_fastapi.indexed.core.fetchone")
async def test_get_collection_success(
    fetchone_mock: mock.AsyncMock,
    fix_collection_links_mock: mock.MagicMock,
    fetch_dict_mock: mock.AsyncMock,
    *args,
) -> None:
    assert core is not None, "init failure"
    fetchone_mock.return_value = ["matching STAC collection uri", ""]
    fix_collection_links_mock.return_value = "fixed collection"
    fetch_dict_mock.return_value = {}
    assert (
        await core.get_collection(
            collection_id="test collection id",
            request=cast(Request, SimpleNamespace()),
        )
        == fix_collection_links_mock.return_value
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fetchone")
async def test_get_collection_indexed_but_missing(
    fetchone_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    *args,
) -> None:
    from stac_index.io.readers.exceptions import UriNotFoundException

    assert core is not None, "init failure"
    fetchone_mock.return_value = ["matching STAC collection uri", ""]
    fetch_dict_mock.side_effect = UriNotFoundException("uri")
    with pytest.raises(NotFoundError) as e:
        await core.get_collection(
            collection_id="test collection id",
            request=cast(Request, SimpleNamespace()),
        )
    assert "index is outdated" in str(e.value)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.get_collections_link")
@mock.patch("stac_fastapi.indexed.core.get_catalog_link")
@mock.patch("stac_fastapi.indexed.core.fix_collection_links")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fetchall")
async def test_all_collections_success(
    fetchall_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    fix_collection_links_mock: mock.MagicMock,
    *args,
) -> None:
    assert core is not None, "init failure"
    fetchall_mock.return_value = [[""], [""]]
    fetch_dict_mock.side_effect = [
        {"id": "mock collection 1"},
        {"id": "mock collection 2"},
    ]
    fixed_collections_mock_value = [
        SimpleNamespace(id="mock fixed collection 1"),
        SimpleNamespace(id="mock fixed collection 2"),
    ]
    fix_collection_links_mock.side_effect = fixed_collections_mock_value
    result = await core.all_collections(
        request=cast(
            Request,
            SimpleNamespace(
                url=SimpleNamespace(path="/collections"),
                scope={"app": SimpleNamespace(root_path="")},
            ),
        )
    )
    assert sorted(result["collections"], key=lambda x: x.id) == sorted(
        fixed_collections_mock_value, key=lambda x: x.id
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.core.format_query_object_name")
@mock.patch("stac_fastapi.indexed.core.get_collections_link")
@mock.patch("stac_fastapi.indexed.core.get_catalog_link")
@mock.patch("stac_fastapi.indexed.core.fix_collection_links")
@mock.patch("stac_fastapi.indexed.core.fetch_dict")
@mock.patch("stac_fastapi.indexed.core.fetchall")
async def test_all_collections_partial_indexed_but_missing(
    fetchall_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    fix_collection_links_mock: mock.MagicMock,
    *args,
) -> None:
    from stac_index.io.readers.exceptions import UriNotFoundException

    assert core is not None, "init failure"
    fetchall_mock.return_value = [[""], [""]]
    fetch_dict_mock.side_effect = [
        {"id": "mock collection 1"},
        UriNotFoundException("uri"),
    ]
    fixed_collections_mock_value = [SimpleNamespace(id="mock fixed collection 1")]
    fix_collection_links_mock.side_effect = fixed_collections_mock_value
    result = await core.all_collections(
        request=cast(
            Request,
            SimpleNamespace(
                url=SimpleNamespace(path="/collections"),
                scope={"app": SimpleNamespace(root_path="")},
            ),
        )
    )
    assert len(result["collections"]) == 1
    assert result["collections"][0] == fixed_collections_mock_value[0]
