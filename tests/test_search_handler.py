from types import SimpleNamespace
from unittest import mock

import pytest
from common import monkeypatch_settings

search_handler = None


@pytest.fixture(autouse=True)
def setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch_settings(monkeypatch)


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.search.search_handler.get_last_load_id")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_search_link")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_catalog_link")
@mock.patch("stac_fastapi.indexed.search.search_handler.StacParser")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_sortable_configs_by_field")
@mock.patch("stac_fastapi.indexed.search.search_handler.fix_item_links")
@mock.patch("stac_fastapi.indexed.search.search_handler.fetch_dict")
@mock.patch("stac_fastapi.indexed.search.search_handler.fetchall")
async def test_search_multi_item_success(
    fetchall_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    fix_item_links_mock: mock.MagicMock,
    get_sortable_configs_by_field_mock: mock.MagicMock,
    stac_parser_mock: mock.MagicMock,
    *args,
) -> None:
    from stac_fastapi.indexed.search.search_handler import SearchHandler

    fetchall_mock.return_value = [["", ""], ["", ""]]
    fetch_dict_mock.side_effect = [
        {"id": "mock item 1"},
        {"id": "mock item 2"},
    ]
    fixed_items_mock_value = [
        SimpleNamespace(id="mock fixed item 1"),
        SimpleNamespace(id="mock fixed item 2"),
    ]
    fix_item_links_mock.side_effect = fixed_items_mock_value
    get_sortable_configs_by_field_mock.return_value = {
        "collection": SimpleNamespace(items_column="col1"),
        "id": SimpleNamespace(items_column="col2"),
    }
    stac_parser_mock.side_effect = [
        SimpleNamespace(
            parse_stac_item=mock.Mock(return_value=[None, {}]),
        )
        for _ in range(len(fetchall_mock.return_value))
    ]
    result = await SearchHandler(
        search_request=SimpleNamespace(
            token=None,
            ids=None,
            collections=None,
            bbox=None,
            intersects=None,
            datetime=None,
            filter=None,
            sortby=None,
            limit=10,
        ),
        request=SimpleNamespace(),
    ).search()
    assert sorted(result["features"], key=lambda x: x.id) == sorted(
        fixed_items_mock_value, key=lambda x: x.id
    )


@pytest.mark.asyncio
@mock.patch("stac_fastapi.indexed.search.search_handler.get_last_load_id")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_search_link")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_catalog_link")
@mock.patch("stac_fastapi.indexed.search.search_handler.StacParser")
@mock.patch("stac_fastapi.indexed.search.search_handler.get_sortable_configs_by_field")
@mock.patch("stac_fastapi.indexed.search.search_handler.fix_item_links")
@mock.patch("stac_fastapi.indexed.search.search_handler.fetch_dict")
@mock.patch("stac_fastapi.indexed.search.search_handler.fetchall")
async def test_search_multi_item_partial_indexed_but_missing(
    fetchall_mock: mock.AsyncMock,
    fetch_dict_mock: mock.AsyncMock,
    fix_item_links_mock: mock.MagicMock,
    get_sortable_configs_by_field_mock: mock.MagicMock,
    stac_parser_mock: mock.MagicMock,
    *args,
) -> None:
    from stac_index.io.readers.exceptions import UriNotFoundException

    from stac_fastapi.indexed.search.search_handler import SearchHandler

    fetchall_mock.return_value = [["", ""], ["", ""]]
    fetch_dict_mock.side_effect = [
        {"id": "mock item 1"},
        UriNotFoundException("uri"),
    ]
    fixed_items_mock_value = [SimpleNamespace(id="mock fixed item 1")]
    fix_item_links_mock.side_effect = fixed_items_mock_value
    get_sortable_configs_by_field_mock.return_value = {
        "collection": SimpleNamespace(items_column="col1"),
        "id": SimpleNamespace(items_column="col2"),
    }
    stac_parser_mock.side_effect = [
        SimpleNamespace(
            parse_stac_item=mock.Mock(return_value=[None, {}]),
        )
        for _ in range(len(fetchall_mock.return_value))
    ]
    result = await SearchHandler(
        search_request=SimpleNamespace(
            token=None,
            ids=None,
            collections=None,
            bbox=None,
            intersects=None,
            datetime=None,
            filter=None,
            sortby=None,
            limit=10,
        ),
        request=SimpleNamespace(),
    ).search()
    assert len(result["features"]) == 1
    assert result["features"][0] == fixed_items_mock_value[0]
