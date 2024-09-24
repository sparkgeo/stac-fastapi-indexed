from types import SimpleNamespace
from typing import List
from unittest.mock import Mock

from stac_index.indexer.reader.reader import _expand_relative_links


def _get_link_provider(links: List[str]) -> SimpleNamespace:
    return SimpleNamespace(
        links=SimpleNamespace(
            link_iterator=Mock(
                return_value=[SimpleNamespace(href=link) for link in links]
            )
        )
    )


def test_expand_relative_links_1():
    provider_href = "https://domain.ca:8999/1/2/root.json"
    link_1_relative = "./3/4.json"
    link_1_absolute = "https://domain.ca:8999/1/2/3/4.json"
    link_provider = _get_link_provider([link_1_relative])
    _expand_relative_links(link_provider, provider_href)
    assert link_provider.links.link_iterator()[0].href == link_1_absolute


def test_expand_relative_links_2():
    provider_href = "https://domain.ca:8999/one/two/three/four"
    link_1_relative = "../../two.json"
    link_1_absolute = "https://domain.ca:8999/one/two.json"
    link_provider = _get_link_provider([link_1_relative])
    _expand_relative_links(link_provider, provider_href)
    assert link_provider.links.link_iterator()[0].href == link_1_absolute


def test_expand_relative_links_3():
    provider_href = "https://domain.ca:8999/a/b/c/d/e?f=g&h=i"
    link_1_relative = ".././../cdefghi"
    link_1_absolute = "https://domain.ca:8999/a/b/cdefghi"
    link_provider = _get_link_provider([link_1_relative])
    _expand_relative_links(link_provider, provider_href)
    assert link_provider.links.link_iterator()[0].href == link_1_absolute
