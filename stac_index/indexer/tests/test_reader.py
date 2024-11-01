import json
import unittest
from types import SimpleNamespace
from typing import List
from unittest.mock import Mock

from pydantic_core import Url, ValidationError
from stac_pydantic import Item

from stac_index.indexer.reader.reader import _expand_relative_links, _read_item


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


class ReadItemtest(unittest.TestCase):
    def test_read_basic_item(self):
        (item, _) = _read_item(json.loads(basic_item_json))
        assert item.id == "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e"
        assert item.type == "Feature"
        assert len(item.links) == 1
        assert (
            item.links[0].href
            == "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
        )
        assert len(item.stac_extensions) == 2
        assert (
            Url("https://stac-extensions.github.io/eo/v1.0.0/schema.json")
            in item.stac_extensions
        )
        assert (
            Url("https://stac-extensions.github.io/projection/v1.0.0/schema.json")
            in item.stac_extensions
        )

    def test_read_item_with_invalid_extension(self):
        self.assertRaises(
            ValidationError,
            lambda: Item(**json.loads(item_with_invalid_extension_json)),
        )
        (item, filtered) = _read_item(json.loads(item_with_invalid_extension_json))
        Item(**filtered)
        assert item.id == "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e"
        assert item.type == "Feature"
        assert len(item.links) == 1
        assert (
            item.links[0].href
            == "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
        )
        assert len(item.stac_extensions) == 1
        assert (
            Url("https://stac-extensions.github.io/projection/v1.0.0/schema.json")
            in item.stac_extensions
        )


basic_item_json = """{
  "id": "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e",
  "type": "Feature",
  "collection": "joplin",
  "links": [
    {
      "rel": "self",
      "type": "application/geo+json",
      "href": "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
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


item_with_invalid_extension_json = """{
  "id": "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e",
  "type": "Feature",
  "collection": "joplin",
  "links": [
    {
      "rel": "self",
      "type": "application/geo+json",
      "href": "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
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
    "eo",
    "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
  ],
  "stac_version": "1.0.0"
}"""
