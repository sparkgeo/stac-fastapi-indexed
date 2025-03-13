import json
import unittest

from pydantic_core import ValidationError
from stac_pydantic import Item

from stac_index.common.indexing_error import IndexingErrorType
from stac_index.common.stac_parser import (
    EOExtensionUriFixer,
    StacParser,
    StacParserException,
)


class StacParserTest(unittest.TestCase):
    def test_read_basic_item(self):
        target = StacParser([])
        (item, _) = target.parse_stac_item(json.loads(basic_item_json))
        assert item.id == "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e"
        assert item.type == "Feature"
        assert len(item.links) == 1
        assert (
            item.links[0].href
            == "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
        )
        assert len(item.stac_extensions) == 2
        assert "https://stac-extensions.github.io/eo/v1.0.0/schema.json" in [
            str(url) for url in item.stac_extensions
        ]
        assert "https://stac-extensions.github.io/projection/v1.0.0/schema.json" in [
            str(url) for url in item.stac_extensions
        ]

    def test_read_item_with_invalid_extension_with_fixer(self):
        self.assertRaises(
            ValidationError,
            lambda: Item(**json.loads(item_with_invalid_extension_json)),
        )
        target = StacParser([])
        with self.assertRaises(StacParserException) as context_manager:
            target.parse_stac_item(json.loads(item_with_invalid_extension_json))
        raised_exception = context_manager.exception
        assert len(raised_exception.indexing_errors) == 1
        indexing_error = raised_exception.indexing_errors[0]
        assert indexing_error.type == IndexingErrorType.item_parsing
        assert indexing_error.possible_fixes == EOExtensionUriFixer.name()

        target = StacParser([EOExtensionUriFixer.name()])
        (item, fields) = target.parse_stac_item(
            json.loads(item_with_invalid_extension_json)
        )
        assert item.id == "386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e"
        assert item.type == "Feature"
        assert len(item.links) == 1
        assert (
            item.links[0].href
            == "/data/collections/joplin/items/386dfa13-c2b4-4ce6-8e6f-fcac73f4e64e.json"
        )
        assert len(item.stac_extensions) == 2
        assert "https://stac-extensions.github.io/projection/v1.0.0/schema.json" in [
            str(url) for url in item.stac_extensions
        ]
        assert "https://stac-extensions.github.io/eo/v1.0.0/schema.json" in [
            str(url) for url in item.stac_extensions
        ]

    def test_read_item_with_invalid_extension_without_fixer(self):
        self.assertRaises(
            ValidationError,
            lambda: Item(**json.loads(item_with_invalid_extension_json)),
        )
        target = StacParser([])
        with self.assertRaises(StacParserException) as context_manager:
            target.parse_stac_item(json.loads(item_with_invalid_extension_json))
        raised_exception = context_manager.exception
        assert len(raised_exception.indexing_errors) == 1
        indexing_error = raised_exception.indexing_errors[0]
        assert indexing_error.type == IndexingErrorType.item_parsing
        assert indexing_error.possible_fixes == EOExtensionUriFixer.name()


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
