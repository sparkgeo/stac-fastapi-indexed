# Known Issues

## CQL2-TEXT Search Filters

The filter extension's [documentation](https://github.com/stac-api-extensions/filter/blob/e836a3e95e8541c2f648db6773a998771e6f726f/README.md#get-query-parameters-and-post-json-fields) requires that GET search requests default to the CQL2-TEXT filter language. This repo uses [pygeofilter](https://github.com/geopython/pygeofilter) to parse filter strings, and pygeofilter does not fully support CQL2-TEXT. There is not currently an exhaustive list of unsupported features, but [this issue](https://github.com/geopython/pygeofilter/issues/105) starts a discussion on that subject.
