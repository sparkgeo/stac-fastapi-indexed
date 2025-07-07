from typing import Any, Callable, Final, Self, cast

from pydantic import BaseModel, model_serializer


class SortableField(BaseModel):
    type: str


class SortablesResponse(BaseModel):
    title: Final[str] = "Sortables"
    properties: dict[str, SortableField]

    @model_serializer(mode="wrap")
    def serialize_model(self: Self, serializer: Callable) -> dict[str, Any]:
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://example.com/sortables",
            "title": "Sortables",
            "type": "object",
            **cast(dict[str, Any], serializer(self)),
        }
