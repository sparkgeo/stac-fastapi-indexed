from typing import Final, List

from pydantic import BaseModel


class SortableField(BaseModel):
    title: str
    description: str


class SortablesResponse(BaseModel):
    title: Final[str] = "STAC Sortables"
    fields: List[SortableField]
