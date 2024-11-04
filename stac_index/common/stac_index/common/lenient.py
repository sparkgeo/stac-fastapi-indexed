import copy
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError


def _remove_item(data, path) -> bool:
    """Remove item at `path` from `data`.

    `data` must a subscriptable collection, such as a dict or list, and may
     contain other subscriptable collections.

    `path` must be a tuple describing a path through `data`. That is, `path[0]`
    should be a valid index into `data`, `path[1]` should be a valid index into
    `data[path[0]]`, etc.

    The item pointed to by `path` will be removed from `data`.
    """
    try:
        path_head = path[0]
        path_rest = path[1:]
        if len(path_rest) > 0:
            return _remove_item(data[path_head], path_rest)
        else:
            del data[path_head]
            return True
    except IndexError:
        return False


PydanticClass = TypeVar("PydanticClass", bound=BaseModel)


def read_pydantic_class(
    fields: dict[str, Any], cls: PydanticClass
) -> tuple[PydanticClass, dict[str, Any]]:
    """Atempt to convert `field` into `Item` in a fault-tolerant way.

    Attempts to construct a `PydanticClass` from `fields`. If there is a
    `ValidationError`, then remove the sources of the errors from `fields`
    and attempt the conversion again.
    """
    try:
        return (cls(**fields), fields)
    except ValidationError as e:
        reduced_fields = copy.deepcopy(fields)
        items_removed = False
        for error in e.errors():
            if _remove_item(reduced_fields, error["loc"]):
                items_removed = True
        if items_removed:
            return read_pydantic_class(reduced_fields, cls)
        else:
            raise
