from enum import Enum


class SearchDirection(Enum):
    Next = 0
    Previous = 1


class SearchMethod(str, Enum):
    GET = "GET"
    POST = "POST"

    @classmethod
    def from_str(cls, method: str):
        for member in cls:
            if member.value == method.upper():
                return member
        raise ValueError(f"{method} is not a valid {cls.__name__}")
