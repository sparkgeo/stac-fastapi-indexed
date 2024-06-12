from enum import Enum


class SearchDirection(Enum):
    Next = 0
    Previous = 1


class SearchMethod(Enum):
    GET = 0
    POST = 1
