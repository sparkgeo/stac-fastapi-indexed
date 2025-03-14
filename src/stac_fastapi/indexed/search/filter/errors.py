from typing import Any


class UnknownField(Exception):
    def __init__(self, field_name: str):
        super().__init__(self)
        self.field_name = field_name


class NotAGeometryField(Exception):
    def __init__(self, argument: Any):
        super().__init__(self)
        self.argument = argument


class NotATemporalField(Exception):
    def __init__(self, argument: Any):
        super().__init__(self)
        self.argument = argument


class UnknownFunction(Exception):
    def __init__(self, function_name: str):
        super().__init__(self)
        self.function_name = function_name
