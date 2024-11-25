from dataclasses import dataclass


@dataclass
class AttributeConfig:
    name: str
    items_column: str
    items_column_type: str
    is_geometry: bool
    is_temporal: bool
