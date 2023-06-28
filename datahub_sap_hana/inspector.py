

from functools import cache
from typing import Dict, List, Optional, Protocol, TypedDict


ColumnDescription = TypedDict(
    "ColumnDescription", 
    {
        "name": str, 
        "type": str, 
        "nullable": bool,
        "default": Optional[str],
        "comment": Optional[str],
    })


class Inspector(Protocol):
    """
    A protocol describing the required methods from the sqlalchemy.engine.reflection.Inspector class.
    """
    def get_columns(self, table_name: str, schema: Optional[str] = None) -> List[ColumnDescription]:
        ...

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        ...

    def get_schema_names(self) -> List[str]:
        ...

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        ...

    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        ...



class CachedInspector:
    def __init__(self, inspector: Inspector):
        self.inspector = inspector

    @cache
    def get_columns(self, table_name: str, schema: Optional[str] = None) -> List[ColumnDescription]:
        return self.inspector.get_columns(table_name, schema)

    @cache
    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, ColumnDescription]:
        return {
            column['name'].lower(): column
            for column in self.get_columns(table_name, schema)
        }

    @cache
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        return self.inspector.get_table_names(schema)

    @cache
    def get_schema_names(self) -> List[str]:
        return self.inspector.get_schema_names()

    @cache
    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        return self.inspector.get_view_names(schema)

    @cache
    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        return self.inspector.get_view_definition(view_name, schema)
