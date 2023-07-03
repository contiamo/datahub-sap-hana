from dataclasses import dataclass

from serde import serde
from sqlglot.lineage import Node


def parse_column_name(column_name: str):
    """
    Removes the table/name and alias from the column name returned by sqlglot,
    eg: instead of 'hotel.room', it will return 'room'.
    """
    parsed_name = column_name.split(".")
    return parsed_name[-1]


@serde
@dataclass
class Table:
    schema: str
    name: str


@serde
@dataclass
class View(Table):
    sql: str


@serde
@dataclass
class ColumnField:
    """
    ColumnField contains the metadata to describe a column in a table.

    Use `ColumnField.from_node(lineage_node)` to create a ColumnField
    from a sqlglot node.
    """

    name: str
    dataset: Table

    @classmethod
    def from_node(cls, node: Node, schema: str):
        """Creates a ColumnField from a sqlglot node."""
        return cls(
            name=parse_column_name(node.name),
            dataset=Table(schema=schema, name=node.source.name),
        )


class UpstreamLineageField(ColumnField):
    """UpstreamField contains the metadata to describe the
    upstream column (source) lineage of a DownstreamField.
    """

    pass


class DownstreamLineageField(ColumnField):
    """DownstreamField contains the metadata to describe the
    downstream column (target) lineage of a column in a table.
    """

    pass
