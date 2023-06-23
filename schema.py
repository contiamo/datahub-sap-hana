from typing import List, Optional

from pydantic import BaseModel


class Entity(BaseModel):
    """
    Represents an entity, which is typically a dataset.

    Attributes:
    -----------
    name : str
        Name of the entity.
    platform : str
        Platform of the entity.
    env : Optional[str], default="PROD"
        Environment of the entity.
    """

    name: str
    platform: str
    env: Optional[str] = "PROD"


class Field(BaseModel):
    # Name of the field
    name: str


class UpstreamField(BaseModel):
    """
    Represents an upstream field that feeds into a column.

    Attributes:
    -----------
    name : str
        Name of the upstream field.
    entity : Entity
        Entity of the upstream field, which is typically a dataset.
    """

    name: str
    entity: Entity


class Upstream(BaseModel):
    """
    Represents an upstream column or dataset that feeds into a field.

    Attributes:
    -----------
    transform : Optional[str], default=None
        Optional transformation applied to the upstream data.
    fields : List[Field]
        List of upstream fields.
    entity : Entity
        Upstream entity, which is typically a dataset.
    """

    transform: Optional[str] = None
    fields: List[UpstreamField]


class ColumnLineage(BaseModel):
    # List of fields in the column lineage
    fields: List[Field]
    # Upstream of the column lineage
    upstream: Upstream


class LineageNode(BaseModel):
    """
    Represents a lineage, which is a list of column lineages that define the
    source and transformation of the column(s) in a dataset.

    Attributes:
    -----------
    column_lineage : List[ColumnLineage]
        List of column lineages.
    entity : Entity
        Entity of the lineage, which is typically a dataset.
    """

    column_lineage: List[ColumnLineage]
    entity: Entity


class Lineage(BaseModel):
    """
    Represents a lineage file, which is a versioned list of fine grained lineages.

    Attributes:
    -----------
    version : int
        Version of the lineage file.
    lineage : List[Lineage]
        List of lineages.
    """

    version: int
    lineage: List[LineageNode]
