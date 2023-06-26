from typing import List, Optional

from pydantic import BaseModel


class Column(BaseModel):
    """
    Represents a column and its metadata.

    Attributes:
    -----------
    name : str
        Name of the column.
    platform : str
        Platform of the column.
    env : Optional[str], default="PROD"
        Environment of the column.
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
    column : Column
        Column metadata of the upstream field
    """

    name: str
    column: Column


class Upstream(BaseModel):
    """
    Represents an upstream column or dataset that feeds into a field.

    Attributes:
    -----------
    transform : Optional[str], default=None
        Optional transformation applied to the upstream data.
    fields : List[Field]
        List of upstream fields.
    column : Column
        Upstream column name and metadata
    """

    transform: Optional[str] = None
    fields: List[UpstreamField]
