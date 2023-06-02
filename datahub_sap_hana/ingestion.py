from typing import Any, Dict

import sqlalchemy_hana.types as custom_types  # type: ignore
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    config_class,  # type: ignore
    platform_name,  # type: ignore
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    register_custom_type,  # type: ignore
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.metadata.com.linkedin.pegasus2avro import schema

register_custom_type(custom_types.TINYINT, schema.NumberType)


class HanaConfig(BasicSQLAlchemyConfig):
    scheme = "hana"

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


@platform_name("SAPHana")
@config_class(HanaConfig)  # type: ignore
class HanaSource(SQLAlchemySource):
    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "hana")

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)
