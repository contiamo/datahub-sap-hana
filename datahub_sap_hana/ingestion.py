import sqlalchemy_hana.types as custom_types
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    register_custom_type,
)

from datahub.metadata.com.linkedin.pegasus2avro import schema

register_custom_type(custom_types.TINYINT, schema.NumberType)


class HanaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "hana"

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class HanaSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "hana")

    @classmethod
    def create(cls, config_dict, ctx):
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)
