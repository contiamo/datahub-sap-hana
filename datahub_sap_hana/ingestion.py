import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple, Union

import sqlalchemy_hana.types as custom_types  # type: ignore
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    config_class,  # type: ignore
    platform_name,  # type: ignore
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.metadata.com.linkedin.pegasus2avro import schema
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

register_custom_type(custom_types.TINYINT, schema.NumberType)


logger: logging.Logger = logging.getLogger(__name__)

# This query only takes 4 columns to match the fields in the ViewLineageEntry class. It also ignores schemas that contain SYS
# Object dependencies in SAP HANA https://help.sap.com/docs/SAP_HANA_PLATFORM/de2486ee947e43e684d39702027f8a94/5ce9a6584eb84f10afbbf2b133534932.html
LINEAGE_QUERY = """
SELECT 
    BASE_OBJECT_NAME as source_table, 
    BASE_SCHEMA_NAME as source_schema,
    DEPENDENT_OBJECT_NAME as dependent_view, 
    DEPENDENT_SCHEMA_NAME as dependent_schema
  from SYS.OBJECT_DEPENDENCIES 
WHERE 
  DEPENDENT_OBJECT_TYPE = 'TABLE'
  OR DEPENDENT_OBJECT_TYPE = 'VIEW'
  AND BASE_SCHEMA_NAME NOT LIKE '%SYS%'
  """


class ViewLineageEntry(BaseModel):
    """Describes the upstream and downstream entities that will be assigned to the columns resulting from the LINEAGE_QUERY

    Attributes:
    source_table (str):  base_object_name in sap hana object dependencies
    source_schema (str): base_schema_name in sap hana
    dependent_view (str): dependent_object_name in sap hana
    dependent_schema (str): dependent_schema_name in sap hana

    """

    source_table: str
    source_schema: str
    dependent_view: str
    dependent_schema: str


class HanaConfig(BasicSQLAlchemyConfig):
    """Represents the attributes needed to configure the SAP HANA DB connection"""

    scheme = "hana"
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["*SYS*"]))
    include_view_lineage: bool = Field(
        default=False, description="Include table lineage for views"
    )

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        regular: str = f"{schema}.{table}"
        return regular


@platform_name("SAPHana")
@config_class(HanaConfig)  # type: ignore
class HanaSource(SQLAlchemySource):
    """Creates a datasource for the lineage of tables from a SAP HANA database.
    It contains the connection to the db using sqlachemy to get the table metadata to construct the downstream-upstream lineage.
    Returns an iterable work unit that gets emitted to Datahub."""

    config: HanaConfig

    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "hana")

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_workunits()

        if self.config.include_view_lineage:
            yield from self._get_view_lineage_workunits()

    def _get_view_lineage_elements(self) -> Dict[Tuple[str, str], List[str]]:
        """Connects to SAP HANA db to run the query statement. The results are then mapped to the ViewLineageEntry attributes.
        Returns a dictionary of downstream and upstream objects from the query results.
        """
        url = self.config.get_sql_alchemy_url()

        engine = create_engine(url, **self.config.options)

        data: List[ViewLineageEntry] = []

        with engine.connect() as conn:
            query_results = conn.execute(LINEAGE_QUERY)

            if not query_results.returns_rows:
                logger.debug("No rows returned.")
                return {}

            for row in query_results:
                data.append(ViewLineageEntry.parse_obj(row))

        lineage_elements: Dict[Tuple[str, str], List[str]] = defaultdict(list)

        for lineage in data:
            if not self.config.view_pattern.allowed(lineage.dependent_view):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            if not self.config.schema_pattern.allowed(lineage.dependent_schema):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            key = (lineage.dependent_view, lineage.dependent_schema)

            lineage_elements[key].append(
                mce_builder.make_dataset_urn(
                    self.platform,
                    self.config.get_identifier(
                        lineage.source_schema, lineage.source_table
                    ),
                    self.config.env,
                )
            )

        return lineage_elements

    def _get_view_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Creates MetadataWorkUnit objects for table lineage based on the downstream and downstream objects from the query results.
        Returns an iterable MetadataWorkUnit that are emitted to Datahub.
        """

        lineage_elements = self._get_view_lineage_elements()

        if not lineage_elements:
            logger.debug("No lineage elements returned.")
            return None

        for key, source_tables in lineage_elements.items():
            dependent_view, dependent_schema = key

            urn = mce_builder.make_dataset_urn(
                self.platform,
                self.config.get_identifier(
                    dependent_schema,
                    dependent_view,
                ),
                self.config.env,
            )

            lineage_mce = mce_builder.make_lineage_mce(source_tables, urn)
            for item in mcps_from_mce(lineage_mce):
                wu = item.as_workunit()
                self.report.report_workunit(wu)
                yield wu
