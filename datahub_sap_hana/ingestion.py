import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Set, Tuple, Union

import datahub.emitter.mce_builder as builder
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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from pydantic import BaseModel
from pydantic.fields import Field as PydanticField
from sqlalchemy import create_engine, inspect
from sqlglot import parse_one
from sqlglot.expressions import DerivedTable
from sqlglot.lineage import Node, lineage

from schema import Entity, Field, UpstreamField

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
    schema_pattern: AllowDenyPattern = PydanticField(
        default=AllowDenyPattern(deny=["*SYS*"])
    )
    include_view_lineage: bool = PydanticField(
        default=False, description="Include table lineage for views"
    )
    include_column_lineage: bool = PydanticField(
        default=False, description="Include column lineage for views"
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

    def get_db_connection(self):
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url)
        conn = engine.connect()
        return conn

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_workunits()

        if self.config.include_view_lineage:
            yield from self._get_view_lineage_workunits()

        if self.config.include_column_lineage:
            yield from self._get_column_lineage_workunits()

    def _get_view_lineage_elements(self, conn) -> Dict[Tuple[str, str], List[str]]:
        """Connects to SAP HANA db to run the query statement. The results are then mapped to the ViewLineageEntry attributes.
        Returns a dictionary of downstream and upstream objects from the query results.
        """

        data: List[ViewLineageEntry] = []

        with conn:
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

            if not self.config.schema_pattern.allowed(lineage.dependent_view):
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
        conn = self.get_db_connection()
        lineage_elements = self._get_view_lineage_elements(conn)

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

    def get_column_lineage_view_definitions(self, conn) -> Iterable[Tuple[str, str]]:
        with conn:
            inspector = inspect(conn)
            schema: List[Any] = inspector.get_schema_names()  # returns a list

            for schema_name in schema:
                views = inspector.get_view_names(schema=schema_name)  # returns a list

                for view_name in views:
                    view_sql = inspector.get_view_definition(view_name, schema_name)

                    if view_sql:
                        yield (view_name, view_sql)

    def _get_column_lineage_for_view(
        self, view_name: str, view_sql: str
    ) -> Tuple[str, List[Node]]:
        """Extracts the columns and the sql definitions of a downstream view.
        These are then passed to the lineage function create a lineage graph of the column of an sql query.
        """

        expression: DerivedTable = parse_one(view_sql)
        selected_columns: List[str] = expression.named_selects

        view_sql = view_sql.lower()
        lineages = [
            lineage(col_name.lower(), view_sql) for col_name in selected_columns
        ]
        return view_name, lineages

    def parse_column_name(self, column_name: str):
        """Removes the table/name and alias from the column name returned by sqlglot, eg: instead of 'hotel.room', it will return 'room'."""

        parsed_name = column_name.split(".")
        return parsed_name[1]

    def get_column_view_lineage_elements(
        self,
    ) -> Iterable[Tuple[Field, List[UpstreamField]]]:
        """This function returns an iterable of tuples containing information about the lineage of columns in a view.
        Each tuple contains a downstream field (a column in a view) and a list of upstream fields
        (columns in other views or tables that are used to calculate/transform the downstream column).
        """

        conn = self.get_db_connection()

        for view_name, view_sql in self.get_column_lineage_view_definitions(conn):
            view_name, column_lineages = self._get_column_lineage_for_view(
                view_name, view_sql
            )
            for lineage_node in column_lineages:
                downstream_fields = Field(name=lineage_node.name)

                upstream_fields_list: List[UpstreamField] = []

                for column in lineage_node.downstream:
                    upstream_fields = UpstreamField(
                        name=self.parse_column_name(column.name),
                        entity=Entity(name=column.source.name, platform="hana"),
                    )
                    upstream_fields_list.append(upstream_fields)

                if len(upstream_fields_list) > 1:
                    yield (downstream_fields, upstream_fields_list)

    def create_column_lineage(
        self,
    ) -> Iterable[Tuple[List[FineGrainedLineage], Set[str], str]]:
        """Returns an iterable of tuples, where each tuple contains a list of FineGrainedLineage objects, which represents
        column-level lineage information and a set of strings representing the upstream dataset URNs created during lineage generation.
        """

        column_lineages: List[FineGrainedLineage] = []
        seen_upstream_datasets: Set[str] = set()
        upstream_columns: List[Any] = []

        upstream_type = FineGrainedLineageUpstreamType.FIELD_SET
        downstream_type = FineGrainedLineageDownstreamType.FIELD

        for (
            downstream_field,
            upstream_fields,
        ) in self.get_column_view_lineage_elements():
            downstream_dataset_urn = builder.make_dataset_urn(
                platform="hana", name=downstream_field.name, env="PROD"
            )

            for upstream_field in upstream_fields:
                upstream_dataset_urn = builder.make_dataset_urn(
                    platform="hana", name=upstream_field.name, env="PROD"
                )
                seen_upstream_datasets.add(upstream_dataset_urn)
                upstream_columns.append(
                    builder.make_schema_field_urn(
                        upstream_dataset_urn, upstream_field.name
                    )
                )

            column_lineages.append(
                FineGrainedLineage(
                    downstreamType=downstream_type,
                    downstreams=[
                        builder.make_schema_field_urn(
                            downstream_dataset_urn, downstream_field.name
                        )
                    ],
                    upstreamType=upstream_type,
                    upstreams=upstream_columns,
                )
            )

            yield column_lineages, seen_upstream_datasets, downstream_dataset_urn

    def _get_column_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Returns an iterable of MetadataChangeProposalWrapper object that contains column lineage information that is sent to Datahub
        after each iteration of the loop. The object is built with column lineages, upstream datasets, and downstream dataset
        URNs from the create_column_lineage method.
        """

        for (
            column_lineages,
            upstream_datasets,
            downstream_dataset_urn,
        ) in self.create_column_lineage():
            upstream_datasets = list(upstream_datasets)
            UpstreamLineage(
                fineGrainedLineages=column_lineages,
                upstreams=[
                    Upstream(dataset=dataset_urn, type=DatasetLineageType.TRANSFORMED)
                    for dataset_urn in upstream_datasets
                ],
            )

            lineage_mce = mce_builder.make_lineage_mce(
                upstream_datasets, downstream_dataset_urn
            )
            for item in mcps_from_mce(lineage_mce):
                wu = item.as_workunit()
                self.report.report_workunit(wu)
                yield wu


if __name__ == "__main__":
    hana_config = HanaConfig(
        username="HOTEL",
        password="Localdev1",
        host_port="localhost:39041",
        database="HXE",
        schema_pattern=AllowDenyPattern(allow=["*"]),
        include_view_lineage=True,
    )  # type: ignore
    hana_source = HanaSource(hana_config, PipelineContext(run_id="test"))

    lineage_urns = hana_source.create_column_lineage()
    print(lineage_urns)

    conn = hana_source.get_db_connection()

    column_lineage_view_definitions = hana_source.get_column_lineage_view_definitions(
        conn
    )
    for view_name, view_sql in column_lineage_view_definitions:
        lineage_for_column = hana_source.get_column_view_lineage_elements()

    column_view_lineage = hana_source.get_column_view_lineage_elements()
    for col_lineage in column_view_lineage:
        print(col_lineage)

    for lineage_urn, upstreams, downstream_urn in hana_source.create_column_lineage():
        print(lineage_urn, upstreams, downstream_urn)

    lineage_emitted = hana_source._get_column_lineage_workunits()
    for le in lineage_emitted:
        print(le)
