from typing import Any, List, Set

import datahub.emitter.mce_builder as builder
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from sqlalchemy import create_engine, inspect
from sqlglot import parse_one
from sqlglot.lineage import lineage

from schema import Entity, UpstreamField

hotel = """SELECT * FROM HOTEL.AVAILABLE_ROOMS_BY_HOTEL """
views_check = """SELECT * FROM VIEWS WHERE SCHEMA_NAME = 'HOTEL'"""


def check_views(query):
    engine = create_engine("hana://HOTEL:Localdev1@localhost:39041/HXE")
    conn = engine.connect()
    print(conn.execute(query).fetchall())


def create_hana_urn():
    dataset_urn = builder.make_dataset_urn_with_platform_instance(
        platform="hana",
        name="HXE",
        platform_instance="hana://HOTEL:Localdev1@localhost:39041/HXE",
        env="PROD",
    )

    print(dataset_urn)


def fldUrn(dataset_urn: str, column_name: str):
    """Returns the URN for a given dataset and column.
    In the datahub ui, if the URN  is urn:li:dataPlatform:bigquery,dev-and-test-env.dbt_kay.dim_customers,PROD, the dataset_name is dev-and-test-env.dbt_kay.dim_customers.
    The column name could be one of the columns in this table, eg: customer_id
    """

    return builder.make_schema_field_urn(dataset_urn, column_name)


def get_view_definitions():
    engine = create_engine("hana://HOTEL:Localdev1@localhost:39041/HXE")

    with engine.connect() as conn:
        inspector = inspect(conn)

        schema: List[Any] = inspector.get_schema_names()  # returns a list

        for schema_name in schema:
            views = inspector.get_view_names(schema=schema_name)  # returns a list

            for view_name in views:
                definition = inspector.get_view_definition(view_name, schema_name)

                yield (view_name, definition)


def get_lineage_for_view(view_name: str, view_sql: str):
    cols = parse_one(view_sql).named_selects
    for col_name in cols:
        yield (view_name, lineage(col_name.lower(), view_sql.lower()))


def parse_column_name(column_name: str):
    parsed_name = column_name.split(".")
    return parsed_name[1]


def create_column_lineage():
    engine = create_engine("hana://HOTEL:Localdev1@localhost:39041/HXE")

    with engine.connect() as conn:
        view_definitions = get_view_definitions(conn)
        for view_name, view_sql in view_definitions:
            # _ notation to ignore view name for now
            for _, lineage_node in get_lineage_for_view(view_name, view_sql):
                # print(
                #    f"processing lineage for column:{view_name}.{lineage_node.name}")
                # we are in the node, which represents the lineage of 1 column
                # lineage_node.name is the name of the column that we want to see the lineage of
                # lineage_node.downstream is the datahub upstream
                # each element of lineage_node.downstream is a Node that represents a column in the source table
                # column.source.name refers to the source table and column.name is the name of the column

                downstream_fields = lineage_node.name

                upstream_fields_list: List[UpstreamField] = []

                for column in lineage_node.downstream:
                    upstream_fields = UpstreamField(
                        name=parse_column_name(column.name),
                        entity=Entity(name=column.source.name, platform="hana"),
                    )
                    upstream_fields_list.append(upstream_fields)

                if len(upstream_fields_list) > 1:
                    yield (downstream_fields, upstream_fields_list)

    def create_fine_grained_lineage():
        column_lineages: List[FineGrainedLineage] = []
        seen_upstream_datasets: Set[str] = set()
        upstream_columns: List[Any] = []

        upstream_type = FineGrainedLineageUpstreamType.FIELD_SET
        downstream_type = FineGrainedLineageDownstreamType.FIELD

        for downstream_field, upstream_fields in create_column_lineage():
            downstream_dataset_urn = builder.make_dataset_urn(
                platform="hana", name=downstream_field, env="PROD"
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
                            downstream_dataset_urn, downstream_field
                        )
                    ],
                    upstreamType=upstream_type,
                    upstreams=upstream_columns,
                )
            )

        yield column_lineages, seen_upstream_datasets

    def create_workunits(self):
        lineage_data = []

        for column_lineage, seen_upstream_datasets in create_fine_grained_lineage():
            for upstream_urn in seen_upstream_datasets:
                lineage_data.append(
                    {"dataset": upstream_urn, "lineage": column_lineage}
                )

        print(lineage_data)
