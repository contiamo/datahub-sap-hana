from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.engine.reflection import Inspector

from sqlglot import parse_one, exp, parse
from sqlglot.lineage import lineage, Node
from sqlglot.optimizer.scope import traverse_scope, ScopeType, walk_in_scope, Scope

from datahub_sap_hana.ingestion import LINEAGE_QUERY
import datahub.emitter.mce_builder as builder
from typing import List, Optional, Union, Any

from typing import Any, List, Optional, Set, Tuple

import datahub.emitter.mce_builder as builder
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,)

from schema import Lineage, ColumnLineage, Field, Upstream, UpstreamField, Entity


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
        env="PROD"
    )

    print(dataset_urn)


def fldUrn(dataset_urn: str, column_name: str):
    """Returns the URN for a given dataset and column.
    In the datahub ui, if the URN  is urn:li:dataPlatform:bigquery,dev-and-test-env.dbt_kay.dim_customers,PROD, the dataset_name is dev-and-test-env.dbt_kay.dim_customers.
    The column name could be one of the columns in this table, eg: customer_id
    """

    return builder.make_schema_field_urn(dataset_urn, column_name)


def get_view_definitions(conn):

    # engine = create_engine("hana://HOTEL:Localdev1@localhost:39041/HXE")

    # with engine.connect() as conn:
    inspector = inspect(conn)

    sql_definition_list = []
    schema = inspector.get_schema_names()  # returns a list
    views = inspector.get_view_names(schema[0])  # returns a list
    # how to get only the relevant schema? exclude anything with %sys%?
    print(f"name of views in {schema[0]}: {views}")
    for view in views:
        definition = inspector.get_view_definition(view, schema[0])
        # because sqlglot wants lowercase and saphana setup is in uppercase
        # sql_definition_list.append(definition.lower())
        yield (view, definition)


def get_lineage_for_view(view_name: str, view_sql: str):

    cols = parse_one(view_sql).named_selects
    for col_name in cols:
        yield (view_name, lineage(col_name.lower(), view_sql.lower()))


def parse_column_name(column_name: str):
    parsed_name = column_name.split(".")
    return parsed_name[1]


def get_lineage():

    engine = create_engine("hana://HOTEL:Localdev1@localhost:39041/HXE")

    with engine.connect() as conn:
        view_definitions = get_view_definitions(conn)
        for view_name, view_sql in view_definitions:
            # _ notation to ignore view name for now
            for _, lineage_node in get_lineage_for_view(view_name, view_sql):
                print(
                    f"processing lineage for column:{view_name}.{lineage_node.name}")
                # we are in the node, which represents the lineage of 1 column
                # lineage_node.name is the name of the column that we want to see the lineage of
                # lineage_node.downstream is the datahub upstream
                # each element of lineage_node.downstream is a Node that represents a column in the source table
                # column.source.name refers to the source table and column.name is the name of the column
                for column in lineage_node.downstream:
                    yield column.source.name, parse_column_name(column.name)


"""


    for node in lineage_list:
        # downstream field is basically the col whose lineage we want to trace. In Sqlglot, this refers to the node name
        downstream_fields = [Field(name=node.name)]

        print(node)

        upstream_columns = []

        upstream_fields = node.downstream

        upstream_type = FineGrainedLineageUpstreamType.FIELD_SET

        for fields in upstream_fields:
            fields = UpstreamField(name=fields.name, entity=Entity(
                name=node.name, platform="hana, env"))

            upstream_dataset_urn = builder.make_dataset_urn_with_platform_instance(
                platform="hana",
                name="HXE",
                platform_instance="hana://HOTEL:Localdev1@localhost:39041/HXE",
                env="PROD")

            # print(fields)

            seen_upstream_datasets.add(upstream_dataset_urn)
            upstream_columns.append(fldUrn(
                upstream_dataset_urn, fields.name))

            # print(seen_upstream_datasets)
            # print(upstream_columns)
"""
