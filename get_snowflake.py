import os
from snowflake.snowpark import Session

# Streamlit in Snowflake
# Try to get the Snowflake session
try:
    import streamlit as st
    session = st.connection('snowflake').session()
# Or for local execution, use env vars.
except:
    connection_parameters = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "database": os.environ.get("SNOWFLAKE_DATABASE"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA")
    }

    session = Session.builder.configs(connection_parameters).create()

def databases(filters: str = None):
    return [{'database': 'db1'}, {'database': 'db2'}, {'database': 'db3'}]


def schemas(filters: str = None):
    return [{'schema': 'db1.schema1'}, {'schema': 'db1.schema2'}, {'schema': 'db1.schema3'}]


def tables(filters: str = None):
    return [{'table': 'db1.schema1.table1'}, {'table': 'db1.schema1.table2'}, {'table': 'db1.schema1.table3'},
            {'table': 'db1.schema1.pelican'}, {'table': 'db1.schema1.dog'}, {'table': 'db1.schema1.zebra'},
            ]


def query(sql):
    print(sql)
    return session.sql(sql).collect()


def get_table_schema(tablename):
    df = session.sql(f"describe table {tablename}").collect()
    schema = {}
    for row in df:
        schema[row.as_dict()["name"]] = row.as_dict()["type"]
    return schema


def write_table(pd_df, table, mode = "overwrite", table_type = "transient", enable_schema_evolution = True):
    df = session.create_dataframe(pd_df)
    df.write.save_as_table(table, mode = mode, table_type = table_type, enable_schema_evolution = enable_schema_evolution)
