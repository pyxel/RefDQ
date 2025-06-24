"""
RefDQ MVP version 0.01
© 2025 Mark Sabin <morboagrees@gmail.com>
Released under Apache 2.0 license. Please see https://github.com/pyxel/differ/blob/main/LICENSE.
"""

import pandas as pd
import os
import yaml
import string
import get_snowflake as sf
from snowflake.snowpark import Row

NULL_PLACEHOLDER = "_£^NULL^£_"


def readfile(file: str):
    f = open(file)
    content = f.read()
    f.close()
    return content


def load_yaml(s: str):
    """Returns a dict from a yaml string."""
    return yaml.safe_load(s)


def load_yaml_config(path = "./refman/config.yaml"):
    """Returns a dict of configurations defined in the configuration file."""
    if os.path.exists(path):
        return load_yaml(readfile(path))
    else:
        return {}


d_config = load_yaml_config()
config_path = d_config.get("config_path", '/home/mark/Documents/dev/nevada/refman')
temp_schema = d_config.get("temp_schema")


def get_targets(path = os.path.join(config_path, 'tables')):
    """Returns a dict of all defined targets."""
    ext = '.yaml'
    files = [os.path.splitext(file)[0] for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and os.path.splitext(file)[1] == ext]
    targets = {}
    for file in files:
        targets[file] = yaml.safe_load(open(os.path.join(path, file + ext)))
    return targets


def get_target_group_names():
    """Returns a list of all defined target table names."""
    targets = get_targets()
    return [targets[k].get('group') for k in targets.keys()]


def get_target_table_names(group = None):
    """Returns a list of all defined target table names."""
    targets = get_targets()
    return [targets[k]['target_table'] for k in targets.keys() if group is None or targets[k].get('group') == group]


def get_check_definitions(path = os.path.join(config_path, 'checks')):
    """Returns a dict of all defined checks."""
    ext = '.yaml'
    files = [os.path.splitext(file)[0] for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and os.path.splitext(file)[1] == ext]
    checks = {}
    for file in files:
        checks[file] = yaml.safe_load(open(os.path.join(path, file + ext)))
        for key in ['type', 'sql', 'description']:
            assert key in checks[file], f"Check definition {file} must contain key {key}."
    return checks


def get_target_sample(target_table, num_rows = 100):
    return sf.query(f"select * from {target_table} limit {num_rows}")

class Target:
    """Defines a target table."""
    
    def __init__(self, target_name: str = None, target_table_name: str = None):

        _targets = get_targets(path = os.path.join(config_path, 'tables'))

        if target_name is None and target_table_name is None:
            raise ValueError("Either target_name or target_table_name must be passed in.")
        elif target_name is None:
            target_name = [k for k in _targets.keys() if _targets[k]['target_table'] == target_table_name][0]

        _target = _targets[target_name]

        self.validate_target_dict(target_name, _target)

        self.target_table = _target['target_table']
        self.primary_key = _target['primary_key']
        self.checks = _target.get('checks', None)


    def validate_target_dict(self, target_name, d_target):
        assert 'target_table' in d_target, ValueError(f"Target table definition yaml for {target_name} must contain key target_table")
        assert 'primary_key' in d_target, ValueError(f"Target table definition yaml for {target_name} must contain key primary_key")


    def __str__(self):
        return self.target_table


class Check:
    """Defines a data check."""
    def __init__(self, definition, general_args, check_args):

        self.definition = definition

        if 'sql' not in definition:
            raise ValueError("Check definition must contain key 'sql'.")

        if 'type' not in definition:
            raise ValueError("Check definition must contain key 'type'.")
        
        self.type = definition['type']
        
        self.description = definition['description'] if 'description' in definition else ''

        # Get the variable placeholders from the SQL string.
        self.variables = [v[1] for v in string.Formatter().parse(definition['sql']) if v[1] is not None]

        args = {**general_args, **check_args}

        for var in self.variables:
            if var not in list(args.keys()):
                raise ValueError(f"Argument '{var}' is required for check '{self.type}'.")

        self.sql = definition['sql'].format(**args)
        

class CheckResult:
    """Defines the result of a data check."""
    def __init__(self, check_type, description, df, check_passed, error):

        self.check_type = check_type
        self.description = description
        self.df = df
        self.check_passed = check_passed
        self.error = None


class RefData:

    def __init__(self, target: Target, df: pd.DataFrame, upload_type: str = 'merge', ignore_schema_error = False):

        assert isinstance(target, Target), ValueError("target must be of type Target.")
        # Target object.
        self.target = target
        # The Snowflake schema for the target table.
        self.targetschema = self.getschema(self.target)
        # The dataframe received from the user.
        # TODO: Find a better way to handle null/nan/none.
        self.df = df.fillna(NULL_PLACEHOLDER).astype(str).replace(NULL_PLACEHOLDER, None)

        # upload type
        self.upload_type = upload_type.lower()
        if self.upload_type not in ['merge', 'replace']:
            raise ValueError("upload_type must be 'merge' or 'replace'.")

        # Upload df to temp table.
        self.stage_table = f"{temp_schema}.{self.target.target_table.split('.')[2]}"
        sf.write_table(pd_df = self.df, table = self.stage_table, mode = "overwrite", table_type = "transient")
        
        # Compare schemas.
        self.sourceschema = self.getschema(self.stage_table)
        self.diffschema = self.comparecolumns()

        if not (self.diffschema == [] or ignore_schema_error): return
        
        # Check data types
        self.diffdatatypes = self.check_data_types()

        if self.diffdatatypes != []: return
        
        # Impact
        self.impact = self.assessimpact()

        # Run checks.
        self.all_checks_passed, self.check_results = self.run_checks()


    def upload_data(self):
        cols = self.targetschema.keys()
        update_cols = ", ".join([f"tgt.{col} = src.{col}" for col in cols])
        insert_cols = ", ".join([col for col in cols])
        value_cols = ", ".join([f"src.{col}" for col in cols])
        if self.upload_type == 'merge':
            sql = f"""
merge into {self.target.target_table} tgt
using {self.stage_table} src
on tgt.{self.target.primary_key} = src.{self.target.primary_key}
when matched then
    update set {update_cols}
when not matched then
    insert ({insert_cols}) values ({value_cols})
"""
        else:
            sql = f"""
insert overwrite into {self.target.target_table} ({insert_cols})
select {insert_cols} from {self.stage_table}
"""
            
        sf.query(sql)
        

    def getcastcols(self):
        """Returns a string of comma-separated column names wrapped in try-cast functions."""
        return ", ".join([f"{'' if dtype.startswith('VARCHAR') else 'try_'}cast({col} as {dtype}) as {col}" for col, dtype in self.targetschema.items()])
    
    
    def getschema(self, tablename):
        """Retrieves and returns the schema of the Snowflake table as a dict { field_name: data type }, for easy comparison."""
        return sf.get_table_schema(tablename)
    
    
    def compareschemas(self):
        """Returns list of tuples: [('Column name', 'Table Data type', 'Uploaded Data type', 'Comparison')]"""
        diff = []
        NOT_FOUND = "(not found)"
        DIFF_DATA_TYPE = "(different data type)"
        for col in self.targetschema.keys():
            if self.sourceschema.get(col, NOT_FOUND) != self.targetschema[col]:
                diff.append((
                    col,
                    self.targetschema[col],
                    self.sourceschema.get(col, NOT_FOUND),
                    NOT_FOUND if self.sourceschema.get(col, NOT_FOUND) == NOT_FOUND else DIFF_DATA_TYPE,
                ))
        return diff
    

    def comparecolumns(self):
        """Returns list of columns that were not found in the uploaded data."""
        diff = []
        for col in self.targetschema.keys():
            if col not in self.sourceschema:
                diff.append(col)
        return diff
    

    def assessimpact(self):
        """Returns the number of rows inserted, updated and deleted."""
        cols = self.targetschema.keys()
        select_cols = ", ".join([col for col in cols])

        if self.upload_type == 'merge':
            sql = """
            with t2 as (
                select {select_cols_tmp} from {t2}
                except
                select {select_cols} from {t1}
            )

            select
                count_if(t1.{key} is null) as inserted,
                count_if(t1.{key} is not null and t2.{key} is not null) as updated,
                count_if(t1.{key} is not null) as table_rows,
                count_if(t2.{key} is not null) as upload_rows
            from {t1} t1
            full join t2 on t2.{key} = t1.{key}
            """.format(
                key = self.target.primary_key,
                t1 = self.target.target_table,
                t2 = self.stage_table,
                select_cols_tmp = self.getcastcols(),
                select_cols = select_cols
            )
        else:
            sql = """
            select
                (select count(*) from {t1} t1) as table_rows,
                (select count(*) from {t2} t2) as upload_rows
            """.format(
                t1 = self.target.target_table,
                t2 = self.stage_table
            )

        df = sf.query(sql)
        for row in df:
            row = row.as_dict()
            #print(row)
            impact = {
                "inserted": row.get("INSERTED"),
                "updated": row.get("UPDATED"),
                "table_rows": row.get("TABLE_ROWS"),
                "upload_rows": row.get("UPLOAD_ROWS"),
                "merge_rows_affected": row.get("INSERTED", 0) + row.get("UPDATED", 0)
            }
            #print(impact)
        return impact
    

    def check_data_types(self):

        # Loop through each column and create a select statement to find values with uncastable data types.
        # Union the select statements.
        cast_select = [f"""select
    {self.target.primary_key} as primary_key, 
    '{name}' as column_name,
    {name} as value,
    '{dtype}' as expected_data_type
from {self.stage_table}
where {name} is not null and {'' if dtype.startswith('VARCHAR') else 'try_'}cast({name} as {dtype}) is null""" for name, dtype in self.targetschema.items()]
        sql = ' union all\n'.join(cast_select) + '\norder by 1,2'
        #print(sql)

        df = sf.query(sql)

        return df    


    def run_checks(self):
        # Parse the check definition files.
        check_definitions = get_check_definitions()
        # List of dataframes to return.
        check_results = []
        all_passed = True

        cols = self.targetschema.keys()
        
        select_cols_tmp = self.getcastcols()
        select_cols = ", ".join([col for col in cols])

        # Loop through checks defined for this table.
        for check in self.target.checks:

            # Get the check type.
            check_type = check["type"]

            # Get the SQL of the check.
            check_sql = check_definitions[check_type]['sql']

            # Get the placeholder variables from the SQL string.
            vars = [v[1] for v in string.Formatter().parse(check_sql) if v[1] is not None]

            # Define the SQL query.
            if self.upload_type == "merge":
            # Create a table expression which is the union of existing and new data.
                table_sql = f"""(
select {select_cols_tmp}
from ({self.stage_table}) src
union all
select {select_cols}
from {self.target.target_table} tgt
where not exists (
    select 1
    from {self.stage_table} src
    where src.{self.target.primary_key} = tgt.{self.target.primary_key}
)
) as __union__t
"""
            else:
                table_sql = self.stage_table

            
            # Pass in all general variable values. They may not all be used, but format() accepts unused values so we don't need to check which are required.
            # General variables:
            d_vars = {
                "table": table_sql,
                "primary_key": self.target.primary_key
            }
            # Variables defined in the check definition SQL.
            # We expect to find values for these in the table check yaml.
            for var in [var for var in vars if var not in list(d_vars.keys())]:
                d_vars[var] = check[var]

            # Get the description from the check definition and the table definition.
            description = check_definitions[check_type]['description'].format(**d_vars) + \
                ('\n' + check.get("description")) if check.get("description") is not None else ''

            sql = check_sql.format(**d_vars)
            
            # Run the check
            error = None
            try:
                df = sf.query(sql)
            except Exception as e:
                error = e
                raise

            check_result = CheckResult(
                check_type = check_type,
                description = description,
                df = df,
                check_passed = True,
                error = error
            )
            if len(df) > 0:
                all_passed = False
                check_result.check_passed = False
            check_results.append(check_result)


        # Return a bool indicating pass/fail of all checks and a list of checks, including data sets containing failed rows.
        return all_passed, check_results




if __name__ == '__main__':
    from snowflake.snowpark import Session
    #import streamlit as st
    #session = st.connection('snowflake').session()

    #session = Session.builder.config('local_testing', True).create()
    #df = pd.DataFrame([(6, 'kds', 22,)], columns = ['ID', 'NAME', 'AGE'])
    #print(df)
    #rd = RefData(target = Target(target_table_name = 'datafold.datafold.names'), df = df)

    #t = Target(target_name = 'names')
    #rd.run_checks()

    #print(rd)

    #c = Check({"sql": "select 1 from {target_table} where {key_cols} = 1", "type": "unique"}, {"target_table": "names"}, {"key_cols": "c1"})
    #print(c.variables, c.sql, c.type, c.description)