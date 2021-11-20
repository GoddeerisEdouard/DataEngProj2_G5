from enum import Enum, auto
from typing import  List
import pyodbc
import config
import functions
import sql_statements
import schedule
import time
from contextlib import contextmanager
import sys
import os
import json
import requests
import constants

CONN_STRING = f'DRIVER={config.DRIVER};SERVER=tcp:{config.SERVER};PORT=1433;DATABASE={config.DATABASE};UID={config.USERNAME};PWD={config.PASSWORD}'
#CONN_STRING = f'DRIVER={config.DRIVER};SERVER={config.SERVER};DATABASE={config.DATABASE};Trusted_Connection=yes;'
DATE_FORMAT = "%Y-%m-%d"

@contextmanager
def open_db_connection(connection_string: str, commit=False):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    try:
        yield cursor
    except pyodbc.DatabaseError as err:
        error, = err.args
        sys.stderr.write(error.message)
        cursor.execute("ROLLBACK")
        functions.logging(cursor, f"Database-connection Error {err.args[1]}")
        raise err
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        connection.close()


def init_db() -> None:
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        cursor.execute(sql_statements.sql_create_cases_table)
        cursor.execute(sql_statements.sql_create_mort_table)
        cursor.execute(sql_statements.sql_create_muni_table)
        cursor.execute(sql_statements.sql_create_vaccins_table)
        cursor.execute(sql_statements.sql_create_logging_table)
        cursor.execute(sql_statements.sql_create_education_muni_table)
        cursor.execute(sql_statements.sql_create_education_age_origin_table)
        cursor.execute(sql_statements.sql_create_income_table)
        functions.logging(cursor, "Database has been initialized")


def fill_database() -> None:
    with open("datasetsconf.json", 'r') as f:
        datasetsdict = json.load(f)
    datasets_data = datasetsdict['datasets']
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        try:
            for data in datasets_data:
                url = data['url']
                extension = url.split('.')[-1]
                if extension == "json":
                    sql_table_manipulation(data['table_name'], cursor, data['column_names'], url)
                elif extension == "xlsx":
                    if not os.path.exists(f"{functions.DATASET_DIR}/{url.split('/')[-1]}"):
                        sql_table_manipulation(data['table_name'], cursor, data['column_names'], url, sheet_name=data['sheet_name'])
                else:
                    print(f"Extension not recognized {extension} from table {data['table_name']}")
            functions.logging(cursor, "Database filled")
        except pyodbc.DatabaseError as err:
            print(err)
            functions.logging(cursor, f"Database Error {err.args[1]}")

def sql_table_manipulation(table: str, cursor: pyodbc.Cursor, variable_list: List[str], url: str, **kwargs) -> None:
    filename = url.split('/')[-1]
    filepath = f"{functions.DATASET_DIR}/{filename}"
    if os.path.exists(filepath):
        with open(filepath) as f:
            old_data_list_dict = json.load(f)
    else:
        old_data_list_dict = []
    try:
        sheet_name = kwargs.get("sheet_name", None)
        new_data_list_dict = functions.get_and_write_data_to_file(url) if sheet_name is None else functions.get_and_write_data_to_file(url, sheet_name=sheet_name)
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
        functions.logging(cursor, f"There was an error in retrieving {table} data: {err.args[1]}")
        return

    insert_delete_update_dict = functions.get_dict_diff(old_data_list_dict, new_data_list_dict)
    
    if insert_delete_update_dict is None:
        functions.logging(cursor, f"Table {table} filled", ra=0)
        return
    
    delete_data = insert_delete_update_dict['delete_data']
    update_data = insert_delete_update_dict['update_data']
    insert_data = insert_delete_update_dict['insert_data']

    rows_affected = 0
    def execute_query(sql_statement_type: SqlStatementType) -> None:
        function = SqlStatementType.get_function(sql_statement_type)
        
        if sql_statement_type is SqlStatementType.INSERT:
            for row in insert_data:
                column_values = [row.get(constants.db_column_name_to_request_key.get(table, {}).get(column_name, column_name), None) for column_name in variable_list]
                cursor.execute(function(table, variable_list), column_values)
        elif sql_statement_type is SqlStatementType.DELETE:
            for id in delete_data:
                cursor.execute(function(table), id)
        elif sql_statement_type is SqlStatementType.UPDATE:
            for row in update_data:
                raw_column_names = list(row['column_changes'].keys())
                valid_column_names = [constants.db_column_name_to_request_key.get(table, {}).get(column_name, column_name) for column_name in raw_column_names]
                column_changes = list(row['column_changes'].values())
                column_changes.append(row['id'])
                cursor.execute(function(table, valid_column_names), column_changes)
    
    if delete_data:
        execute_query(SqlStatementType.DELETE)
        rows_affected += len(delete_data)
    if update_data:
        execute_query(SqlStatementType.UPDATE)
        rows_affected += len(update_data)
    if insert_data:
        execute_query(SqlStatementType.INSERT)
        rows_affected += len(insert_data)

    functions.logging(cursor, f"Table {table} filled", ra=rows_affected)

class SqlStatementType(Enum):
    UPDATE=auto(),
    INSERT=auto(),
    DELETE=auto()
    @classmethod
    def get_function(self, p):
        if self.UPDATE is p:
            return functions.sql_update_where
        elif self.INSERT is p:
            return functions.sql_insert_into
        elif self.DELETE is p:
            return functions.sql_delete_where

init_db()
fill_database()
schedule.every().day.at("01:00").do(fill_database)

while True:
    schedule.run_pending()
    time.sleep(60)
