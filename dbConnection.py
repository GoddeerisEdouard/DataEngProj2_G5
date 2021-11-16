import pyodbc
import config
import constants
import functions
import sql_statements
import schedule
import time
from datetime import datetime
from contextlib import contextmanager
import sys
import os

#CONN_STRING = f'DRIVER={config.DRIVER};SERVER=tcp:{config.SERVER};PORT=1433;DATABASE={config.DATABASE};UID={config.USERNAME};PWD={config.PASSWORD}'
CONN_STRING = f'DRIVER={config.DRIVER};SERVER={config.SERVER};DATABASE={config.DATABASE};Trusted_Connection=yes;'
DATE_FORMAT = "%Y-%m-%d"

@contextmanager
def open_db_connection(connection_string, commit=False):
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
        cursor.execute(sql_statements.sql_create_population_table)
        functions.logging(cursor, "Database has been initialized")


def fill_database() -> None:
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        try:
            sql_table_manipulation("Cases", cursor, ["DATE", "PROVINCE", "REGION", "AGEGROUP", "SEX", "CASES"], 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json')
            sql_table_manipulation("Mort", cursor, ["DATE", "REGION", "AGEGROUP", "SEX", "DEATHS"], 'https://epistat.sciensano.be/Data/COVID19BE_MORT.json')
            sql_table_manipulation("Muni", cursor, ["NIS5", "DATE", "TX_DESCR_NL", "PROVINCE", "REGION", "CASES"], 'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json')
            sql_table_manipulation("Vaccins", cursor, ["DATE", "REGION", "AGEGROUP", "SEX", "BRAND", "DOSE", "COUNT"], 'https://epistat.sciensano.be/Data/COVID19BE_VACC.json')
            if not os.path.isfile("TF_SOC_POP_STRUCT_2021.xlsx"):
                sql_table_manipulation("Population", cursor, ["REFNIS", "MUNI", "PROVINCE", "REGION", "SEX", "NATIONALITY", "AGE", "POPULATION", "YEAR"], 'https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2021.xlsx')
            functions.logging(cursor, "Database filled")
        except pyodbc.DatabaseError as err:
            functions.logging(cursor, f"Database Error {err.args[1]}")

def sql_table_manipulation(table, cursor: pyodbc.Cursor, variable_list, url) -> None:
    insert_data, delete_data = functions.get_data(url)
    
    if insert_data is None:
        functions.logging(cursor, f"There was an error in retrieving {table} data")
        return
    
    def execute_query(data, functie):
        rows_affected = 0
        for iteration, row in enumerate(data):
            list_ = []
            for column_name in variable_list:
                list_.append(functions.variable_switch(column_name, row[column_name]) if column_name in row else None)
            cursor.execute(functie(table, variable_list), list_)
            rows_affected = iteration+1
        return rows_affected
    
    rows_affected = execute_query(delete_data, (lambda table_name, query_variables: functions.sql_delete_where(table_name, query_variables)))
    rows_affected = execute_query(insert_data, (lambda table_name, query_variables: functions.sql_insert_into(table_name, query_variables)))
    functions.logging(cursor, f"Table {table} filled", ra=rows_affected)

init_db()
fill_database()
schedule.every().day.at("01:00").do(fill_database)

while True:
    schedule.run_pending()
    time.sleep(60)
