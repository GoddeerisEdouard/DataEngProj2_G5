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

CONN_STRING = f'DRIVER={config.DRIVER};SERVER=tcp:{config.SERVER};PORT=1433;DATABASE={config.DATABASE};UID={config.USERNAME};PWD={config.PASSWORD}'
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
        functions.logging(cursor, "Database has been initialized")


def fill_database() -> None:
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        try:
            sql_table_manipulation("Cases", cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json', ["DATE", "PROVINCE", "REGION", "AGEGROUP", "SEX", "CASES"])
            sql_table_manipulation("Mort", cursor, 'https://epistat.sciensano.be/Data/COVID19BE_MORT.json', ["DATE", "REGION", "AGEGROUP", "SEX", "DEATHS"])
            sql_table_manipulation("Muni", cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json', ["NIS5", "DATE", "MUNI", "PROVINCE", "REGION", "CASES"])
            sql_table_manipulation("Vaccins", cursor, 'https://epistat.sciensano.be/Data/COVID19BE_VACC.json', ["DATE", "REGION", "AGEGROUP", "SEX", "BRAND", "DOSE", "COUNT"])
            functions.logging(cursor, "Database filled")
        except pyodbc.DatabaseError as err:
            functions.logging(cursor, f"Database Error {err.args[1]}")

def sql_table_manipulation(table, cursor: pyodbc.Cursor, data_url, variable_list) -> None:

    new_data, delete_data = functions.get_data(data_url)
    
    if new_data is None:
        functions.logging(cursor, f"There was an error in retrieving {table} data")
        return
    
    def effect(data, functie):
        rows_affected = 0
        for iteration, row in enumerate(data):
            list_ = []
            for item in variable_list:
                list_.append(functions.variable_switch(item, row[item]) if item in row else None)
            cursor.execute(functie(table, variable_list), list_)
            rows_affected = iteration
        return rows_affected

    rows_affected = effect(delete_data, lambda table, variable_list: functions.sql_delete_where(table, variable_list))
    rows_affected = effect(new_data, lambda table, variable_list: functions.sql_insert_into(table, variable_list))
    
    # for row in delete_data:
    #     list_ = []
    #     for item in variable_list:
    #         list_.append(functions.variable_switch(item, row[item]) if item in row else None)
    #     cursor.execute(functions.sql_delete_where(table, variable_list), list_)
          
    # for iteration, row in enumerate(new_data):
    #     list_ = []
    #     for item in variable_list:
    #         list_.append(functions.variable_switch(item, row[item]) if item in row else None)
    #     cursor.execute(functions.sql_insert_into(table, variable_list), list_)
    #     rows_affected = iteration
    
    functions.logging(cursor, f"Table {table} filled", ra=rows_affected)


        
#init_db()
#fill_database()
schedule.every().day.at("01:00").do(fill_database)

while True:
    schedule.run_pending()
    time.sleep(60)
