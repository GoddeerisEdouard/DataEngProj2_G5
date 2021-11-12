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
            data_cases_agesex(cursor)
            data_mort(cursor)
            data_municipality(cursor)
            data_vaccins(cursor)
            functions.logging(cursor, "Database filled")
        except pyodbc.DatabaseError as err:
            functions.logging(cursor, f"Database Error {err.args[1]}")


def data_cases_agesex(cursor: pyodbc.Cursor) -> None:
    new_data, delete_data = functions.get_data('https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json')
    if new_data is None:
        functions.logging(cursor, "There was an error in retrieving cases data")
        return

    rows_affected = 0
    for row in delete_data:
        cursor.execute("delete from Cases where DATE=? and PROVINCE=? and REGION=? and AGEGROUP=? and SEX=? and CASES=?", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('CASES', None))
    
    for iteration, row in enumerate(new_data):
        cursor.execute("insert into Cases(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('CASES', None))
        rows_affected = iteration
    functions.logging(cursor, "Table Cases filled", ra=rows_affected)


def data_mort(cursor: pyodbc.Cursor) -> None:
    new_data, delete_data = functions.get_data('https://epistat.sciensano.be/Data/COVID19BE_MORT.json')
    if new_data is None:
        functions.logging(cursor, "There was an error in retrieving mort data")
        return
    
    rows_affected = 0
    for row in delete_data:
        cursor.execute("delete from Mort where DATE=? and REGION=? and AGEGROUP=? and SEX=? and DEATHS=?", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('DEATHS', None))

    for iteration, row in enumerate(new_data):
        cursor.execute("insert into Mort(DATE, REGION, AGEGROUP, SEX, DEATHS) values (?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('DEATHS', None))
        rows_affected = iteration
    functions.logging(cursor, "Table Mort filled", ra=rows_affected)


def data_municipality(cursor: pyodbc.Cursor) -> None:
    new_data, delete_data = functions.get_data('https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json')
    if new_data is None:
        functions.logging(cursor, "There was an error in retrieving municipality data")
        return

    rows_affected = 0
    for row in delete_data:
        cursor.execute("delete from Muni where NIS5=? and DATE=? and MUNI=? and PROVINCE=? and REGION=? and CASES=?", row.get('NIS5', None), datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        row.get('TX_DESCR_NL', None), constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row['CASES'].replace('<', '') if 'CASES' in row else None)

    for iteration, row in enumerate(new_data):
        cursor.execute("insert into Muni(NIS5, DATE, MUNI, PROVINCE, REGION, CASES) values (?, ?, ?, ?, ?, ?)", row.get('NIS5', None), datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        row.get('TX_DESCR_NL', None), constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row['CASES'].replace('<', '') if 'CASES' in row else None)
        rows_affected = iteration
    functions.logging(cursor, "Table Muni filled", ra=rows_affected)


def data_vaccins(cursor: pyodbc.Cursor) -> None:
    new_data, delete_data = functions.get_data('https://epistat.sciensano.be/Data/COVID19BE_VACC.json')
    if new_data is None:
        functions.logging(cursor, "There was an error in retrieving vaccin data")
        return

    rows_affected = 0
    for row in delete_data:
        cursor.execute("delete from Vaccins where DATE=? and REGION=? and AGEGROUP=? and SEX=? and BRAND=? and DOSE=? and COUNT=?", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('BRAND', None), row.get('DOSE', None), row.get('COUNT', None))

    for iteration, row in enumerate(new_data):
        cursor.execute("insert into Vaccins(DATE, REGION, AGEGROUP, SEX, BRAND, DOSE, COUNT) values (?, ?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('BRAND', None), row.get('DOSE', None), row.get('COUNT', None))
        rows_affected = iteration
    functions.logging(cursor, "Table Vaccins filled", ra=rows_affected)


init_db()
fill_database()
schedule.every().day.at("01:00").do(fill_database)

while True:
    schedule.run_pending()
    time.sleep(60)
