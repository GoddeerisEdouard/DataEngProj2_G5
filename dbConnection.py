import pyodbc
from requests.exceptions import RequestException
import config
import constants
import sql_statements
import requests
import schedule
import time
from datetime import datetime
from contextlib import contextmanager
import pyodbc
import sys

CONN_STRING = f'DRIVER={config.DRIVER};SERVER=tcp:{config.SERVER};PORT=1433;DATABASE={config.DATABASE};UID={config.USERNAME};PWD={config.PASSWORD}'


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
        logging(cursor, f"Database has been initialized | {datetime.now()}")


def fillDatabase() -> None:
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        try:
            data_cases_agesex(cursor)
            data_mort(cursor)
            data_municipality(cursor)
            data_vaccins(cursor)
        except pyodbc.DatabaseError as err:
            logging(cursor, f"Database Error | {datetime.now()}")

        logging(cursor, f"Database filled | {datetime.now()}")

def data_cases_agesex(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Cases")
    
    res = getData(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json')

    for elem in res:
        cursor.execute("insert into Cases(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, constants.provinces[elem.get('PROVINCE', None)], constants.regions[elem.get('REGION', None)], elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('CASES', None))

    logging(cursor, f"Table Cases filled | {datetime.now()}")


def data_mort(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Mort")
   
    res = getData(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_MORT.json')

    for elem in res:
        cursor.execute("insert into Mort(DATE, REGION, AGEGROUP, SEX, DEATHS) values (?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, constants.regions[elem.get('REGION', None)], elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('DEATHS', None))

    logging(cursor, f"Table Mort filled | {datetime.now()}")


def data_municipality(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Muni")
    
    res = getData(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json')

    for elem in res:
        cursor.execute("insert into Muni(NIS5, DATE, MUNI, PROVINCE, REGION, CASES) values (?, ?, ?, ?, ?, ?)", elem.get('NIS5', None), datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, elem.get('TX_DESCR_NL', None), constants.provinces[elem.get('PROVINCE', None)], constants.regions[elem.get('REGION', None)], elem.get('CASES', None))

    logging(cursor, f"Table Muni filled | {datetime.now()}")


def data_vaccins(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Vaccins")

    res = getData(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_VACC.json')

    for elem in res:
        cursor.execute("insert into Vaccins(DATE, REGION, AGEGROUP, SEX, BRAND, DOSE, COUNT) values (?, ?, ?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, constants.regions[elem.get('REGION', None)], elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('BRAND', None), elem.get('DOSE', None), elem.get('COUNT', None))

    logging(cursor, f"Table Vaccins filled | {datetime.now()}")


def logging(cursor: pyodbc.Cursor, logging_data) -> None:
    cursor.execute("insert into Logging(LOGGING) values (?)", logging_data)

def getData(cursor, data):
    try:
        return requests.get(data).json()
    except requests.RequestException as err:
        logging(cursor, f"There was an error in retrieving: {data}")

init_db()
fillDatabase()
schedule.every().day.at("01:00").do(fillDatabase)

while True:
    schedule.run_pending()
    time.sleep(60)
