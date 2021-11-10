from typing import Optional
import pyodbc
import requests
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
        logging(cursor, "Database has been initialized")


def fill_database() -> None:
    with open_db_connection(CONN_STRING, commit=True) as cursor:
        try:
            data_cases_agesex(cursor)
            data_mort(cursor)
            data_municipality(cursor)
            data_vaccins(cursor)
            logging(cursor, "Database filled")
        except pyodbc.DatabaseError as err:
            logging(cursor, f"Database Error {err.args[1]}")


def data_cases_agesex(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Cases")
    
    data = get_data(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json')
    if data is None:
        logging(cursor, "There was an error in retrieving cases data")
        return

    for row in data:
        cursor.execute("insert into Cases(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('CASES', None))
    logging(cursor, "Table Cases filled")


def data_mort(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Mort")
   
    data = get_data(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_MORT.json')
    if data is None:
        logging(cursor, "There was an error in retrieving mort data")
        return
    
    for row in data:
        cursor.execute("insert into Mort(DATE, REGION, AGEGROUP, SEX, DEATHS) values (?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('DEATHS', None))
    logging(cursor, "Table Mort filled")


def data_municipality(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Muni")
    
    data = get_data(cursor, 'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json')
    if data is None:
        logging(cursor, "There was an error in retrieving municipality data")
        return

    for row in data:
        cursor.execute("insert into Muni(NIS5, DATE, MUNI, PROVINCE, REGION, CASES) values (?, ?, ?, ?, ?, ?)", row.get('NIS5', None), datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        row.get('TX_DESCR_NL', None), constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row['CASES'].replace('<', '') if 'CASES' in row else None)
    logging(cursor, "Table Muni filled")


def data_vaccins(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Vaccins")

    data = get_data('https://epistat.sciensano.be/Data/COVID19BE_VACC.json')
    if data is None:
        logging(cursor, "There was an error in retrieving vaccin data")
        return

    for row in data:
        cursor.execute("insert into Vaccins(DATE, REGION, AGEGROUP, SEX, BRAND, DOSE, COUNT) values (?, ?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('BRAND', None), row.get('DOSE', None), row.get('COUNT', None))
    logging(cursor, "Table Vaccins filled")


def logging(cursor: pyodbc.Cursor, logging_data) -> None:
    cursor.execute("insert into Logging(DATE, LOGGING) values (?, ?)", datetime.now(), logging_data)

def get_data(url: str) -> Optional[list[dict]]:
    response = None
    try:
        response = requests.get(url,timeout=3)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("Http Error: ", errh)
        pass
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting: ", errc)
        pass
    except requests.exceptions.Timeout as errt:
        print("Timeout Error: ", errt)
        pass
    except requests.exceptions.RequestException as err:
        print("Oops, Something Else: ", err)
        pass
    return response.json() if response is not None else response

init_db()
fill_database()
schedule.every().day.at("01:00").do(fill_database)

while True:
    schedule.run_pending()
    time.sleep(60)
