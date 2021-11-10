from typing import Optional, List
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
import json
import os
from itertools import groupby

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
    #cursor.execute("truncate table Cases")
    
    data = get_data('https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json')
    if data is None:
        logging(cursor, "There was an error in retrieving cases data")
        return

    rows_affected = 0
    for iteration, row in enumerate(data):
        cursor.execute("insert into Cases(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('CASES', None))
        rows_affected = iteration
    logging(cursor, "Table Cases filled", ra=rows_affected)


def data_mort(cursor: pyodbc.Cursor) -> None:
    #cursor.execute("truncate table Mort")
   
    data = get_data('https://epistat.sciensano.be/Data/COVID19BE_MORT.json')
    if data is None:
        logging(cursor, "There was an error in retrieving mort data")
        return
    
    rows_affected = 0
    for iteration, row in enumerate(data):
        cursor.execute("insert into Mort(DATE, REGION, AGEGROUP, SEX, DEATHS) values (?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('DEATHS', None))
        rows_affected = iteration
    logging(cursor, "Table Mort filled", ra=rows_affected)


def data_municipality(cursor: pyodbc.Cursor) -> None:
    #cursor.execute("truncate table Muni")
    
    data = get_data('https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.json')
    if data is None:
        logging(cursor, "There was an error in retrieving municipality data")
        return

    rows_affected = 0
    for iteration, row in enumerate(data):
        cursor.execute("insert into Muni(NIS5, DATE, MUNI, PROVINCE, REGION, CASES) values (?, ?, ?, ?, ?, ?)", row.get('NIS5', None), datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        row.get('TX_DESCR_NL', None), constants.provinces[row.get('PROVINCE', None)], constants.regions[row.get('REGION', None)], row['CASES'].replace('<', '') if 'CASES' in row else None)
        rows_affected = iteration
    logging(cursor, "Table Muni filled", ra=rows_affected)


def data_vaccins(cursor: pyodbc.Cursor) -> None:
    #cursor.execute("truncate table Vaccins")

    data = get_data('https://epistat.sciensano.be/Data/COVID19BE_VACC.json')
    if data is None:
        logging(cursor, "There was an error in retrieving vaccin data")
        return

    rows_affected = 0
    for iteration, row in enumerate(data):
        cursor.execute("insert into Vaccins(DATE, REGION, AGEGROUP, SEX, BRAND, DOSE, COUNT) values (?, ?, ?, ?, ?, ?, ?)", datetime.strptime(row['DATE'], DATE_FORMAT) if 'DATE' in row else None, 
        constants.regions[row.get('REGION', None)], row.get('AGEGROUP', None), row.get('SEX', None), row.get('BRAND', None), row.get('DOSE', None), row.get('COUNT', None))
        rows_affected = iteration
    logging(cursor, "Table Vaccins filled", ra=rows_affected)


def logging(cursor: pyodbc.Cursor, logging_data, **kwargs) -> None:
    rows_affected = kwargs.get('ra', 0)
    cursor.execute("insert into Logging(DATE, LOGGING, ROWS_AFFECTED) values (?, ?, ?)", datetime.now(), logging_data, rows_affected)

def get_data(url: str) -> Optional[List[dict]]:
    response = None
    try:
        response = requests.get(url,timeout=3)
        response.raise_for_status()
        filename = url.rsplit('/', 1)[-1]
        
        if os.path.isfile(filename):
            result = []
            with open(filename) as f:
                old_data = json.load(f)
                for iteration, row in enumerate(response.json()):
                    if row not in old_data:
                        result.append()
                    """if iteration > len(old_data):
                        result.append(row)
                    elif row != old_data[iteration]:
                        result.append(row)"""
                        
            if len(result) != 0:
                with open(filename, 'w') as f:
                    json.dump(response.json(), f, indent=2)
            return result
        else:
            with open(filename, 'w') as f:
                json.dump(response.json(), f, indent=2)

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
