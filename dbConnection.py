import pyodbc
import config
import requests
import schedule
import time
from datetime import datetime

conn = pyodbc.connect(f'DRIVER={config.DRIVER};SERVER=tcp:{config.SERVER};PORT=1433;DATABASE={config.DATABASE};UID={config.USERNAME};PWD={config.PASSWORD}')

def init_db() -> pyodbc.Cursor:
    sql_create_cases_table = """
    IF OBJECT_ID(N'dbo.Cases', N'U') IS NULL
    BEGIN
        CREATE TABLE Cases(
        [DATE] DATE,
        PROVINCE TEXT,
        REGION TEXT,
        AGEGROUP TEXT,
        SEX VARCHAR(1),
        CASES INT
        )
    END
    """

    sql_create_mort_table = """
    IF OBJECT_ID(N'dbo.Mort', N'U') IS NULL
    BEGIN
        CREATE TABLE Mort(
        [DATE] DATE,
        REGION TEXT,
        AGEGROUP TEXT,
        SEX VARCHAR(1),
        DEATHS INT
        )
    END"""

    with conn.cursor() as cursor:
        cursor.execute(sql_create_cases_table)
        cursor.execute(sql_create_mort_table)
        cursor.commit()
    print("database has been initialized!")


def fillDatabase() -> None:
    with conn.cursor() as cursor:
        data_cases_agesex(cursor)
        data_mort(cursor)
        cursor.commit()
    print(f"filled db at {datetime.now()}")


def data_cases_agesex(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Cases")

    res = requests.get(
        'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json').json()
    
    for elem in res:
        cursor.execute("insert into Cases(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, elem.get('PROVINCE', None), elem.get('REGION', None), elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('CASES', None))


def data_mort(cursor: pyodbc.Cursor) -> None:
    cursor.execute("truncate table Mort")

    res = requests.get(
        'https://epistat.sciensano.be/Data/COVID19BE_MORT.json').json()

    for elem in res:
        cursor.execute("insert into Mort(DATE, REGION, AGEGROUP, SEX, DEATHS) values (?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, elem.get('REGION', None), elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('DEATHS', None))

init_db()
schedule.every().day.at("01:00").do(fillDatabase)

while True:
    schedule.run_pending()
    time.sleep(60)