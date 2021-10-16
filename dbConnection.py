import pyodbc
import config
import requests
import schedule
import time
from datetime import datetime

conn = pyodbc.connect(f'Driver={config.DRIVER};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

def fillDatabase():
    cursor = conn.cursor()
    
    cursor.execute("truncate table DATASET")
    
    

    res = requests.get(
        'https://epistat.sciensano.be/Data/COVID19BE_MORT.json').json()
    
    
    
    cursor.commit()

def data_cases_agesex(cursor):
    res = requests.get(
        'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json').json()

    for elem in res:
        print(elem)
        cursor.execute("insert into DATASET(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
            'DATE') is not None else None, elem.get('PROVINCE', None), elem.get('REGION', None), elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('CASES', None))

schedule.every().day.at("01:00").do(fillDatabase)

while True:
    schedule.run_pending()
    time.sleep(60)
    
# nohup python3.8 dbConnection.py &