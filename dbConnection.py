import pyodbc
import config
import requests
from datetime import datetime

conn = pyodbc.connect(f'Driver={config.DRIVER};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM DATASET')

kak = cursor.fetchall()
<<<<<<< HEAD
print(kak)
=======

res = requests.get(
    'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json').json()

for elem in res:
    print(elem)
    cursor.execute("insert into DATASET(DATE, PROVINCE, REGION, AGEGROUP, SEX, CASES) values (?, ?, ?, ?, ?, ?)", datetime.strptime(elem['DATE'], "%Y-%m-%d") if elem.get(
        'DATE') is not None else None, elem.get('PROVINCE', None), elem.get('REGION', None), elem.get('AGEGROUP', None), elem.get('SEX', None), elem.get('CASES', None))

cursor.commit()
>>>>>>> refs/remotes/origin/main
