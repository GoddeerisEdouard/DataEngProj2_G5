import pyodbc
import config

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM DATASET')

kak = cursor.fetchall()
print(kak)
