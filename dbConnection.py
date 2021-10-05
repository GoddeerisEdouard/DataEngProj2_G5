import pyodbc
import config

conn = pyodbc.connect(f'Driver={config.DRIVER};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM DATASET')

kak = cursor.fetchall()
print(kak)
