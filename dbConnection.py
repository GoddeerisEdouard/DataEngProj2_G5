import pyodbc
import config

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
<<<<<<< HEAD
cursor.execute('SELECT * FROM DATASET')
=======
cursor.execute('SELECT * FROM DATASET')

kak = cursor.fetchall()
print(kak)
>>>>>>> refs/remotes/origin/main
