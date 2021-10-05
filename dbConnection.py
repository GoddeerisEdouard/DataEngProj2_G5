import pyodbc
import config

conn = pyodbc.connect('Driver={SQL Server};'
                      f'Server={config.SERVER};'
                      f'Database={config.DATABASE};'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM DATASET')