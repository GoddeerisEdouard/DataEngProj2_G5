from datetime import datetime
import json
import os
from itertools import groupby
import pyodbc
import requests
from typing import Optional, List
import constants
import pyexcel_xlsx

DATE_FORMAT = "%Y-%m-%d"

def logging(cursor: pyodbc.Cursor, logging_data, **kwargs) -> None:
    rows_affected = kwargs.get('ra', 0)
    cursor.execute("insert into Logging(DATE, LOGGING, ROWS_AFFECTED) values (?, ?, ?)", datetime.now(), logging_data, rows_affected)

def get_data_from_xlsx(response, filename):
    with open(filename, 'wb') as f:
        f.write(response.content)
    data = pyexcel_xlsx.get_data(filename)
    sheet_name = "TH12"
    
    data_list = []
    for i in range(1, len(data[sheet_name])):
        data_list.append({
            'REFNIS' : data[sheet_name][i][0],
            'MUNI' : data[sheet_name][i][1],
            'PROVINCE' : data[sheet_name][i][7],
            'REGION' : data[sheet_name][i][10],
            'SEX' : data[sheet_name][i][12],
            'NATIONALITY' : data[sheet_name][i][14],
            'AGE' : data[sheet_name][i][19],
            'POPULATION' : data[sheet_name][i][20],
            'YEAR' : 2021
        })
    return [data_list, []]

def group_json_by_date(data: json) -> dict:
    data_group = groupby(data, lambda row: row['DATE'])
    data_dict: dict = {}
    for date, group in data_group:
        data_dict[date] = []
        for content in group:
            data_dict[date].append(content)
    return data_dict

def clean_data(data: json):
    clean_data = []
    leftover_data = []
    for row in data:
        if 'DATE' in row:
            clean_data.append(row)
        else:
            leftover_data.append(row)
    return [clean_data, leftover_data]

def get_data(url: str):# -> List[Optional[List[dict]]]:
    response = None
    try:
        response = requests.get(url,timeout=3)
        response.raise_for_status()
        filename = url.rsplit('/', 1)[-1]
        extension = url.rsplit('.', 1)[-1]
        
        if extension == "xlsx":
            return get_data_from_xlsx(response, filename)
        
        if not os.path.isfile(filename):
            with open(filename, 'w') as f:
                json.dump(response.json(), f, indent=2)
            return [response.json(), []]
        
        insert_data = []
        delete_data = []
        with open(filename) as f:
            old_data = json.load(f)
                
        if old_data == response.json():
            return [insert_data, delete_data]
                
        clean_response_data, leftover_response_data = clean_data(response.json())
        clean_old_data, leftover_old_data = clean_data(old_data)
                
        # Veranderd de json-data naar een lijst met datum als key en de bijhorende json-objecten als data in een dictionary
        old_data_dict = group_json_by_date(clean_old_data)
        response_data_dict = group_json_by_date(clean_response_data)
            
        if leftover_old_data != leftover_response_data:
            for row in leftover_old_data:
                delete_data.append(row)
            for row in leftover_response_data:
                insert_data.append(row)
            
        for key in sorted(response_data_dict, key=lambda x:x, reverse=True):
            if old_data_dict == response_data_dict:
                with open(filename, 'w') as f:
                    json.dump(response.json(), f, indent=2)
                return [insert_data, delete_data]
                    
            if key not in old_data_dict:
                for row in response_data_dict[key]:
                    insert_data.append(row)
                response_data_dict.pop(key)
            elif response_data_dict[key] != old_data_dict[key]:
                for row in old_data_dict[key]:
                    delete_data.append(row)
                for row in response_data_dict[key]:
                    insert_data.append(row)
                        
        with open(filename, 'w') as f:
            json.dump(response.json(), f, indent=2)
        return [insert_data, delete_data]

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
    return [response, []]

def variable_switch(key, value):
    SQL_STR_TRANS = {
        "DATE" : date_append(value),
        "REGION" : constants.regions[value],
        "PROVINCE" : constants.provinces[value.rsplit(' ', 1)[-1] if type(value) == str else value],
        "CASES" : value.replace('<', '') if type(value) == str else value
    }
    return SQL_STR_TRANS.get(key, value)
        
def date_append(value):
    try:
        assert datetime.strptime(str(value), DATE_FORMAT)
        return datetime.strptime(value, DATE_FORMAT) 
    except:
        return None

def sql_insert_into(table, query_variables):
    return f"INSERT INTO {table}({', '.join(query_variables)}) values ({('?, ' * len(query_variables))[:-2]});"

def sql_delete_where(table, query_variables):
    return f"DELETE FROM {table} WHERE {' = ? AND '.join(query_variables)} = ?;"
