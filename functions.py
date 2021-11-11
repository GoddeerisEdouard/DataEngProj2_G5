import datetime
import json
import os
from itertools import groupby
import pyodbc
import requests
from typing import Optional, List

def logging(cursor: pyodbc.Cursor, logging_data, **kwargs) -> None:
    rows_affected = kwargs.get('ra', 0)
    cursor.execute("insert into Logging(DATE, LOGGING, ROWS_AFFECTED) values (?, ?, ?)", datetime.now(), logging_data, rows_affected)

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
                
                if old_data == response.json():
                    return response
                
                new_data = clean_data(response.json())
                
                # Veranderd de json-data naar een lijst met datum als key en de bijhorende json-objecten als data in een dictionary
                old_data_dict = group_json_by_date(clean_data(old_data)[0])
                response_data_dict = group_json_by_date(new_data[0])
                
                for key in response_data_dict:
                    if key not in old_data_dict:
                        for row in response_data_dict[key]:
                            result.append(row)
                    elif response_data_dict[key] != old_data_dict[key]:
                            for row in response_data_dict[key]:
                                result.append(row)
                
                if new_data[1] != []:
                    for row in new_data[1]:
                        result.append(row)
                
                with open("test.json", 'w') as f:
                    json.dump(result, f, indent=2)
                        
            if len(result) != 0:
                with open(filename, 'w') as f:
                    json.dump(response.json(), f, indent=2)
            return result
        else:
            with open(filename, 'w') as f:
                json.dump(response.json(), f, indent=2)
            return response.json()

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
    return response