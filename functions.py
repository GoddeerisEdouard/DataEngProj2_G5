from datetime import datetime
import json
import pyodbc
import requests
from typing import List, Union
import constants
import pyexcel_xlsx
from deepdiff import DeepDiff
import re

DATE_FORMAT = "%Y-%m-%d"
DATASET_DIR = "dataset_files"

def logging(cursor: pyodbc.Cursor, logging_data: str, **kwargs) -> None:
    rows_affected = kwargs.get('ra', 0)
    cursor.execute("insert into Logging(DATE, LOGGING, ROWS_AFFECTED) values (?, ?, ?)", datetime.now(), logging_data, rows_affected)

def get_and_write_data_to_file(url: str, **kwargs) -> List[dict]:
    try:
        response = requests.get(url)
        response.raise_for_status()
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
        raise err
    
    filename = url.split('/')[-1]
    filepath = f"{DATASET_DIR}/{filename}"
    extension = filename.split('.')[-1]
    json_data = []
    sheet_name = kwargs.get('sheet_name', None)

    if extension == "xlsx":
        filepath = f"{DATASET_DIR}/{filename}"
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        data = pyexcel_xlsx.get_data(filepath)
        year = filename.split('_')[-1].split('.')[0]
        
        for i in range(1, len(data[sheet_name])):
            row = {}
            for c in range(len(data[sheet_name][i])):
                row[data[sheet_name][0][c]] = data[sheet_name][i][c]
            row['YEAR'] = year
            json_data.append(row)
        return json_data

    json_data = response.json()
    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=2)
    return json_data


def get_dict_diff(old_data_dict: List[dict], new_data_dict: List[dict]) -> dict:
    result: dict = {
        "insert_data" : [],
        "delete_data" : [],
        "update_data" : []
    }

    if old_data_dict == []:
        for row in new_data_dict:
            for i in range(len(row)):
                row[list(row.keys())[i]] = convert_to_correct_value(list(row.keys())[i], list(row.values())[i])
        result['insert_data'] = new_data_dict
        return result
    
    diff_old_new_data = DeepDiff(old_data_dict, new_data_dict, ignore_order=True)
    if not diff_old_new_data:
        return None

    if 'iterable_item_added' in diff_old_new_data:
        insert_data_result = []
        for i, row in enumerate(diff_old_new_data['iterable_item_added']):
            value = list(diff_old_new_data['iterable_item_added'].values())[i]
            row_dict = {}
            for p in range(len(value)):
                row_dict[list(value.keys())[p]] = convert_to_correct_value(list(value.keys())[p], list(value.values())[p])
            insert_data_result.append(row_dict)
        result['insert_data'] = insert_data_result
    if 'iterable_item_removed' in diff_old_new_data:
        result['delete_data'] = [int(re.findall(r"\d+", key)[0]) + 1 for key in list(diff_old_new_data['iterable_item_removed'].keys())]
    if 'values_changed' in diff_old_new_data:
        for i, key in enumerate(diff_old_new_data['values_changed'].keys()):
            row = {
                "id" : None,
                "column_changes" : {}
            }
            row["id"] = int(re.findall(r"\d+", key)[0]) + 1
            column_name = re.findall(r"'([^']*)'", key)[0]
            raw_new_value = list(diff_old_new_data['values_changed'].values())[i]['new_value']
            row["column_changes"][column_name] = convert_to_correct_value(column_name, raw_new_value)
            diff_old_new_data['values_changed']
            result['update_data'].append(row)
    return result


def convert_to_correct_value(column_name: str, value: Union[str, int]) -> Union[str, int]:
    valid_column_name = constants.request_key_to_column_name.get(column_name, column_name)
    valid_value = constants.request_value_to_column_value.get(value, value)
    code_to_valid_prov_reg = constants.code_to_prov_reg_name.get(valid_value, valid_value)
    if valid_column_name == "DATE":
        return date_append(valid_value) if type(valid_value) == str else None
    if valid_column_name == "REGION":
        if "BE" in valid_value:
            return code_to_valid_prov_reg
        return constants.regions[valid_value]
    if valid_column_name == "PROVINCE":
        if "BE" in valid_value:
            return code_to_valid_prov_reg
        if "Provincie " in valid_value:
            return valid_value.split(' ')[-1]
        return constants.provinces[valid_value]
    if valid_column_name == "CASES":
        return valid_value.replace('<', '') if type(valid_value) == str else valid_value
    if valid_column_name == "AGEGROUP":
        return valid_value.replace(' en ouder', '+') if type(valid_value) == str else valid_value
    if valid_column_name == "SEX":
        if valid_value == "1":
            return "M"
        elif valid_value == "2":
            return "F"
        else:
            return valid_value
    if valid_column_name == "INCOME":
        return int(valid_value)
    else:
        return valid_value

        
def date_append(value: str):
    try:
        return datetime.strptime(value, DATE_FORMAT)
    except ValueError:
        return None

def sql_insert_into(table: str, query_variables: List[str]) -> str:
    return f"INSERT INTO {table}({', '.join(query_variables)}) values ({('?, ' * len(query_variables))[:-2]});"

def sql_delete_where(table: str) -> str:
    return f"DELETE FROM {table} WHERE ID = ?;"

def sql_update_where(table: str, column_names_to_be_updated: List[str]) -> str:
    return f"UPDATE {table} SET {' = ?, '.join(column_names_to_be_updated)} = ? WHERE ID = ?;"