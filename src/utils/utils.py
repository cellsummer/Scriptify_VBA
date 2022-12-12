#! /usr/bin/env python3
"""Utility functions
"""

import json
from os.path import exists
from pathlib import Path

# from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
import pandas as pd
import shutil


class AttributeDict(dict):
    """
    Special dictionary with attributes.
    """

    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value


def read_config(config_file: str, fields: list = []) -> AttributeDict:
    """Read json config file
    Read a generic json file as config. Returns dict/class-like object.

    Args:
        config_file (str): config file path or path string
        fields (list): A list of fields to read in. Default to read all fields

    Returns:
        (AttributeDict): A special dictionary that can be referenced by key or attribute

    Examples:
        >>> my_config = read_config('path/to/config.json')
        >>> print(my_config['first_key'])
        'first_key_value'
        >>> print(my_config.second_key)
        'second_key_value'
    """

    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    if not fields:
        fields = config.keys()

    if not set(fields).issubset(set(config.keys())):
        raise KeyError(f"Some of the fields in {fields} don't exit in config file")

    config_filtered = {key: config[key] for key in fields}

    return AttributeDict(config_filtered)


def write_df_to_excel(
    df: pd.DataFrame,
    excel_file: str,
    excel_tab: str,
    startrow: int = 0,
    startcol: int = 0,
) -> None:
    """write dataframe to excel
    If excel file/tab does not exist: create a new excel file/tab
    Otherwise, overwrite to the existing tab

    Args:
        df (DataFrame): data to write
        excel_file (str): excel file path
        excel_tab (str): tab to write to
        startrow (int): starting row (0-based)
        startcol (int): starting row (0-based)

    Returns:
        None

    Examples:
        >>> write_df_to_excel(df, "otuput.xlsx", "summary_tab")
    """
    if exists(excel_file):
        # if excel file already exists, make a bak copy
        file_ext = Path(excel_file).suffix
        file_base_name = Path(excel_file).stem
        file_directory = Path(excel_file).parent
        bak_file = Path(file_directory) / f"{file_base_name}_bak{file_ext}"
        shutil.copy(excel_file, bak_file)
        mode = "a"
        if_sheet_exists = "overlay"
    else:
        mode = "w"
        if_sheet_exists = None

    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        excel_file, mode=mode, if_sheet_exists=if_sheet_exists
    ) as xl_writer:

        df.to_excel(
            xl_writer,
            engine="openpyxl",
            sheet_name=excel_tab,
            header=True,
            index=False,
            startcol=startcol,
            startrow=startrow,
        )


fields = ["server", "db", "table"]

config = read_config("configs/my_config.json", fields)
# config = read_config("configs/my_config.json", )
sample_df = pd.DataFrame(data={"fields": fields})
xl_file = "temp/out1.xlsx"

write_df_to_excel(sample_df, xl_file, "reserves", startrow=4)


print(config)
print(config.server)
print(config.db)
print(config["table"])
