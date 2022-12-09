#! /usr/bin/env python3

# from collections import namedtuple
from typing import Any
import json

class AttributeDict(dict):
    '''
    Dictionary with attributes. 
    '''
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value

def read_config(config_file: str, fields: list) -> Any:
    # Config = namedtuple("Config", fields)
    
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    if not set(fields).issubset(set(config.keys())):
        raise KeyError(f"Some of the fields in {fields} don't exit in config file")
    
    config_filtered = {key:config[key] for key in fields}

    return AttributeDict(config_filtered)

fields = ["server", "db", "table",]

config = read_config("my_config.json", fields)


print(config)
print(config.server)
print(config.db)
print(config['table'])
