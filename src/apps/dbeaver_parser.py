"""parse dbeaver sql script with variables
"""
import re
from pathlib import Path

def replace_variable(line: str, var_defs: dict[str, str]) -> str:
    """replace ${var} with dict[var]"""
    new_line = line
    for var in re.findall(r"\$\{([a-z_A-Z]*)\}", line):
        new_line = new_line.replace(f"${{{var}}}", var_defs[var])

    return new_line


def parse_lines(lines: list):
    var_defs = {}
    new_lines = ["-- === Generated From Parameterized SQL Script ==="]
    for line in lines:
        new_line =replace_variable(line, var_defs)
        # line start with $set
        if new_line[:4] == "@set":
            var_key = new_line[4:].split("=")[0].strip()
            var_value = new_line[4:].split("=")[1].strip()
            var_defs[var_key] = var_value
        else:
            new_lines.append(new_line)

    return new_lines

def parse_dbeaver(in_file, out_file=None):
    with open(in_file, 'r') as input:
        lines = input.readlines() 

    new_lines = parse_lines(lines)
    # append _parsed if not supplied
    if not out_file:
        dir = Path(in_file).parent
        basename = Path(in_file).stem + '_parsed'
        ext = 'sql'
        out_file = Path(dir) / f'{basename}.{ext}'

    with open(out_file, 'w+') as output:
        output.writelines(new_lines)

    return 



if __name__ == "__main__":
    var_defs = {
        "var": "column1",
        "table": "commission_table",
        "my_table": "another_tbl",
        "condition": "a=1",
    }
    lines = [
        "@set valdate = 202206",
        "@set table_name = my_table_${valdate}",
        "@set unit = 1000",
        "@set url = s3://arrprod-amazon.com/arr-repo/${table_name}",
        "",
        "",
        "SELECT",
        "	${valdate} as VALDATE,",
        "	'${table_name}' as my_table,",
        "	'${url}' as URL,",
        "	321 / ${unit} as results," "	now() as query_time",
        "            ]",
    ]
    # replace_variable(test, var_defs)
    parse_dbeaver('Scripts/test.sql')
