
#! /usr/bin/env python3

"""pseudo scripts to deal with tabular dataframe

"""

import pandas as pd


class PolicyInfo:
    """Group of mapping functions for policy information"""

    def __init__(self, data=[]):
        self.df = pd.DataFrame(data=data)

    def load_data_from_sql(self, conn, sql) -> None:
        self.df = pd.read_sql_query(conn, sql)

    def _map_stat_code(self, row) -> str:
        # dependencies = ["policy_id", "lpolicy_id"]

        # if not all([d in row for d in dependencies]):
        #     raise KeyError(
        #         f"Couldn't find one or more mapping drivers {dependencies}!"
        #     )

        if row.lpolicy_id == "aaa":
            return "CSO80"

        return "CET80"

    def _map_admin_system(self, row) -> str:
        admin_maps = {"aaa": "flex", "bbb": "mips", "ccc": "schd"}
        return admin_maps[row.policy_id]


def main():
    data = {
        "policy_id": ["aaa", "bbb", "ccc"],
        "age": [30, 40, 50],
        "gender": ["M", "F", "M"],
    }
    pit = PolicyInfo(data)
    print(pit.df.policy_id)
    pit.df["stat_code"] = pit.df.apply(pit._map_stat_code, axis=1)
    pit.df["system"] = pit.df.apply(pit._map_admin_system, axis=1)
    print(pit.df)
    print("hello")


if __name__ == "__main__":
    main()
