import os
import json
import pandas as pd


class FMT:
    def __init__(
        self,
    ):
        self.none_existed_combos = []

    def _load_config(self, config: str):
        if not os.path.exists(config):
            raise FileNotFoundError("File: {config} doesn't exist.")

        with open(config, "r") as file:
            fmt_config = json.load(file)

        print(f"{config} has been loaded")
        print(fmt_config)

    def _load_pit(
        self,
    ):
        pass

    def _load_fmt(
        self,
    ):
        pass

    def _get_combos(self, df, columns, sep="|"):
        """
        return a list of combos from a dataframe
        """
        if df.empty:
            return []

        if columns is None:
            msg = "Empty list of columns specified in function get_combo"
            raise Exception(msg)
            return

        if any(col not in df.columns for col in columns):
            msg = "Some of the tags don't exist in the columns of dataframe."
            raise Exception(msg)
            return

        combos = [
            sep.join(
                [str(_) for _ in df.loc[row_no, columns]]
            ) for row_no in df.index
        ]

        return combos

    def get_pit_combos(tab: str):
        pass

    def get_fmt_combos(tab: str):
        pass

    def check_exists(self, fmt_combos, pit_combos):
        '''
        check if pit combos all exist in fmt_combos
        '''
        if set(pit_combos).issubset(set(fmt_combos)):
            return True

        self.none_existed_combos = list(set(pit_combos) - set(fmt_combos))
        return False


def main():
    df = pd.DataFrame(
        data={
            "age": [34, 33, 40],
            "gender": ["m", "f", "m"],
            "rating": ["R0", "R0", "R1"],
        }
    )
    cols = ['age', 'gender', 'rating']
    cols_wrong = ['ddd']
    my_fmt = FMT()
    combos = my_fmt._get_combos(df, cols[:2])
    # for row_no in df.index:
    #     row = df.loc[row_no, :]
    #     for key in row:
    #         print(key)
    print(combos)
    print(my_fmt.check_exists(combos, combos+['ss']))
    print(my_fmt.none_existed_combos)


if __name__ == "__main__":
    main()
