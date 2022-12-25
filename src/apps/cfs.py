#! /usr/bin/env python3

"""module to deal with cashflows
"""

# TODO
# given cashflow name (column name) and discount column, calculate the present value
# translate forward rate to discount factors
# standardize column name for scenario number and projetion month
import re
import pandas as pd
from utils.utils import AttributeDict, read_config


class CFs:
    """stochastic cashflow class"""

    _COL_PROJ_MTH = "t"
    _COL_DISCOUNT_FACTOR = "disc"
    _COL_SCENARIO_NUMBER = "scenario_no"
    _N = 2

    def __init__(self, params: AttributeDict, df: pd.DataFrame):
        """
        Args:
            params (AttributeDict): dict-like parameters list
            df (pd.DataFrame): cashflows in dataframe
        """
        self.params = params
        self.df = df

    def rename_df_columns(self) -> None:
        """rename df columns"""
        # rename must-have columns
        standard_columns_rename = {
            self.params.proj_mth_col: self._COL_PROJ_MTH,
            # "discount factor - risk free rate": "disc",
            self.params.discount_col: self._COL_DISCOUNT_FACTOR,
            self.params.scenario_number_col: self._COL_SCENARIO_NUMBER,
        }

        self.df.rename(columns=standard_columns_rename, inplace=True)

        # make all column names lower case letter connected with underscores
        self.df.rename(mapper=self.standardize_column_name, axis=1, inplace=True)

        return

    def calc_equivalent_weights(
        self,
    ) -> None:
        """use discount factor as weights (by scenarios)"""
        total_discount_factors = self.df[["t", "disc"]].groupby(["t"]).sum()
        total_discount_factors.reset_index()
        total_discount_factors = pd.concat(
            [total_discount_factors] * self._N, ignore_index=True
        )
        total_discount_factors.reset_index()
        self.df.loc[:, "total_discount_factors"] = total_discount_factors.loc[:, "disc"]
        self.df["weights"] = self.df["disc"] / self.df["total_discount_factors"]

        return

    @staticmethod
    def standardize_column_name(raw_name: str) -> str:
        """standardize column names
        All column names should be lower case, no spaces(replace with underscores); no special chars
        Allowed characters are [a-z_0-9]
        Args:
            raw_name (str): original column name
        Returns:
            (str): standardized column name
        Example:
            >>> standardize_column_names('seg/separate Acct - Expenses(*)')
            >>> seg_separate_acct_expenses
        """

        # first make the string all lower case
        new_name = raw_name.lower()

        # Second replace all chars that are not in a-z, 0-9, underscore and spaces with spaces
        regex = re.compile(r"[^0-9a-z_\s]")
        new_name = regex.sub(" ", new_name)

        # third, remove leading and trailing spaces
        new_name = new_name.strip()

        # next replace one or more spaces with a single underscore
        regex = re.compile(r"\s+")
        new_name = regex.sub("_", new_name)

        return new_name


def pv_by_scenario(cf_col: str, discount_col: str, df: pd.DataFrame):
    pass


if __name__ == "__main__":
    # test_string = "seg/separate Acct - Expenses(*)"
    # print(test_string)
    # print(standardize_column_names(test_string))
    # test_string = r"(Main)/Row - Year 1 - Expenses + Commissions"
    # print(test_string)
    # print(standardize_column_names(test_string))

    params = read_config("configs/cfs_config.json")
    df = pd.DataFrame(data=pd.read_csv("temp/ifrs17_cfs.csv"))
    cfs = CFs(params, df)
    cfs.rename_df_columns()
    print(cfs.df.columns)
