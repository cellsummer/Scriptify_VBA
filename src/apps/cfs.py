#! /usr/bin/env python3

"""module to deal with cashflows
"""

# TODO
# given cashflow name (column name) and discount column, calculate the present value
# translate forward rate to discount factors
# standardize column name for scenario number and projetion month
import re
import pandas as pd
from utils.utils import AttributeDict, read_config, logger
from typing import Dict


class CFs:
    """stochastic cashflow class"""

    _COL_PROJ_MTH = "t"
    _COL_DISCOUNT_FACTOR = "disc"
    _COL_SCENARIO_NUMBER = "scenario_no"
    _COL_ESS_WEIGHTS = "ess_weights"
    # number of stochastic scenarios
    _N = 2

    def __init__(self, params: AttributeDict, df: pd.DataFrame):
        """
        Args:
            params (AttributeDict): dict-like parameters list
            df (pd.DataFrame): cashflows in dataframe
        """
        self.params = params
        self.df = df
        self.rename_df_columns()
        self.clean_params()
        self.grouped_cfs = []

    def clean_params(
        self,
    ) -> None:
        """clean up the params entries from user\'s config"""
        # standardize cfs
        for idx, cf_name in enumerate(self.params.cfs):
            self.params.cfs[idx] = self.standardize_column_name(cf_name)

        # common filters
        self.filter_is_stochastic = (self.df[self._COL_SCENARIO_NUMBER] > 0) & (
            self.df[self._COL_SCENARIO_NUMBER] <= 1000
        )

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
        """use discount factor as weights (by stochastic scenarios)"""
        total_discount_factors = (
            self.df.loc[
                self.filter_is_stochastic,
                [self._COL_PROJ_MTH, self._COL_DISCOUNT_FACTOR],
            ]
            .groupby([self._COL_PROJ_MTH])
            .sum()
        )
        total_discount_factors.reset_index()
        total_discount_factors = pd.concat(
            [total_discount_factors] * self._N, ignore_index=True
        )
        total_discount_factors.reset_index()
        self.df.loc[:, "total_discount_factors"] = total_discount_factors.loc[
            :, self._COL_DISCOUNT_FACTOR
        ]
        self.df[self._COL_ESS_WEIGHTS] = (
            self.df[self._COL_DISCOUNT_FACTOR] / self.df["total_discount_factors"]
        )
        self.df.drop(columns=["total_discount_factors"], inplace=True)

        return

    def calc_ess_cfs(self) -> None:
        """calculate equivalent single scenario cashflows
        This calculation will append the ESS scenario to self.df with scenario_no set to -1
        """

        # temp column for the weighted cashflows
        suffix = "weighted_"

        # raw cashflows + goruped cashflows
        all_cfs = self.params.cfs + self.grouped_cfs

        for cf_name in all_cfs:
            weighted_cf_name = suffix + cf_name
            self.df[weighted_cf_name] = (
                self.df[cf_name] * self.df[self._COL_ESS_WEIGHTS]
            )

        ess_df = (
            self.df.loc[
                self.filter_is_stochastic,
                [self._COL_PROJ_MTH] + [suffix + col for col in all_cfs],
            ]
            .groupby(self._COL_PROJ_MTH)
            .sum()
        )

        # drop temp weighted columns
        self.df.drop(columns=[suffix + col for col in all_cfs], inplace=True)

        rename_mapper = {suffix + col: col for col in all_cfs}
        ess_df.rename(columns=rename_mapper, inplace=True)

        # set scenario number to be -1
        ess_df[self._COL_SCENARIO_NUMBER] = -1

        # set discount factor to be average of stochastic discount rates
        ess_df[self._COL_DISCOUNT_FACTOR] = (
            self.df.loc[
                self.filter_is_stochastic,
                [self._COL_PROJ_MTH, self._COL_DISCOUNT_FACTOR],
            ]
            .groupby(self._COL_PROJ_MTH)
            .mean()[self._COL_DISCOUNT_FACTOR]
        )
        ess_df.reset_index(inplace=True)

        # concat with self.df
        self.df = pd.concat([self.df, ess_df])

        return

    def _calc_pvs(
        self,
    ) -> pd.DataFrame:
        """calculate pv of cashflows by scenario"""
        results = AttributeDict()
        pv_col = "pv"

        # raw cashflows + goruped cashflows
        all_cfs = self.params.cfs + self.grouped_cfs

        for col in all_cfs:
            self.df[pv_col] = self.df[col] * self.df[self._COL_DISCOUNT_FACTOR]
            results[col] = (
                self.df.loc[:, [pv_col, self._COL_SCENARIO_NUMBER]]
                .groupby(self._COL_SCENARIO_NUMBER)
                .sum()[pv_col]
            )

        self.df.drop(columns=pv_col, inplace=True)

        return pd.DataFrame(data=results)


    def group_cfs(self, groupings: list[str]) -> None:
        '''group cashflows use the grouping defined in the params
        Args:
            groupings (list): list of groupings to add to the dataframe 
        Returns:
            None: new columns are added to self.df. new columns are named as <grouping_def>.<group_name>
        '''
        for grouping_def in groupings:
            # check if it is defined in params
            if grouping_def not in self.params.cfs_groupings:
                logger.warn(f'{grouping_def} is not defined in params: Ignored')
                continue
            
            for local_group_name, group_def in self.params.cfs_groupings[grouping_def].items():
                global_group_name = f'{grouping_def}.{local_group_name}'
                self._add_cfs_group(global_group_name, group_def)

        return
    
    def _add_cfs_group(self, group_name: str, group_def: Dict[str, int]) -> None:
        '''add an aggregate of cashflows to self.df
        Args:
            group_name (str): the suffix added to the new cashflow group name
            group_def (dict): definition of the cashflow group. Key is cashflow name. Value is sign.
        Returns:
            None: A new columns is added to self.df, named as group_name. 
        '''

        self.df[group_name] = 0

        for cf, sign in group_def.items():
            self.df[group_name] += self.df[cf] * sign

        # update grouped cfs 
        self.grouped_cfs.append(group_name)

        return

    def calc_bels(self, bel_definitions: list[str]) -> pd.DataFrame:
        '''Calculate Bels in each scenario
        Args:
            bel_definitions (list): list of bel definitions
        Returns:
            pd.DataFrame: dataframe (index is scenario, column is bel defintions)
        '''
        pvs = self._calc_pvs()
        for bel_def_name in bel_definitions:
            # loop through all bel_defintions
            if bel_def_name not in self.params.bel_definitions:
                # bel definition not in params, do nothing
                logger.warn(f"{bel_def_name} is not defined in self.params")
                continue
            
            bel_def = self.params.bel_definitions[bel_def_name]
            bel_colum =f"{bel_def_name}.bel" 
            pvs[bel_colum] = 0
            for cf, sign in bel_def.items():
                # loop through all cashflow components in current bel defintion
                # TODO move this part to a separate function
                cf_col = f'{bel_def_name}.{cf}'
                pvs[bel_colum] += pvs[cf_col] * sign

        return pvs 


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
    print(cfs.df.columns)
