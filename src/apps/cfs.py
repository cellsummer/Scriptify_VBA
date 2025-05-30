#! /usr/bin/env python3

"""module to deal with cashflows
"""

# TODO
# translate forward rate to discount factors
# standardize column name for scenario number and projetion month
import re
import pandas as pd
from utils.utils import AttributeDict, logger
from typing import Dict, Any, Iterable
from sqlalchemy import create_engine


class CFDataHandler:
    """handles the data to feed into class CFs
    Examples:
        >>> config = read_config('/path/to/config.json')
        >>> cf_raw_data = CFDataHandler(config).load_cfs()
        >>> cfs = CFs(config, cf_raw_data)
    """

    def __init__(self, config: AttributeDict):
        self.config = config

    def load_cfs(
        self,
    ) -> pd.DataFrame:
        """main method to load cfs data"""
        scenario_df = self._load_scenario_file()
        cashflow_df = self._load_cashflow_file()

        df = cashflow_df.join(scenario_df).reset_index()

        return df

    def _load_scenario_file(
        self,
    ) -> pd.DataFrame:
        """load scenario file"""
        scn_number = self.config.scenario_number_col
        proj_mth = self.config.projection_month_col
        disc = self.config.discount_col
        df = pd.DataFrame()

        if self.config.source == "sql":
            server = self.config.inforce_server
            db = self.config.inforce_db
            tbl = self.config.scenario_file
            sql = f"SELECT [{scn_number}], [{proj_mth}], [{disc}]  FROM {tbl}"
            connection_str = f"mssql+pyodbc://{server}:{db}"
            conn = create_engine(connection_str)

            df = pd.read_sql_query(sql, conn)
            # TODO: scenarios may miss t=0 values
            df = df.set_index([scn_number, proj_mth], inplace=False)

        elif self.config.source == "csv":
            # TODO: depracated
            data_file = self.config.scenario_file
            df = df.concat(df, pd.read_csv(data_file))
            df = df[[scn_number, proj_mth, disc]].copy()
            df = df.set_index([scn_number, proj_mth], inplace=False)
        else:
            logger.error("Unsupported source for scenario file!")

        return df

    def _load_cashflow_file(
        self,
    ) -> pd.DataFrame:
        """load cf projections"""
        scn_number = self.config.scenario_number_col
        proj_mth = self.config.projection_month_col
        slice = self.config.slice
        df = pd.DataFrame()

        if self.config.source == "sql":
            server = self.config.results_server
            db = self.config.results_db
            tbl = self.config.cashflow_file
            sql = f"SELECT * FROM {tbl}"
            if slice:
                sql += f" WHERE {self._create_slicing_condition(slice)}"
            connection_str = f"mssql+pyodbc://{server}:{db}"
            conn = create_engine(connection_str)
            df = pd.read_sql_query(sql, conn)
            df = df.set_index([scn_number, proj_mth])

        elif self.config.source == "csv":
            data_file = self.config.cashflow_file
            df = df.concat(pd.read_csv(data_file, index_col=False))
            df = df.set_index([scn_number, proj_mth])
        else:
            logger.error("Unsupported source for scenario file!")
            df = pd.DataFrame()

        return df

    @staticmethod
    def _create_slicing_condition(slice: dict[str, Any]) -> str:
        """based on the slice dictionary, create a condition string
        Args:
            slice: dictionary defining slicing conditions
        Returns:
            str: condition string that can be used in the sql query
        Examples:
            >>> slicing = {'name': 'Alex', 'age', 15}
            >>> _create_slicing_condition(slicing)
            >>> [name] = 'Alex' AND [age] = 15
        """
        conditions = []
        for key, val in slice.items():
            if type(val) == str:
                val = f"'{val}'"
            conditions.append(f"[{key}] = {val}")

        return " AND ".join(conditions)


class CFs:
    """stochastic cashflow class
    Examples:
        >>> cfs = CFs(params, cf_raw_data)
        >>> cfs.group_cfs(params.cfs_groupings.keys()) #Groups cashflows
        >>> cfs.calc_ess_cfs() #calculate ess cashflows
        >>> cfs.calc_bels(params.bel_definitions) #calculate BEls
    """

    _COL_PROJ_MTH = "t"
    _COL_DISCOUNT_FACTOR = "discount_factor"
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
        self.grouped_cfs = []
        self._rename_df_columns()
        self._clean_params()

    def _clean_params(
        self,
    ) -> None:
        """clean up the params entries from user\'s config"""
        # standardize cfs
        for idx, cf_name in enumerate(self.params.cfs):
            self.params.cfs[idx] = self._standardize_column_name(cf_name)

        # common filters
        self.filter_is_stochastic = (self.df[self._COL_SCENARIO_NUMBER] > 0) & (
            self.df[self._COL_SCENARIO_NUMBER] <= 1000
        )

    def _rename_df_columns(self) -> None:
        """rename df columns"""
        # rename must-have columns
        standard_columns_rename = {
            self.params.projection_month_col: self._COL_PROJ_MTH,
            # "discount factor - risk free rate": "disc",
            self.params.discount_col: self._COL_DISCOUNT_FACTOR,
            self.params.scenario_number_col: self._COL_SCENARIO_NUMBER,
        }

        self.df.rename(columns=standard_columns_rename, inplace=True)

        # make all column names lower case letter connected with underscores
        self.df.rename(mapper=self._standardize_column_name, axis=1, inplace=True)

        return

    def _calc_equivalent_weights(
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

        # calculate weights
        self._calc_equivalent_weights()
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

    def calc_grouped_cfs(self, groupings: Iterable[str]) -> None:
        """group cashflows use the grouping defined in the params
        Args:
            groupings (list): list of groupings to add to the dataframe
        Returns:
            None: new columns are added to self.df. new columns are named as <grouping_def>.<group_name>
        """
        for grouping_def in groupings:
            # check if it is defined in params
            if grouping_def not in self.params.cfs_groupings:
                logger.warn(f"{grouping_def} is not defined in params: Ignored")
                continue

            for local_group_name, group_def in self.params.cfs_groupings[
                grouping_def
            ].items():
                global_group_name = f"{grouping_def}.{local_group_name}"
                self._add_cfs_group(global_group_name, group_def)

        return

    def _add_cfs_group(self, group_name: str, group_def: Dict[str, int]) -> None:
        """add an aggregate of cashflows to self.df
        Args:
            group_name (str): the suffix added to the new cashflow group name
            group_def (dict): definition of the cashflow group. Key is cashflow name. Value is sign.
        Returns:
            None: A new columns is added to self.df, named as group_name.
        """

        self.df[group_name] = 0

        for cf, sign in group_def.items():
            self.df[group_name] += self.df[cf] * sign

        # update grouped cfs
        self.grouped_cfs.append(group_name)

        return

    def calc_bels(self, bel_definitions: Iterable[str]) -> pd.DataFrame:
        """Calculate Bels in each scenario
        Args:
            bel_definitions (list): list of bel definitions
        Returns:
            pd.DataFrame: dataframe (index is scenario, column is bel defintions)
        """
        pvs = self._calc_pvs()
        for bel_def_name in bel_definitions:
            # loop through all bel_definitions
            if bel_def_name not in self.params.bel_definitions:
                # bel definition not in params, do nothing
                logger.warn(f"{bel_def_name} is not defined in self.params")
                continue

            bel_def = self.params.bel_definitions[bel_def_name]
            bel_colum = f"{bel_def_name}.bel"
            pvs[bel_colum] = 0
            for cf, sign in bel_def.items():
                # loop through all cashflow components in current bel defintion
                # TODO move this part to a separate function
                cf_col = f"{bel_def_name}.{cf}"
                pvs[bel_colum] += pvs[cf_col] * sign

        return pvs

    @staticmethod
    def _standardize_column_name(raw_name: str) -> str:
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


def process_cfs(params: AttributeDict):
    """main function for this app"""
    # df = CFDataHandler(params).load_cfs()
    df = pd.DataFrame(data=pd.read_csv("inputs/ifrs17_cfs.csv"))
    cfs = CFs(params, df)
    cfs_groupings = ["fire_gmm"]
    bel_definitions = ["fire_gmm", "kc4"]
    # cfs_groupings = params.cfs_groupings.keys()
    # bel_definitions = params.bel_definitions.keys()
    cfs.calc_grouped_cfs(groupings=cfs_groupings)
    cfs.calc_ess_cfs()
    res = cfs.calc_bels(bel_definitions=bel_definitions)
    print(res)
    print(cfs.df)
