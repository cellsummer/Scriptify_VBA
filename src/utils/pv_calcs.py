#! /usr/bin/env python3
"""functions to calculate present values
"""

import pandas as pd
from utils.utils import logger, AttributeDict

# from typing import Dict


def calc_pv(df_cfs, discount_factors) -> AttributeDict:
    """calculate the present value of the cf_vector using the discount_factors vector
    Args:
        cfs (pd.DataFrame): Cashflows
        discount_factors (pd.Series): Discount factors

    Returns:
        dict: preseent values of cashflow streams

    """
    results = AttributeDict()

    for cf_col in df_cfs.columns:
        results[cf_col] = (df_cfs[cf_col] * discount_factors).sum()

    return results
