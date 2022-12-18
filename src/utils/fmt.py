#! usr/bin/env python3
import re
import pandas as pd

"""table driven mapping with wildcard using regex
"""


def merge_df_by_regex(
    main_df: pd.DataFrame, regex_df: pd.DataFrame, key_col: str, regex_col: str
) -> pd.DataFrame:
    """Merge two dataframes using regex match
    Args:
        main_df (pd.DataFrame): main dataframe that contains the look-up keys
        regex_df (pd.DataFrame): lookup dataframe that contains the look-up keys in regex format
        lookup_col (str): column to lookup in main df
        regex_col (str): regex pattern column in the regex df
    Returns:
        None. Main df is updated if a match was found
    """
    # get the index from both dataframe that have the key-pattern match
    indices = [
        (regex_idx, main_idx)
        for regex_idx, pattern in enumerate(regex_df[regex_col])
        for main_idx, key in enumerate(main_df[key_col])
        if re.match(pattern, key)
    ]
    regex_idx, main_idx = zip(*indices)

    regex_t = regex_df.iloc[list(regex_idx), :].reset_index(drop=True)
    main_t = main_df.iloc[list(main_idx), :].reset_index(drop=True)

    merged_t = pd.concat([main_t, regex_t], axis=1)

    return merged_t


def map_with_wildcard(
    cells_table: pd.DataFrame,
    fmt_table: pd.DataFrame,
) -> pd.DataFrame:
    """map cells table using fmt table
    Args:
        cells_table (pd.DataFrame): cell table that has all tags
        fmt_table (pd.DataFrame): fmt table that has the relavent tag combos and mapped assumptions
        _sep (str): separators to join the different tags.
    Returns:
        A new cells dataframe that has assumptions mapped from the fmt table
    """
    # symbol used to separate different tags when lookup
    sep = "~"
    # will create a lookup column in cells table and a regex column in fmt table
    key_col_name = "key_col"
    regex_col_name = "regex_col"

    # all tag fields start with "tag_"

    fmt_tag_columns = list(
        filter(lambda col: col.startswith("tag_"), fmt_table.columns)
    )
    fmt_assumption_columns = list(
        filter(lambda col: not col.startswith("tag_"), fmt_table.columns)
    )

    # construct lookup keys in cells table
    cells_table[key_col_name] = cells_table.apply(
        lambda record: sep.join([str(record[col]) for col in fmt_tag_columns]), axis=1
    )

    # construct regex keys in fmt table
    fmt_table[regex_col_name] = fmt_table.apply(
        lambda record: "^"
        + sep.join([str(record[col]) for col in fmt_tag_columns])
        + "$",
        axis=1,
    )

    # prepare the two tables to merge
    cells_table_to_merge = cells_table.loc[
        :, ~cells_table.columns.isin(fmt_assumption_columns)
    ]
    fmt_table_to_merge = fmt_table.loc[:, fmt_assumption_columns + [regex_col_name]]

    merged_df = merge_df_by_regex(
        cells_table_to_merge,
        fmt_table_to_merge,
        key_col=key_col_name,
        regex_col=regex_col_name,
    )

    merged_df.drop(columns=[key_col_name, regex_col_name], inplace=True)

    return merged_df
