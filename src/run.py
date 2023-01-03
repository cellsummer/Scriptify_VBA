#! /usr/bin/env python3

# from gooey import Gooey, GooeyParser
from utils.utils import logger, read_config
from utils.pv_calcs import calc_pv
from utils.fmt import map_with_wildcard
import pandas as pd
from typing import Any
from apps.cfs import CFs


def run_programs(program_name: str, args: Any) -> None: 
    if program_name == 'fmt':
        logger.info("Start running fmt ...")
        test_fmt_mapping()
        logger.info("Finished running fmt.")
    elif program_name == 'cfs':
        logger.info("Start running cfs ...")
        test_cfs(args)
        logger.info("Finished running cfs.")
    else:
        logger.error(f"Incorrect command: {program_name} !")

    return

def test():
    logger.info(f"Running {__file__}")

    config = read_config("configs/my_config.json")
    logger.info("config file loaded")
    logger.info(config["server"])
    logger.info(config.db)
    logger.info(config["table"])

    logger.info(f"Finished running {__file__}")


def test_pv():
    data = {
        "t": [0, 1, 2, 3, 4, 5],
        "premiums": [1000, 0, 0, 0, 0, 0],
        "claims": [0, 100, 100, 100, 100, 200],
        "discount_factors": [1, 0.99, 0.95, 0.9, 0.8, 0.75],
    }
    df = pd.DataFrame(data=data)
    pvs = calc_pv(df[["premiums", "claims"]], df["discount_factors"])
    print(pvs)
    print(pvs.premiums)
    print(pvs["claims"])

    # print("Validation:")
    # pv = 0
    # for claim, discount_factor in zip(df.claims, df.discount_factors):
    #     pv += claim * discount_factor
    #     print(claim, discount_factor, claim * discount_factor, pv)


def test_fmt_mapping():
    # test data
    data_repeat = 100_000
    cell_tags_data = {
        "tag_plan_code": ["EI110", "EI125", "EI170"] * data_repeat,
        "tag_gender": ["M", "F", "M"] * data_repeat,
        "tag_risk_class": ["P", "s", "N"] * data_repeat,
        "table_premium": ["", "", ""] * data_repeat,
        "table_min_premium": ["", "", ""] * data_repeat,
    }

    premium_fmt_data = {
        "tag_plan_code": ["EI110"] * 6 + ["EI125"] + ["EI170"],
        "tag_gender": ["M"] * 3 + ["F"] * 3 + [".*"] * 2,
        "tag_risk_class": ["P", "S", "N"] * 2 + [".*"] * 2,
    }

    premium_fmt_data["table_premium"] = [
        f"Premiums_{p}_{g}_{r}"
        for p, g, r in zip(
            premium_fmt_data["tag_plan_code"],
            premium_fmt_data["tag_gender"],
            premium_fmt_data["tag_risk_class"],
        )
    ]

    premium_fmt_data["table_min_premium"] = [
        f"Min_Premiums_{p}_{g}_{r}"
        for p, g, r in zip(
            premium_fmt_data["tag_plan_code"],
            premium_fmt_data["tag_gender"],
            premium_fmt_data["tag_risk_class"],
        )
    ]

    cell_tags = pd.DataFrame(data=cell_tags_data)
    premium_fmt = pd.DataFrame(data=premium_fmt_data)

    # print(cell_tags.head())
    # print(premium_fmt)

    cells_df = map_with_wildcard(cells_table=cell_tags, fmt_table=premium_fmt)
    print(cells_df.tail())


def test_io():
    # read data from excel
    fmt_pf = pd.read_excel("temp/fmt.xlsx", sheet_name="reserves")
    print(fmt_pf)


def test_cfs(config: str):
    # config = "configs/cfs_config.json"
    params = read_config(config)
    df = pd.DataFrame(data=pd.read_csv("temp/ifrs17_cfs.csv"))
    cfs = CFs(params, df)
    cfs.group_cfs(groupings=["fire_gmm","kc4"])
    cfs.calc_equivalent_weights()
    cfs.calc_ess_cfs()
    res = cfs.calc_bels(bel_definitions=["fire_gmm","kc4"])
    print(cfs.df[cfs.df["t"] < 5].ess_weights)
    print(cfs.df[cfs.df["scenario_no"] == -1])
    # res = cfs.calc_pvs()
    print(res)
    print("result of ess: ", f'{res.loc[-1, "fire_gmm.bel"]:,.2f}')
    print(
        "result of stochastic scenarios:",
        f'{0.5 * (res.loc[1, "fire_gmm.bel"] + res.loc[2, "fire_gmm.bel"]): ,.2f}',
    )
    # print(cfs.df.columns)
    # print(cfs.df.head())
    # print(cfs.df.tail())
    # cfs.df.to_csv('temp/cfs.csv')


if __name__ == "__main__":
    logger.error("Please run thorugh main.py or main-gui.py")
    # test_fmt_mapping()
    # test_io()
