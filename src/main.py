#! /usr/bin/env python3

# from gooey import Gooey, GooeyParser
from utils.utils import logger, read_config
from utils.pv_calcs import calc_pv
from utils.fmt import map_with_wildcard
import pandas as pd


# @Gooey(program_name='sample program')
# def main():

#     logger.info('application started')
#     main_parser = GooeyParser()
#     main_parser.add_argument(
#         'command', help='call a command', type=str, default='gui', nargs='?')
#     main_parser.add_argument('--config', help='config file for command')
#     main_args = main_parser.parse_args()

#     if main_args.command == 'gui':
#         print('launching gui program')
#     elif main_args.command == 'app1':
#         print(f'running app1 with config {main_args.config}')
#     else:
#         print(f'running app2 with config {main_args.config}')


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


if __name__ == "__main__":
    # test_fmt_mapping()
    test_io()
