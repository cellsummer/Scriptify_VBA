#! /usr/bin/env python3

# from gooey import Gooey, GooeyParser
from argparse import ArgumentParser
from run import run_programs


def construct_parser():
    """main program with subcommand"""
    parser = ArgumentParser()
    # Add a subcommand
    subparsers = parser.add_subparsers(title="subcommands", dest="subcommand")

    # Create the parser for the cfs subcommand
    parser_cfs = subparsers.add_parser(
        "cfs", help="calculate pvs for stochastic cashflows"
    )
    # Add a required argument to the "cfs" subcommand
    parser_cfs.add_argument(
        "--config", help="cfs parameters in josn format", required=True
    )

    # Create the parser for the "b" subcommand
    parser_fmt = subparsers.add_parser("fmt", help="construct fmt tables")
    # Add a required argument to the "b" subcommand
    parser_fmt.add_argument("--config", help="optional config for fmt")

    # Parse the arguments
    args = parser.parse_args()

    return args


# @Gooey(program_name='sample program')
def main_gui():
    args = construct_parser()
    run_programs(args.subcommand, args.config)


def main():
    args = construct_parser()
    run_programs(args.subcommand, args.config)


if __name__ == "__main__":
    main()
