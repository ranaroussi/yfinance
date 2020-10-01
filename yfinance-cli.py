# Command line interface for yfinance module
# Contributed by @animesh-srivastava at GitHub
# This file should also work if you have yfinance module installed in your
# python environment

from argparse import ArgumentParser
from yfinance import Ticker
import os
from datetime import date, timedelta, datetime


def get_data(ticker, start, end):
    cwd = os.getcwd()
    print(f"Date range is {start} to {end}\n")
    if not os.path.isdir(os.path.join(cwd, "yfinance-data")):
        os.mkdir(os.path.join(cwd, "yfinance-data"))
    if not os.path.isdir(os.path.join(cwd, "yfinance-data", ticker)):
        os.mkdir(os.path.join(cwd, "yfinance-data", ticker))
    t = Ticker(ticker)
    try:
        data1 = t.history(
            interval="1d",
            start=start,
            end=end,
            actions=False)

        if data1.shape[0] != 0:
            path = os.path.join(cwd, f"yfinance-data/{ticker}/{ticker}_DAILY_{start}_{end}.csv")
            data1.to_csv(path)
            print(f'Saved to {path}\n')

        data2 = t.history(
            interval="1wk",
            start=start,
            end=end,
            actions=False)

        if data2.shape[0] != 0:
            path = os.path.join(cwd, f"yfinance-data/{ticker}/{ticker}_WEEKLY_{start}_{end}.csv")
            data2.to_csv(path)
            print(f'Saved to {path}\n')

        data3 = t.history(
            interval="1mo",
            start=start,
            end=end,
            actions=False)

        if data3.shape[0] != 0:
            path = os.path.join(cwd, f"yfinance-data/{ticker}/{ticker}_MONTHLY_{start}_{end}.csv")
            data3.to_csv(path)
            print(f'Saved to {path}\n')

        return 0
    except Exception as e:
        print(e)


def Main():
    parser = argparse.ArgumentParser(
        prog='A command line wrapper around yfinance module')
    parser.add_argument(
        "-t",
        "--ticker",
        required=True,
        help="The company symbol or ticker.",
        type=str)
    parser.add_argument(
        "-p",
        "--period",
        help="The period for which data is required. \
        Following methods work - \
        3d for three days, \
        2w for two weeks, \
        3m for three months and \
        5y for five years.",
        type=str)
    parser.add_argument(
        "-s",
        "--start",
        help="Specify the starting date in YYYY-MM-DD.",
        action='store')
    parser.add_argument(
        "-e",
        "--end",
        help="Specify the ending date in YYYY-MM-DD.",
        action='store')
    args = parser.parse_args()
    if args.period and not(args.start and args.end):
        print(f"\nTime period specified is {args.period}\n")
        try:

            if args.period[-1] == 'y':
                get_data(args.ticker, start=str(
                    date.today() - timedelta(days=int(args.period[:-1]) * 365)), end=date.today())
            elif args.period[-1] == 'm':
                get_data(args.ticker, start=str(
                    date.today() - timedelta(days=int(args.period[:-1]) * 30)), end=date.today())
            elif args.period[-1] == 'w':
                get_data(args.ticker, start=str(
                    date.today() - timedelta(days=int(args.period[:-1]) * 7)), end=date.today())
            elif args.period[-1] == 'd':
                get_data(args.ticker, start=str(
                    date.today() - timedelta(days=int(args.period[:-1]))), end=date.today())
        except ValueError as v:
            print("Error - ", v)
    elif args.start and args.end and not(args.period):
        print(f"Start Date : {args.start} End Date: {args.end}\n")
        if (args.start) <= (args.end):
            get_data(args.ticker, start=args.start, end=args.end)
        else:
            raise ValueError(
                "Ending date cannot be before than starting date.")
    elif args.start and not args.end:
        print(f"Taking {date.today()} as ending date\n")
        get_data(args.ticker, start=args.start, end=date.today())

    elif args.end and not args.start:
        start = datetime.strptime(args.end, "%Y-%m-%d").date() - timedelta(365)
        print(f"Taking {start} as starting date\n")
        get_data(args.ticker, start=start, end=args.end)

    elif args.start and args.end and args.period:
        print("Default precedence is Date Range\n")
        if (args.start) <= (args.end):
            get_data(args.ticker, start=args.start, end=args.end)
        else:
            print("Ending date cannot be before than starting date.")
    elif not(args.start) and not(args.period) and not(args.end):
        end = date.today()
        start = (end - timedelta(365))
        print(f"Proceeding with default values - {start} to {end}")
        get_data(args.ticker, start=start, end=end)

if __name__ == "__main__":
    Main()
