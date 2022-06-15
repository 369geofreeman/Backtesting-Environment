import time
import pprint
import requests
import numpy as np

from itertools import combinations
from typing import List

import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

from binance.client import Client

from database import Hdf5Client
from utils import *


class Tests:
    def __init__(self, base_asset: str, quote_asset: str, exchange: str,
                 timeframe, from_time: str, to_time: str, all_coins: List[str]):

        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.exchange = exchange
        self.tf = timeframe
        self.from_time = from_time
        self.to_time = to_time
        self.all_coins = all_coins

    def get_data(self, coin=None):

        if not coin:
            coin = self.base_asset

        try:
            ft = int(datetime.datetime.strptime(
                self.from_time, "%d/%m/%Y").timestamp() * 1000)

            if self.to_time == "":
                tt = int(datetime.datetime.now().timestamp() * 1000)
            else:
                tt = int(datetime.datetime.strptime(
                    self.to_time, "%d/%m/%Y").timestamp() * 1000)

        except ValueError:
            print("Error with time entered.")

        h5_db = Hdf5Client(self.exchange)
        data = h5_db.get_data(coin, from_time=ft, to_time=tt)
        data = resample_timeframe(data, self.tf)

        return data

    def get_all_coins(self):
        all_usdt_pairs = []

        if self.exchange == "binance":

            client = Client()

            for coin in client.get_all_tickers():

                if "UP" in coin["symbol"] or "DOWN" in coin["symbol"] or \
                        "BULL" in coin["symbol"] or "BEAR" in coin["symbol"]:
                    continue
                elif "USDT" in coin['symbol']:
                    all_usdt_pairs.append(coin['symbol'].replace('USDT', ''))

        elif self == "FTX":

            base_url = "https://ftx.com/api"
            endpoint = "/markets"

            try:
                response = requests.get(base_url + endpoint)
                response = response.json()

                for coin in response["result"]:

                    if "-PERP" in coin['name']:
                        all_usdt_pairs.append(
                            coin['name'].replace('-PERP', ''))

            except Exception as e:
                print(
                    f"Connection error {e} while making GET request to {endpoint}")

        elif self.exchange == "kucoin":

            base_url = "https://api.kucoin.com"
            endpoint = "/api/v1/market/allTickers"

            try:
                response = requests.get(base_url + endpoint)
                response = response.json()

                for coin in response['data']['ticker']:

                    if "UP" in coin['symbolName'] or "DOWN" in coin['symbolName']:
                        continue

                    elif "USDT" in coin['symbolName']:
                        all_usdt_pairs.append(
                            coin['symbolName'].replace('-USDT', ''))

            except Exception as e:
                print(
                    f"Connection error {e} while making GET request to {endpoint}")
                return None

        return sorted(all_usdt_pairs)

    def cointegration(self, df: pd.DataFrame):
        model = sm.OLS(df.Y.iloc[:90], df.X.iloc[:90])
        model = model.fit()

        df['spread'] = df.Y - model.params[0] * df.X

        # ADF
        statsmodel_adf = adfuller(df.spread, maxlag=1)
        p_value, critical_values = statsmodel_adf[0], statsmodel_adf[4]

        if p_value < critical_values["1%"]:
            return 99
        elif p_value < critical_values["5%"]:
            return 95
        else:
            return 0

    def coint_analysis(self):

        print("\n=> Getting combinations..")
        coin_combos = list(combinations(self.all_coins, 2))
        print(f"\nTotal combinations => {len(coin_combos)}\n")
        winners = []
        losers = []

        for idx, pair in enumerate(coin_combos):
            c1 = self.get_data(pair[0])
            c2 = self.get_data(pair[1])
            c1 = c1["close"]
            c2 = c2["close"]

            data = pd.concat([c1, c2], axis=1)
            data.columns = ['Y', 'X']
            data = data.apply(pd.to_numeric)

            tst = self.cointegration(data)

            if tst != 0:
                winners.append(
                    {"coins": [pair[0], pair[1]], "confidence": f"{tst}%"})
            else:
                losers.append(
                    {"coins": [pair[0], pair[1]], "confidence": f"{tst}%"})

            print(f"\rPair {idx+1} of {len(coin_combos)} complete", end="")

        return winners, losers


# all_coins = ["BNBUSDT", "DOTUSDT", "ENJUSDT", "LTCUSDT", ]    # 4 pairs / 6 combinations
all_coins = ["BNBUSDT", "DOTUSDT", "ENJUSDT", "LTCUSDT",
             "VETUSDT", "FTMUSDT", "XRPUSDT", "AXSUSDT",
             "XEMUSDT", "TVKBUSD", "ANTUSDT", "GHSTBUSD",
             "IOTAUSDT", "LINKUSDT", "SANDUSDT", "ROSEUSDT",
             "QUICKUSDT", "BTCUSDT", "ETHUSDT", "BadgerUSDT",
             "BANDUSDT"]   # 21 pairs / 210 combinations

base_asset = "BTCUSDT"
quote_asset = "ETHUSDT"
exchange = "binance"
timeframe = "15m"
from_time = "05/12/2021"      # (dd/mm/yyyy)
to_time = "01/01/2022"        # (dd/mm/yyyy)

tests = Tests(base_asset, quote_asset, exchange,
              timeframe, from_time, to_time, all_coins)

# Get all coins test
# ---
# pprint.pprint(tests.get_all_coins())

# Get data tests
# ---
# x = tests.get_data()
# print(x)
# x = x["close"]
# print(x)
# print(type(x))

# Cointegration tests
# ---
winners, losers = tests.coint_analysis()

print("\n ---- WINNERS ---\n")
[print(w) for w in winners]

print("\n ---- LOSERS ----\n")
[print(l) for l in losers]


# test for multiple comparisons problem
