import datetime
from ctypes import *

import pandas as pd


TF_EQUIV = {"1m": "1Min", "5m": "5Min", "15m": "15Min", "30m": "30Min",
            "1h": "1H", "4h": "4H", "12h": "12H", "1d": "D"}

STRAT_PARAMS = {
    "obv": {
        "ma_period": {"name": "MA Period", "type": int, "min": 2, "max": 200},
    },
    "skylab": {
        "base_asset": {"name": "Choose Base Asset [BTC / ETH]", "type": str},
        "std_dev": {"name": "Standard Deviation Length", "type": int, "min": 5, "max": 50},
        "std_dev_multi": {"name": "Standard Deviation Multiplier", "type": int, "min": 1, "max": 5},
    },
    "coint": {
        "base_asset": {"name": "Choose Quote Asset", "type": str},
        "test_type": {"name": "Test type [Johansen / Engle-Granger]", "type": str},
        "std_dev_multi": {"name": "Standard Deviation Multiplier", "type": int, "min": 1, "max": 5},
    },
    "ichimoku": {
        "kijun": {"name": "Kijun Period", "type": int, "min": 2, "max": 200},
        "tenkan": {"name": "Tenkan Period", "type": int, "min": 2, "max": 200},
    },
    "sup_res": {
        "min_points": {"name": "Min. Points", "type": int, "min": 2, "max": 20},
        "min_diff_points": {"name": "Min. Difference between Points", "type": int, "min": 2, "max": 100},
        "rounding_nb": {"name": "Rounding Number", "type": float, "min": 10, "max": 500, "decimals": 2},
        "take_profit": {"name": "Take Profit %", "type": float, "min": 1, "max": 40, "decimals": 2},
        "stop_loss": {"name": "Stop Loss %", "type": float, "min": 1, "max": 40, "decimals": 2},
    },
    "sma": {
        "slow_ma": {"name": "Slow MA Period", "type": int, "min": 2, "max": 200},
        "fast_ma": {"name": "Fast MA Period", "type": int, "min": 2, "max": 200},
    },
    "psar": {
        "initial_acc": {"name": "Initial Acceleration", "type": float, "min": 0.01, "max": 0.02, "decimals": 2},
        "acc_increment": {"name": "Acceleration Increment", "type": float, "min": 0.01, "max": 0.3, "decimals": 2},
        "max_acc": {"name": "MAx Acceleration", "type": float, "min": 0.05, "max": 1, "decimals": 2},
    },
}


def ms_to_dt(ms: int) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(ms / 1000)


def resample_timeframe(data: pd.DataFrame, tf: str) -> pd.DataFrame:
    return data.resample(TF_EQUIV[tf]).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )


def get_library():
    lib = CDLL("backtestingCpp/build/libbacktesting.dylib")

    # SMA

    lib.Sma_new.restype = c_void_p
    lib.Sma_new.argtypes = [c_char_p, c_char_p, c_char_p, c_longlong, c_longlong]
    lib.Sma_execute_backtest.restype = c_void_p
    lib.Sma_execute_backtest.argtypes = [c_void_p, c_int, c_int]

    lib.Sma_get_pnl.restype = c_double
    lib.Sma_get_pnl.argtypes = [c_void_p]
    lib.Sma_get_max_dd.restype = c_double
    lib.Sma_get_max_dd.argtypes = [c_void_p]

    # Psar

    lib.Psar_new.restype = c_void_p
    lib.Psar_new.argtypes = [c_char_p, c_char_p, c_char_p, c_longlong, c_longlong]
    lib.Psar_execute_backtest.restype = c_void_p
    lib.Psar_execute_backtest.argtypes = [c_void_p, c_double, c_double, c_double]

    lib.Psar_get_pnl.restype = c_double
    lib.Psar_get_pnl.argtypes = [c_void_p]
    lib.Psar_get_max_dd.restype = c_double
    lib.Psar_get_max_dd.argtypes = [c_void_p]

    return lib






