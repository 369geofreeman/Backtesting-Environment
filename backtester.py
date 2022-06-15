from ctypes import *

from database import Hdf5Client

from utils import resample_timeframe, STRAT_PARAMS, get_library
import strategies.obv
import strategies.skylab
import strategies.ichimoku
import strategies.support_resistance


def run(exchange: str, symbol: str, strategy: str, tf: str, from_time: int, to_time: int):

    params_des = STRAT_PARAMS[strategy]

    params = dict()

    for p_code, p in params_des.items():
        while True:
            try:
                if p_code == "base_asset":
                    while True:
                        params[p_code] = p["type"](
                            input(p["name"] + ": ")).upper()
                        if params[p_code] not in ["ETHUSDT", "BTCUSDT"]:
                            continue
                        else:
                            break
                    break
                else:
                    params[p_code] = p["type"](input(p["name"] + ": "))
                    break
            except ValueError:
                continue
# -- OBV

    if strategy == "obv":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.obv.backtest(
            data, ma_period=params["ma_period"])

        return pnl, max_drawdown

# -- SKYLAB

    elif strategy == "skylab":
        h5_db = Hdf5Client(exchange)
        data_quote = h5_db.get_data(symbol, from_time, to_time)
        data_quote = resample_timeframe(data_quote, tf)

        try:
            base_symbol = params["base_asset"]
            data_base = h5_db.get_data(base_symbol, from_time, to_time)
            data_base = resample_timeframe(data_base, tf)
            data_base = data_base[str(
                data_quote.index[0]):str(data_quote.index[-1])]
            trading_pair = [base_symbol.replace(
                "USDT", ""), symbol.replace("USDT", "")]

        except Exception as e:
            return "It went wrong", e

        pnl, max_drawdown = strategies.skylab.backtest(data_base,
                                                       data_quote,
                                                       trading_pair,
                                                       std_dev=params["std_dev"],
                                                       std_dev_multi=params["std_dev_multi"],
                                                       backtest=True)

        return pnl, max_drawdown

# -- Ichimoku

    elif strategy == "ichimoku":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.ichimoku.backtest(
            data, tenkan_period=params["tenkan"], kijun_period=params["kijun"])

        return pnl, max_drawdown

    elif strategy == "sup_res":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.support_resistance.backtest(data, min_points=params["min_points"],
                                                                   min_diff_points=params["min_diff_points"],
                                                                   rounding_nb=params["rounding_nb"],
                                                                   take_profit=params["take_profit"], stop_loss=params["stop_loss"])

        return pnl, max_drawdown

# -- Simple Moving Average

    elif strategy == "sma":

        lib = get_library()

        obj = lib.Sma_new(exchange.encode(), symbol.encode(),
                          tf.encode(), from_time, to_time)
        lib.Sma_execute_backtest(obj, params["slow_ma"], params["fast_ma"])
        pnl = lib.Sma_get_pnl(obj)
        max_drawdown = lib.Sma_get_max_dd(obj)

        return pnl, max_drawdown

# -- PSAR

    elif strategy == "psar":

        lib = get_library()

        obj = lib.Psar_new(exchange.encode(), symbol.encode(),
                           tf.encode(), from_time, to_time)
        lib.Psar_execute_backtest(
            obj, params["initial_acc"], params["acc_increment"], params["max_acc"])
        pnl = lib.Psar_get_pnl(obj)
        max_drawdown = lib.Psar_get_max_dd(obj)

        return pnl, max_drawdown
