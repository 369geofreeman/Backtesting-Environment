import pandas as pd
import typing


def updateData(c1: pd.DataFrame, c2: pd.DataFrame, std_dev: int) -> pd.DataFrame:
    """
    Builds self.df
    -------------
    Columns
    ---
    c1_data:    Coin 1 close data
    c2_data:    Coin 2 close data
    range:      Distance between coin_1 and coin_2 close,
    mean:       Mean of ranges,
    std_dev:    Standard deviation of ranges
    """

    df = pd.DataFrame()

    df["c1_close"] = c1["close"]
    df['c2_close'] = c2['close']
    df['range'] = df['c1_close'] - df['c2_close']
    df['mean'] = df['range'].rolling(window=std_dev).mean()
    df['std_dev'] = df['mean'].rolling(window=std_dev).std()

    df['swap'] = '' * len(df)
    df['balance'] = 0 * len(df)
    df["PNL"] = 0 * len(df)
    df = df.reset_index(drop=True)

    return df


def long(data: pd.DataFrame, idx: int, std_dev_multi: int):
    '''
    Checks to see if we should switch trade to other coin
    -----------------------------------------------------
    Params
    ---
    :data: dataframe - subset of dataframe to calculate long entry
    :idx:  int       - Current row index of dataframe
    '''

    if data.loc[idx, 'range'] > data.loc[idx, 'mean'] + (data.loc[idx, 'std_dev'] * std_dev_multi):
        return 'coin1'

    elif data.loc[idx, 'range'] < data.loc[idx, 'mean'] - (data.loc[idx, 'std_dev'] * std_dev_multi):
        return 'coin2'

    return False


def backtest(df_base: pd.DataFrame,
             df_quote: pd.DataFrame,
             trading_pair: typing.List[str],
             std_dev: int,
             std_dev_multi: int,
             backtest: bool):

    data = updateData(df_base, df_quote, std_dev)

    c1 = df_base
    c2 = df_quote

    balance = 1000
    market_entry = data.loc[300, 'c1_close']
    coin_holdings = balance / c1.loc[c1.index[300], 'close']
    curr_coin = 'coin1'
    highest_balance = 0
    success_trades = 0
    coin1, coin2 = trading_pair

    for idx, row in data.iterrows():

        if idx > data.index[0] + 300 and idx < data.index[-1]:

            # Get data chunks
            data_chunk = data.loc[idx-300:idx]
            open_long = long(data_chunk, idx, std_dev_multi)

            # Coin 2 trade
            if open_long == 'coin2' and curr_coin == 'coin1':

                curr_coin = 'coin2'
                balance += (coin_holdings * (data.loc[idx, 'c1_close'] - market_entry))
                market_entry = data.loc[idx, 'c2_close']
                coin_holdings = balance / data.loc[idx, 'c2_close']
                success_trades += 1
                data.loc[idx, 'swap'] = 'yes'

            # Coin 1 trade
            elif open_long == 'coin1' and curr_coin == 'coin2':

                curr_coin = 'coin1'
                balance += (coin_holdings * (data.loc[idx, 'c2_close'] - market_entry))
                market_entry = data.loc[idx, 'c1_close']
                coin_holdings = balance / data.loc[idx, 'c1_close']
                success_trades += 1
                data.loc[idx, 'swap'] = 'yes'

            else:
                data.loc[idx, 'swap'] = 'no'

            if balance > highest_balance:
                highest_balance = balance

            data.loc[idx, 'balance'] = balance
            data.loc[idx, 'PNL'] = balance - data.loc[idx - 1, 'balance']

        else:
            data.loc[idx, 'balance'] = balance
            data.loc[idx, 'swap'] = 'no'

    # Calculate max DrawDown
    data["cum_pnl"] = data["PNL"].cumsum()
    data["max_cum_pnl"] = data["cum_pnl"].cummax()
    data["drawdown"] = data["max_cum_pnl"] - data["cum_pnl"]

    data.drop(["cum_pnl", "max_cum_pnl"], axis=1, inplace=True)

    if backtest:

        # Print main backtest results

        print('\nTrading pair: {} - {}\n'.format(coin1, coin2))
        print("Total candles: {}".format(len(data)))
        print('Total trades: {}'.format(success_trades))
        print('\nStarting balance $1000')
        print('Maximum balance achieved: ${}'.format(round(highest_balance)))
        print('Percentage change: {}%'.format(round((balance/1000) * 100, 2)))
        print('---\nFinal balance: ${}'.format(round(balance, 2)))
        print('-------\n')

        # Print coin 1 info

        c1_hold = 1000 / c1.loc[c1.index[0], 'close']
        c1_entry = c1.loc[c1.index[0], 'close']
        c1_bal = 1000 + (c1_hold * (c1.loc[c1.index[-1], 'close'] - c1_entry))
        c1_starting_price = c1.loc[c1.index[0], 'close']
        c1_ending_price = c1.loc[c1.index[-1], 'close']
        print('{} starting price: ${}'.format(coin1, c1_starting_price))
        print('{} ending price: ${}'.format(coin1, c1_ending_price))
        print('Balance after holding {} without trading: ${}'.format(coin1, round(c1_bal, 2)))

        # Print coin 2 info

        c2_hold = 1000 / c2.loc[c2.index[0], 'close']
        c2_entry = c2.loc[c2.index[0], 'close']
        c2_bal = 1000 + (c2_hold * (c2.loc[c2.index[-1], 'close'] - c2_entry))
        c2_starting_price = c2.loc[c2.index[0], 'close']
        c2_ending_price = c2.loc[c2.index[-1], 'close']
        print('\n{} starting price: ${}'.format(coin2, c2_starting_price))
        print('{} ending price: ${}'.format(coin2, c2_ending_price))
        print('Balance after holding {} without trading: ${}'.format(coin2, round(c2_bal, 2)))
        print('------\n')

        if c1_bal < balance and c2_bal < balance:
            print('---\nSUCCESS\nThe bot outraded both {} amd {}!\n---\n'.format(coin1, coin2))

        elif c1_bal > balance and c2_bal > balance:
            print('{}-{} pair was out traded by holding both {} and {}'.format(coin1, coin2, coin1, coin2))

        elif c1_bal > balance:
            print('{}-{} pair was out traded by holding {}'.format(coin1, coin2, coin1))

        else:
            print('{}-{} pair was out traded by holding {}'.format(coin1, coin2, coin2))

    return data['PNL'].sum(), data["drawdown"].max()






