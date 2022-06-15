#include <iostream>
#include <cstring>

#include "strategies/Psar.h"

int main(int, char **)
{
    std::string symbol = "BTCUSDT";
    std::string exchange = "binance";
    std::string timeframe = "1h";

    char *symbol_char = strcpy((char *)malloc(symbol.length() + 1), symbol.c_str());
    char *exchange_char = strcpy((char *)malloc(exchange.length() + 1), exchange.c_str());
    char *tf_char = strcpy((char *)malloc(timeframe.length() + 1), timeframe.c_str());

    Psar psar(exchange_char, symbol_char, tf_char, 0, 1630074127000);
    psar.execute_backtest(0.02, 0.02, 0.2);
    printf("%f | %f\n", psar.pnl, psar.max_dd);
}
