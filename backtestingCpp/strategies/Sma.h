#include <string>
#include <vector>

class Sma
{
public:
    Sma(char *exchange_c, char *symbol_c, char *timeframe_c, long long from_time, long long to_time);
    void execute_backtest(int slow_ma, int fast_ma);

    std::string exchange;
    std::string symbol;
    std::string timeframe;

    std::vector<double> ts, open, high, low, close, volume;

    double pnl = 0;
    double max_dd = 0;
};