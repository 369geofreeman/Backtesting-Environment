#include <string>
#include <vector>

class Psar
{
public:
    Psar(char *exchange_c, char *symbol_c, char *timeframe_c, long long from_time, long long to_time);
    void execute_backtest(double initial_acc, double acc_increment, double max_acc);

    std::string exchange;
    std::string symbol;
    std::string timeframe;

    std::vector<double> ts, open, high, low, close, volume;

    double pnl = 0;
    double max_dd = 0;
};