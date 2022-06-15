#include <string>
#include <tuple>
#include <vector>

std::tuple<std::vector<double>, std::vector<double>, std::vector<double>,
           std::vector<double>, std::vector<double>, std::vector<double>>
rearrange_candles(double **candles, std::string tf, long long from_time, long long to_time, int array_size);
