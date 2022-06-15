#include <math.h>

#include "Utils.h"

using namespace std;

tuple<vector<double>, vector<double>, vector<double>, vector<double>, vector<double>, vector<double>>
rearrange_candles(double **candles, string tf, long long from_time, long long to_time, int array_size)
{
    vector<double> ts, open, high, low, close, volume;
    double tf_ms;

    if (tf.find("m") != string::npos)
    {
        string minutes = tf.substr(0, tf.find("m"));
        tf_ms = stod(minutes) * 60.0 * 1000.0;
    }
    else if (tf.find("h") != string::npos)
    {
        string minutes = tf.substr(0, tf.find("h"));
        tf_ms = stod(minutes) * 60.0 * 60.0 * 1000.0;
    }
    else
    {
        printf("Parsing timeframe failed for %s\n", tf.c_str());
        return make_tuple(ts, open, high, low, close, volume);
    }

    double current_ts = 0.0;
    double current_o;
    double current_h;
    double current_l;
    double current_c;
    double current_v;

    for (int i = 0; i < array_size; i++)
    {

        if (candles[i][0] < from_time)
        {
            continue;
        }
        else if (current_ts == 0.0)
        {
            current_ts = candles[i][0] - fmod(candles[i][0], tf_ms);
            current_o = candles[i][1];
            current_h = candles[i][2];
            current_l = candles[i][3];
            current_c = candles[i][4];
            current_v = candles[i][5];
        }

        if (candles[i][0] > to_time)
        {
            break;
        }

        if (candles[i][0] >= current_ts + tf_ms)
        {
            ts.push_back(current_ts);
            open.push_back(current_o);
            high.push_back(current_h);
            low.push_back(current_l);
            close.push_back(current_c);
            volume.push_back(current_v);

            int missing_candles = (candles[i][0] - current_ts) / tf_ms - 1;

            if (missing_candles > 0)
            {
                printf("Missing %i candle(s) from %f\n", missing_candles, current_ts);

                for (int u = 0; u < missing_candles; u++)

                {
                    ts.push_back(current_ts + tf_ms * (u + 1));
                    open.push_back(current_c);
                    high.push_back(current_c);
                    low.push_back(current_c);
                    close.push_back(current_c);
                    volume.push_back(0);
                }
            }

            current_ts = candles[i][0] - fmod(candles[i][0], tf_ms);
            current_o = candles[i][1];
            current_h = candles[i][2];
            current_l = candles[i][3];
            current_c = candles[i][4];
            current_v = candles[i][5];
        }
        else
        {

            if (candles[i][2] > current_h)
            {
                current_h = candles[i][2];
            }

            if (candles[i][3] < current_l)
            {
                current_l = candles[i][3];
            }

            current_c = candles[i][4];
            current_v += candles[i][5];
        }
    }

    return make_tuple(ts, open, high, low, close, volume);
}