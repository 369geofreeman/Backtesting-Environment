from typing import *
import logging

import requests
import dateutil.parser


logger = logging.getLogger()


class FtxClient:
    def __init__(self):

        self._base_url = "https://ftx.com/api"

        self.symbols = self._get_symbols()

    def _make_request(self, endpoint: str, query_parameters: Dict):

        try:
            response = requests.get(self._base_url + endpoint, params=query_parameters)

        except Exception as e:
            logger.error("Connection error while making request to %s: %s",
                         endpoint, e)

            return None

        if response.status_code == 200:
            json_response = response.json()
            if json_response['success']:
                return json_response['result']
            else:
                logger.error("Error while making request to %s: %s (status code = %s)",
                             endpoint,
                             json_response,
                             json_response.status_code)
                return None

        else:
            logger.error("Error while making request to %s: %s (status code = %s)",
                         endpoint,
                         response.json(),
                         response.status_code)

            return None

    def _get_symbols(self) -> List[str]:

        params = dict()

        endpoint = "/markets"
        data = self._make_request(endpoint, params)

        symbols = [x['name'] for x in data]

        return symbols

    def get_historical_data(self, symbol: str,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None):

        params = dict()

        params['resolution'] = 60
        params['limit'] = 5000

        if start_time is not None:
            params['start_time'] = int(start_time / 1000)
        if end_time is not None:
            params['end_time'] = int(end_time / 1000)

        endpoint = f"/markets/{symbol}/candles"
        raw_candles = self._make_request(endpoint, params)

        candles = []

        if raw_candles is not None:
            for c in raw_candles:
                ts = dateutil.parser.isoparse(c["startTime"]).timestamp() * 1000
                candles.append(((ts), (c["open"]), (c["high"]), (c["low"]), (c["close"]), (c["volume"])))
            return candles
        else:
            return None






