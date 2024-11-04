import argparse
import time
import requests
import math
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from collections import deque
from manage import redis_manage, dbsql_manage, utils


class Watcher_BIN:
    def __init__(self, symbol="BTC"):
        self.symbol = symbol
        self._get_information()
        self._recent_price_len_3s = 20
        self._format_buffer()
        redis_manage.setRedisData(f'ma_bin_{self.symbol}_status', 'init')
        redis_manage.setRedisData(f'ma_bin_{self.symbol}_start_time', str(utils.setTimezoneDateTime()))

        self._bin_stopped = False
        self._stopped_time = utils.setTimezoneDateTime()
        self.calc_min = False

        self._time_updown = 5  # 급등,급락기준(분)
        self._break_updown = 60  # 급등,급락시break시간(분)
        self._rate_updown = 2.1  # 등락율(%)
        self._ctime_updown = 20  # 급등,급락판단을위한계산주기(초)
        self._mtime_min = 30  # 최소참고중위가격(분)
        self._mtime_max = 31  # 최대참고중위가격(분)
        self.price = 0

        try:
            self._scheduler = BlockingScheduler()
            self._scheduler.add_job(self.watch3s_bin, 'cron', second="*/3")
            self._scheduler.add_job(self.watch1m_bin, 'cron', minute="*")
            self._scheduler.add_job(self._get_information, 'cron', minute="*/10", second="30")
        except RuntimeError as e:
            print("RuntimeError:", e)
            pass
        except KeyboardInterrupt:
            self._scheduler.shutdown()

    def _get_information(self):
        # Get follows info from database.
        params = dbsql_manage.get_watcher_params_db()
        if params is not None:
            self._mtime_min = int(params['w1'])  # 최소참고중위가격(분)
            self._mtime_max = int(params['w2'])  # 최대참고중위가격(분)
            self._ctime_updown = int(params['w3'])  # 급등,급락판단을위한계산주기(초)
            self._time_updown = int(params['w8'])  # 급등,급락기준(분)
            self._rate_updown = float(params['w9'])  # 등락율(%)
            self._break_updown = int(params['w10'])  # 급등,급락시break시간(분)

            self._recent_price_len_3s = math.ceil(self._ctime_updown / 3)
            self._recent_price_len_1m = self._mtime_max
        self._no_live = False

    def watch3s_bin(self):
        if self._bin_stopped:
            if not self._check_restart_MA():
                return

        if self.calc_min is True:
            return

        try:
            bin_status = redis_manage.getRedisData('bin_socket_status')
            if bin_status is not None and bin_status.decode("utf-8") == 'ok':
                price_bin = float(redis_manage.getRedisData('BIN_' + self.symbol.upper()))
            else:
                api_url = 'https://api.binance.com/api/v3/ticker/price'
                params = {'symbol': self.symbol.upper()}
                response = requests.get(api_url, params=params)
                data = response.json()
                price_bin = float(data['price'])
            if price_bin > 0:
                self.price = price_bin
                self.deque_append_left_bin(price_bin)
        except Exception as e:
            print(str(e))
            pass

    def deque_append_left_bin(self, value):
        self._bin_prices_3s.appendleft(value)

    def _format_buffer(self):
        self._bin_prices_3s = deque(self._recent_price_len_3s * [0], self._recent_price_len_3s)
        self._bin_prices_1m = deque(self._recent_price_len_1m * [0], self._recent_price_len_1m)
        self._bin_prices = deque()

    def _check_stop_MA(self, avr_price, rate):
        if not self._bin_stopped and avr_price > 0 and self._bin_prices_1m[self._time_updown - 1] > 0 and rate > self._rate_updown:
            self._bin_stopped = True
            self._stopped_time = utils.setTimezoneDateTime()
            # format prices ma deque
            self._format_buffer()
            try:
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_status', 'stop')
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_stop_time', str(self._stopped_time))
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_status_msg', f'rate_diff {rate} is bigger !')
            except Exception as e:
                print(str(e))
                return False
            return True
        return False

    def _check_restart_MA(self):
        _stopped_delay = utils.setTimezoneDateTime() - self._stopped_time
        if self._break_updown <= _stopped_delay.total_seconds() / 60:  # minutes
            self._bin_stopped = False
            try:
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_status', 'start')
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_start_time', str(utils.setTimezoneDateTime()))
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_status_msg', f'passed the {self._break_updown}!')
                return True
            except Exception as e:
                print(str(e))
                redis_manage.resetRedis()
                return False
        return False

    def _calculate_MA_RATE(self):
        _bin_none_zero_count = sum(1 if x > 0 else 0 for x in self._bin_prices_3s)
        avr_bin_price = sum(self._bin_prices_3s) / _bin_none_zero_count if _bin_none_zero_count > 0 else 0
        try:
            pre_price = self._bin_prices_1m[self._time_updown - 1]
            bin_rate = abs((pre_price - avr_bin_price) * 100 / avr_bin_price) if avr_bin_price * pre_price > 0 else 0
        except Exception as e:
            print(str(e))
            bin_rate = 0
            pass
        return avr_bin_price, bin_rate

    def _calculate_FINAL_MA_COUNT(self):
        bin_none_zero_count = sum(1 if x > 0 else 0 for x in self._bin_prices_1m)
        avr_bin_price_final = sum(self._bin_prices_1m) / bin_none_zero_count if bin_none_zero_count > 0 else 0
        return avr_bin_price_final, bin_none_zero_count

    def watch1m_bin(self):
        if self._bin_stopped:
            return
        self.calc_min = True
        avr_bin_price, bin_rate = self._calculate_MA_RATE()
        if self._check_stop_MA(avr_bin_price, bin_rate):
            return

        self._bin_prices_1m.appendleft(avr_bin_price)
        avr_bin_price_final, bin_none_zero_count = self._calculate_FINAL_MA_COUNT()

        self._bin_prices.appendleft(self.price)
        if bin_none_zero_count >= self._mtime_min:
            max_price = max(self._bin_prices)
            min_price = min(self._bin_prices)
            self._bin_prices.pop()
            payload = {
                "symbol": self.symbol,
                "time": str(utils.setTimezoneDateTime()),
                "price": str(avr_bin_price_final),
                "count": str(bin_none_zero_count),
                "max_price": str(max_price),
                "min_price": str(min_price)
            }
            try:
                redis_manage.hmsetRedisData(f'MA_BIN_{self.symbol.upper()}', payload)
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_live_time', str(utils.setTimezoneDateTime()))
                redis_manage.setRedisData(f'ma_bin_{self.symbol}_status', 'ok')
            except Exception as e:
                print(str(e))
                pass
        self.calc_min = False

    def update_settings(self):
        self._get_information()

    def run(self):
        self._scheduler.start()
        time.sleep(3)


def main(symbol="BTC"):
    watcher2_coin = Watcher_BIN(symbol=symbol)
    watcher2_coin.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--symbol",
        help="this is symbol name.",
        default='EOS',
        dest="symbol"
    )
    main(**vars(parser.parse_args()))
