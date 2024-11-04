import argparse
import time
import requests
import math
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from collections import deque
from manage import redis_manage, dbsql_manage, utils


class Watcher_HTX:
    def __init__(self, symbol="ADA"):
        self.symbol = symbol
        self._get_information()
        self._recent_price_len_3s = 20
        self._format_buffer()
        redis_manage.setRedisData(f'ma_htx_{self.symbol}_status', 'init')
        redis_manage.setRedisData(f'ma_htx_{self.symbol}_start_time', str(utils.setTimezoneDateTime()))

        self._htx_stopped = False
        self._stopped_time = utils.setTimezoneDateTime()
        self.calc_min = False

        self._time_updown = 5  # 급등,급락기준(분)
        self._break_updown = 60  # 급등,급락시break시간(분)
        self._rate_updown = 10  # 등락율(%)
        self._ctime_updown = 20  # 급등,급락판단을위한계산주기(초)
        self._mtime_min = 30  # 최소참고중위가격(분)
        self._mtime_max = 31  # 최대참고중위가격(분)
        self.price = 0

        try:
            self._scheduler = BlockingScheduler()
            self._scheduler.add_job(self.watch3s_htx, 'cron', second="*/3")
            self._scheduler.add_job(self.watch1m_htx, 'cron', minute="*")
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
            # self._rate_updown = float(params['w9'])  # 등락율(%)
            self._break_updown = int(params['w10'])  # 급등,급락시break시간(분)

            self._recent_price_len_3s = math.ceil(self._ctime_updown / 3)
            self._recent_price_len_1m = self._mtime_max
        self._no_live = False

    def watch3s_htx(self):
        if self._htx_stopped:
            if not self._check_restart_MA():
                return
        if self.calc_min is True:
            return

        try:
            htx_status = redis_manage.getRedisData('htx_socket_status')
            if htx_status is not None and htx_status.decode("utf-8") == 'ok':
                price_htx = float(redis_manage.getRedisData('HTX_' + self.symbol.upper()))
            else:
                symbol = self.symbol.replace('USDT', '-USDT')
                api_url = f'https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code={symbol}'
                response = requests.get(api_url)
                data = response.json()
                price = data['tick']['close']
                price_htx = float(price)
            if price_htx > 0:
                self.price = price_htx
                self.deque_append_left_htx(price_htx)
        except Exception as e:
            print(str(e))
            pass

    def deque_append_left_htx(self, value):
        self._htx_prices_3s.appendleft(value)

    def _format_buffer(self):
        self._htx_prices_3s = deque(self._recent_price_len_3s * [0], self._recent_price_len_3s)
        self._htx_prices_1m = deque(self._recent_price_len_1m * [0], self._recent_price_len_1m)
        self._htx_prices = deque()

    def _check_stop_MA(self, avr_price, rate):
        if not self._htx_stopped and avr_price > 0 and self._htx_prices_1m[self._time_updown - 1] > 0 and rate > self._rate_updown:
            self._htx_stopped = True
            self._stopped_time = utils.setTimezoneDateTime()
            # format prices ma deque
            self._format_buffer()
            try:
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_status', 'stop')
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_stop_time', str(self._stopped_time))
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_status_msg', f'rate_diff {rate} is bigger !')
                return True
            except Exception as e:
                print(str(e))
                return False
        return False

    def _check_restart_MA(self):
        _stopped_delay = utils.setTimezoneDateTime() - self._stopped_time
        if self._break_updown <= _stopped_delay.total_seconds() / 60:  # minutes
            self._htx_stopped = False
            try:
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_status', 'start')
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_start_time', str(utils.setTimezoneDateTime()))
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_status_msg', f'passed the {self._break_updown}!')
                return True
            except Exception as e:
                print(str(e))
                redis_manage.resetRedis()
                return False
        return False

    def _calculate_MA_RATE(self):
        _htx_none_count = sum(1 if x > 0 else 0 for x in self._htx_prices_3s)
        avr_htx_price = sum(self._htx_prices_3s) / _htx_none_count if _htx_none_count > 0 else 0
        try:
            pre_price = self._htx_prices_1m[self._time_updown - 1]
            htx_rate = abs((pre_price - avr_htx_price) * 100 / avr_htx_price) if avr_htx_price * pre_price > 0 else 0
        except Exception as e:
            print(str(e))
            htx_rate = 0
            pass
        return avr_htx_price, htx_rate

    def _calculate_FINAL_MA_COUNT(self):
        htx_none_count = sum(1 if x > 0 else 0 for x in self._htx_prices_1m)
        avr_htx_price_final = sum(self._htx_prices_1m) / htx_none_count if htx_none_count > 0 else 0
        return avr_htx_price_final, htx_none_count

    def watch1m_htx(self):
        if self._htx_stopped:
            return
        self.calc_min = True
        avr_htx_price, htx_rate = self._calculate_MA_RATE()
        if self._check_stop_MA(avr_htx_price, htx_rate):
            return

        self._htx_prices_1m.appendleft(avr_htx_price)
        avr_htx_price_final, htx_none_count = self._calculate_FINAL_MA_COUNT()

        self._htx_prices.appendleft(self.price)
        if htx_none_count >= self._mtime_min:
            max_price = max(self._htx_prices)
            min_price = min(self._htx_prices)
            self._htx_prices.pop()
            payload = {
                "symbol": self.symbol,
                "time": str(utils.setTimezoneDateTime()),
                "price": str(avr_htx_price_final),
                "count": str(htx_none_count),
                "max_price": str(max_price),
                "min_price": str(min_price)
            }
            try:
                redis_manage.hmsetRedisData(f'MA_HTX_{self.symbol.upper()}', payload)
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_live_time', str(utils.setTimezoneDateTime()))
                redis_manage.setRedisData(f'ma_htx_{self.symbol}_status', 'ok')
            except Exception as e:
                print(str(e))
                pass
        self.calc_min = False

    def update_settings(self):
        self._get_information()

    def run(self):
        self._scheduler.start()
        time.sleep(3)


def main(symbol="EOS"):
    watcher2_coin = Watcher_HTX(symbol=symbol)
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
