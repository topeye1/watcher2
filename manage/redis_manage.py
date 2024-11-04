import redis
from datetime import datetime
from dotenv import load_dotenv
import os

from manage import utils

load_dotenv()

redis_db = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), password=os.getenv('REDIS_PASSWORD'), db=0)


def resetRedis():
    global redis_db
    redis_db = redis.Redis(host=os.getenv('REDIS_HOST'), port=6379, db=0)


def setRealCoinPrice(data, tp):
    coin_name = data[0]
    coin_price = data[1]
    setRedisData(coin_name, coin_price)
    setRedisData(tp + '_socket_status', 'ok')
    setRedisData(tp + '_socket_live_time', str(utils.setTimezoneDateTime()))


def setSocketError(tp):
    setRedisData(tp + '_socket_status', 'error')
    setRedisData(tp + '_socket_error_time', str(utils.setTimezoneDateTime()))


def setRedisData(key, data):
    redis_db.set(key, data)


def hmsetRedisData(key, data):
    redis_db.hmset(key, data)


def getRedisData(key):
    try:
        val = redis_db.get(key)
        if val is None or val == str():
            val = 0.0
    except Exception as e:
        print(f"exception key={key}, {e}")
        val = 0.0
        pass

    return val
