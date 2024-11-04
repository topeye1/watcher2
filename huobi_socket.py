import asyncio
import platform
import time

import websockets
import json
import gzip
from manage.marketList import htx_markets
from manage import redis_manage


def decompress_data(data):
    return gzip.decompress(data).decode('utf-8')


async def huobi_ws_client(market):
    uri = "wss://api.hbdm.com/linear-swap-ws"
    while True:
        try:
            symbol = market.replace('USDT', '-USDT')
            async with websockets.connect(uri, ping_interval=10) as websocket:
                subscribe_message = {
                    "sub": f"market.{symbol}.kline.1min",
                    "id": market
                }
                await websocket.send(json.dumps(subscribe_message))
                while True:
                    response = await websocket.recv()
                    msg = decompress_data(response)
                    res_data = json.loads(msg)

                    try:
                        ch = res_data['ch']
                        symbol = ch.replace("market.", "").replace(".kline.1min", "").upper()
                        symbol = symbol.replace('-USDT', 'USDT')
                        tick = res_data['tick']
                        price = tick['close']
                        market_data = [
                            'HTX_' + symbol,
                            float(price)
                        ]
                        redis_manage.setRealCoinPrice(market_data, "htx")
                    except Exception as e:
                        pass
        except Exception as e:
            print("ConnectionError, reconnection...", e)
            redis_manage.setSocketError("htx")
            await asyncio.sleep(5)


async def main():
    tasks = [huobi_ws_client(market.upper()) for market in htx_markets]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
