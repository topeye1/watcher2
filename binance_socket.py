import asyncio
import json
import platform

import websockets

from manage.marketList import bin_markets
from manage import redis_manage


async def binance_ws_client(coin):
    url = f"wss://fstream.binance.com/ws/{coin}@trade"
    while True:
        try:
            async with websockets.connect(url, ping_interval=10) as websocket:
                while True:
                    recv_data = await websocket.recv()
                    res_data = json.loads(recv_data)
                    try:
                        symbol = res_data['s']
                        price = res_data['p']
                        market_data = [
                            'BIN_' + symbol,
                            float(price)
                        ]
                        redis_manage.setRealCoinPrice(market_data, "bin")
                    except Exception as e:
                        pass
        except Exception as e:
            print("ConnectionError, reconnection...", e)
            redis_manage.setSocketError("htx")
            await asyncio.sleep(5)


async def main():
    tasks = [binance_ws_client(market.lower()) for market in bin_markets]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
