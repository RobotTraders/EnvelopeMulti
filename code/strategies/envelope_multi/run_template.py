import asyncio
import sys
from pathlib import Path
import json

sys.path.append(str(Path(__file__).parents[2]))
from envelope_trader import EnvelopeTrader
from utilities.bitget_futures_async import BitgetFuturesAsync
from utilities.timeframe_utils import filter_active_timeframes


PARAMS = {
    '5m': {
        'XXX/USDT:USDT': {
            'balance_fraction': 0.5,
            'leverage': 1,
            'average_type': 'DCM',
            'average_period': 5,
            'envelopes_longs': [0.07, 0.11, 0.14, 0.17],
            'envelopes_shorts': [0.12, 0.16, 0.20],
            'stop_loss_pct': 0.45,
        },
    },
    '15m': { 
        'YYY/USDT:USDT': {
            'balance_fraction': 0.5,
            'leverage': 1,
            'average_type': 'DCM',
            'average_period': 5,
            'envelopes_longs': [0.07, 0.11, 0.14, 0.17],
            'stop_loss_pct': 0.45,
        },
    },  
    '30m': {},  
    '1h': {},   
    '4h': {},   
}

KEY_NAME = '' # << fill this according to the api key you want to use in secret.json
KEY_PATH = Path(__file__).parents[4] / 'EnvelopeMulti/secret.json'
PRODUCT_TYPE = 'USDT-FUTURES' # 'USDC-FUTURES'
MARGIN_MODE = 'isolated' # 'crossed'


async def main():   
    try:
        with open(KEY_PATH) as f:
            api_setup = json.load(f)[KEY_NAME]
            
        exchange = BitgetFuturesAsync(api_setup, product_type=PRODUCT_TYPE, margin_mode=MARGIN_MODE)
        
        tasks = [
            asyncio.create_task(exchange.load_markets()),
            asyncio.create_task(exchange.load_balance()),
        ]
        await asyncio.gather(*tasks)
            
        envelope_trader = EnvelopeTrader(
            params=PARAMS,
            exchange=exchange,
            margin_mode=MARGIN_MODE,
            verbose=False
        )
        await envelope_trader.run()
            
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
