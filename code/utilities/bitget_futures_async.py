import asyncio
import time
from uuid import uuid4
import pandas as pd
from asyncio import gather
from pydantic import BaseModel
from enum import Enum
from datetime import datetime
from typing import Any, Optional, Dict, List
import ccxt.async_support as ccxt
from ccxt.base.errors import DDoSProtection


MAX_RETRIES = 5
BASE_DELAY = 1


class BitgetPlanOrderData(BaseModel):
    orderId: str
    clientOid: str

class BitgetPlanOrderResponse(BaseModel):
    code: str
    data: BitgetPlanOrderData
    msg: str
    requestTime: int

class TpslPlanType(str, Enum):
    TAKE_PROFIT = 'profit_plan'      
    STOP_LOSS = 'loss_plan'          
    TRAILING_STOP = 'moving_plan'    
    POSITION_TAKE_PROFIT = 'pos_profit' 
    POSITION_STOP_LOSS = 'pos_loss'     

class TriggerPlanType(str, Enum):
    NORMAL = 'normal_plan'     
    TRAILING = 'track_plan'   

class TriggerType(str, Enum):
    MARK_PRICE = 'mark_price'  
    FILL_PRICE = 'fill_price' 

class OrderType(str, Enum):
    LIMIT = 'limit'    
    MARKET = 'market' 

class BitgetPendingOrdersData(BaseModel):
    endId: Optional[str]
    entrustedList: Optional[List[Dict[str, Any]]]
    
class BitgetPendingOrdersResponse(BaseModel):
    code: str
    data: BitgetPendingOrdersData
    msg: str
    requestTime: int

class CancelPlanOrderItem(BaseModel):
    orderId: str
    clientOid: str
    errorMsg: Optional[str] = None

class CancelPlanOrderData(BaseModel):
    successList: List[CancelPlanOrderItem]
    failureList: List[CancelPlanOrderItem]

class CancelPlanOrderResponse(BaseModel):
    code: str
    data: CancelPlanOrderData
    msg: str
    requestTime: int

class PositionMode(str, Enum):
    ONE_WAY = 'one_way_mode'
    HEDGE = 'hedge_mode'

class SetPositionModeData(BaseModel):
    posMode: str

class SetPositionModeResponse(BaseModel):
    code: str
    data: SetPositionModeData
    msg: str
    requestTime: int

class BitgetHistoricalOrderData(BaseModel):
    planType: str
    symbol: str
    size: str
    orderId: str
    executeOrderId: Optional[str]
    clientOid: Optional[str]
    planStatus: str
    price: str
    executePrice: str
    feeDetail: Optional[Any]
    baseVolume: Optional[str]
    callbackRatio: Optional[str]
    triggerPrice: str
    triggerType: str
    side: str
    posSide: str
    marginCoin: str
    marginMode: str
    enterPointSource: str
    tradeSide: str
    posMode: str
    orderType: str
    cTime: str
    uTime: Optional[str]
    stopSurplusExecutePrice: Optional[str]
    stopSurplusTriggerPrice: Optional[str]
    stopSurplusTriggerType: Optional[str]
    stopLossExecutePrice: Optional[str]
    stopLossTriggerPrice: Optional[str]
    stopLossTriggerType: Optional[str]

class BitgetHistoricalOrdersData(BaseModel):
    entrustedList: Optional[List[BitgetHistoricalOrderData]] = None
    endId: Optional[str] = None

class BitgetHistoricalOrdersResponse(BaseModel):
    code: str
    data: BitgetHistoricalOrdersData
    msg: str
    requestTime: int

class BitgetFuturesAsync:
    def __init__(self, api_setup: Optional[Dict[str, Any]] = None, product_type: Optional[str] = 'USDT-FUTURES', margin_mode: Optional[str] = 'isolated') -> None:
        if api_setup is None:
            self.session = ccxt.bitget(config={'enableRateLimit': True})
        else:
            api_setup.update({
                "options": {"defaultType": "future"},
                "enableRateLimit": True,
                "rateLimit": 60,
            })
            self.session = ccxt.bitget(api_setup)
            
        self.futures_params = {
            'productType': product_type,
            'marginCoin': 'USDT' if product_type == 'USDT-FUTURES' else 'USDC',
            'marginMode': margin_mode,
        }
        self.private_mix_session = BitgetCcxtPrivateMix(self.session, self.futures_params)
        self.total_balance = 0
        

    async def load_markets(self):
        try:
            self.markets = await self.session.load_markets()
        except Exception as e:
            raise Exception(f"Failed to initialize exchange: {e}")

    async def load_balance(self) -> None:
        try:
            balance_data = await self.session.fetch_balance()
            self.total_balance = balance_data['USDT']['total']
        except Exception as e:
            raise Exception(f"Failed to load balance: {e}")
        
    async def close(self):
        if hasattr(self, 'session') and self.session is not None:
            try:
                await self.session.close()
            except Exception as e:
                print(f"Error during session cleanup: {e}")

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.fetch_ticker(symbol)
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch ticker for {symbol}: {e}")

    def fetch_min_tick_size(self, symbol: str) -> float:
        try:                
            return self.markets[symbol]['precision']['price']
        except Exception as e:
            raise Exception(f"Failed to fetch minimum tick size for {symbol}: {e}")
        
    def fetch_min_amount_tradable(self, symbol: str) -> float:
        try:
            return self.markets[symbol]['limits']['amount']['min']
        except Exception as e:
            raise Exception(f"Failed to fetch minimum amount tradable: {e}")

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        try:
            return self.session.amount_to_precision(symbol, amount)
        except Exception as e:
            raise Exception(f"Failed to convert amount {amount} {symbol} to precision", e)
        
    def price_to_precision(self, symbol: str, price: float) -> str:
        try:
            if price == None:
                return None
            return self.session.price_to_precision(symbol, price)
        except Exception as e:
            raise Exception(f"Failed to convert price {price} to precision for {symbol}", e)

    async def fetch_balance(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.fetch_balance(params or {})
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch balance: {e}")

    async def fetch_recent_ohlcv(self, symbol: str, timeframe: str) -> pd.DataFrame:
        bitget_fetch_limit = 200
        timeframe_to_milliseconds = {
            '1m': 60000, '2m': 120000, '3m': 180000, '5m': 300000, '15m': 900000, '30m': 1800000,
            '1h': 3600000, '2h': 7200000, '4h': 14400000, '1d': 86400000,
        }
        
        end_timestamp = int(time.time() * 1000)
        start_timestamp = end_timestamp - (bitget_fetch_limit * timeframe_to_milliseconds[timeframe])
        current_timestamp = start_timestamp

        async def fetch_chunk(start: int, end: int) -> List:
            try:
                for attempt in range(MAX_RETRIES):
                    try:
                        return await self.session.fetch_ohlcv(
                            symbol,
                            timeframe,
                            params={
                                "startTime": str(start),
                                "endTime": str(end),
                                "limit": bitget_fetch_limit,
                            }
                        )
                    except Exception as e:
                        if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                            delay = BASE_DELAY * (2 ** attempt)
                            print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        raise e
            except Exception as e:
                raise Exception(f"Failed to fetch OHLCV chunk: {e}")

        chunks = []
        while current_timestamp < end_timestamp:
            request_end_timestamp = min(
                current_timestamp + (bitget_fetch_limit * timeframe_to_milliseconds[timeframe]),
                end_timestamp
            )
            chunks.append(fetch_chunk(current_timestamp, request_end_timestamp))
            current_timestamp += (bitget_fetch_limit * timeframe_to_milliseconds[timeframe]) + 1

        ohlcv_data = []
        chunk_results = await gather(*chunks)
        for chunk in chunk_results:
            ohlcv_data.extend(chunk)

        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)

        return df

    async def fetch_order(self, id: str, symbol: str) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.fetch_order(id, symbol)
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch order {id} info for {symbol}: {e}")

    async def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.fetch_open_orders(symbol)
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch open orders: {e}")

    async def fetch_open_positions(self, symbols=None) -> List[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    if not symbols:
                        return []
                    
                    if not isinstance(symbols, list):
                        symbols = [symbols]
                    
                    positions = await self.session.fetch_positions(symbols, params=self.futures_params)
                    return [
                        pos for pos in positions 
                        if pos.get('contracts') is not None 
                        and float(pos['contracts']) > 0
                    ]
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch open positions: {e}")

    async def flash_close_position(self, symbol: str, side: Optional[str] = None) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.close_position(symbol, side=side)
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to fetch closed order for {symbol}", e)

    async def set_leverage(self, symbol: str, margin_mode: str = 'isolated', leverage: int = 1) -> None:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    if margin_mode == 'isolated':
                        params = {**self.futures_params, 'holdSide': 'long'}
                        await self.session.set_leverage(leverage, symbol, params=params)
                        
                        params = {**self.futures_params, 'holdSide': 'short'}
                        await self.session.set_leverage(leverage, symbol, params=params)
                    else:
                        await self.session.set_leverage(leverage, symbol, params=self.futures_params)
                    return
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to set leverage: {e}")

    async def set_margin_mode(self, symbol: str, margin_mode: str = 'isolated') -> None:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    await self.session.set_margin_mode(
                        margin_mode,
                        symbol,
                        params=self.futures_params,
                    )
                    return
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to set margin mode: {e}") 

    async def cancel_trigger_orders(self, ids: List[str], symbol: str) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.session.cancel_orders(ids, symbol, params={'stop': True})
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
        except Exception as e:
            raise Exception(f"Failed to cancel the {symbol} trigger orders {ids}", e)
        
    # ------------------------
    #  "Private Mix" Methods
    # ------------------------
    async def place_market_trigger_order(
        self, 
        oid: str, 
        symbol: str, 
        side: str, 
        size: str, 
        trigger_price: str, 
        stop_loss_price: str, 
    ) -> Optional[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    trigger_price = self.price_to_precision(symbol, trigger_price)
                    stop_loss_price = self.price_to_precision(symbol, stop_loss_price)
                    size = self.amount_to_precision(symbol, size)
                    
                    return await self.private_mix_session.place_trigger_order(
                        client_oid=oid,
                        symbol=symbol,
                        size=size,
                        side=side,
                        trigger_price=trigger_price,
                        stop_loss_trigger_price=stop_loss_price,
                        trade_side="open",
                        order_type=OrderType.MARKET,
                        plan_type=TriggerPlanType.NORMAL,
                        trigger_type=TriggerType.FILL_PRICE,
                        stop_loss_trigger_type=TriggerType.FILL_PRICE,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to place trigger market order: {e}")

    async def place_limit_trigger_order(
        self, 
        oid: str, 
        symbol: str, 
        side: str, 
        size: str, 
        execution_price: str,
        trigger_price: str, 
        stop_loss_price: str, 
    ) -> Optional[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    trigger_price = self.price_to_precision(symbol, trigger_price)
                    stop_loss_price = self.price_to_precision(symbol, stop_loss_price)
                    price = self.price_to_precision(symbol, execution_price)
                    size = self.amount_to_precision(symbol, size)
                    
                    return await self.private_mix_session.place_trigger_order(
                        client_oid=oid,
                        symbol=symbol,
                        size=size,
                        side=side,
                        price=price,
                        trigger_price=trigger_price,
                        stop_loss_trigger_price=stop_loss_price,
                        trade_side="open",
                        order_type=OrderType.LIMIT,
                        plan_type=TriggerPlanType.NORMAL,
                        trigger_type=TriggerType.FILL_PRICE,
                        stop_loss_trigger_type=TriggerType.FILL_PRICE,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to place trigger limit order: {e}")
        
    async def modify_market_trigger_order(
        self,
        oid: str,
        symbol: str,
        trigger_price: str,
        size: str,
        stop_loss_price: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    trigger_price = self.price_to_precision(symbol, trigger_price)
                    size = self.amount_to_precision(symbol, size)
                    if stop_loss_price is not None and stop_loss_price != "0":
                        stop_loss_price = self.price_to_precision(symbol, stop_loss_price)
                        
                    return await self.private_mix_session.modify_trigger_order(
                        client_oid=oid,
                        new_size=size,
                        new_trigger_price=trigger_price,
                        new_stop_loss_trigger_price=stop_loss_price,
                        new_stop_loss_trigger_type=TriggerType.FILL_PRICE,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to modify trigger market order: {e}")

    async def modify_limit_trigger_order(
        self,
        oid: str,
        symbol: str,
        execution_price: str,
        trigger_price: str,
        size: str,
        stop_loss_price: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    trigger_price = self.price_to_precision(symbol, trigger_price)
                    price = self.price_to_precision(symbol, execution_price)
                    size = self.amount_to_precision(symbol, size)
                    if stop_loss_price is not None and stop_loss_price != "0":
                        stop_loss_price = self.price_to_precision(symbol, stop_loss_price)
                       
                    return await self.private_mix_session.modify_trigger_order(
                        client_oid=oid,
                        new_size=size,
                        new_price=price,
                        new_trigger_price=trigger_price,
                        new_stop_loss_trigger_price=stop_loss_price,
                        new_stop_loss_trigger_type=TriggerType.FILL_PRICE,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to modify trigger limit order: {e}")

    async def place_position_stop_loss(self, symbol: str, hold_side: str, price: str) -> None:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    price = self.price_to_precision(symbol, price)
                    return await self.private_mix_session.place_tpsl_order(
                        symbol,
                        price,
                        hold_side,
                        plan_type=TpslPlanType.POSITION_STOP_LOSS,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            print(f"Error placing stop loss for {symbol}: {str(e)}")
            raise Exception(f"Failed to place stop loss for {symbol} position: {e}")

    async def modify_position_stop_loss(self, symbol: str, hold_side: str, price: str, client_oid: str) -> None:
        """Modify an existing position stop loss order"""
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    price = self.price_to_precision(symbol, price)
                    return await self.private_mix_session.modify_tpsl_order(
                        symbol=symbol,
                        client_oid=client_oid,
                        trigger_price=price,
                        size="0",
                        trigger_type=TriggerType.MARK_PRICE
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
            
        except Exception as e:
            print(f"Error modifying stop loss for {symbol}: {str(e)}")
            raise Exception(f"Failed to modify stop loss for {symbol} position: {e}")
        
    async def place_position_take_profit(self, symbol: str, hold_side: str, price: str) -> None:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    price = self.price_to_precision(symbol, price)
                    return await self.private_mix_session.place_tpsl_order(
                        symbol,
                        price,
                        hold_side,
                        plan_type=TpslPlanType.POSITION_TAKE_PROFIT,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            print(f"Error placing take profit for {symbol}: {str(e)}")
            raise Exception(f"Failed to place take profit for {symbol} position: {e}")

    async def fetch_open_trigger_orders(self, symbol: str) -> List[str]:
        try:
            response = await self.private_mix_session.fetch_pending_trigger_orders(
                symbol=symbol,
                plan_type='normal_plan'
            )
            if response.data.entrustedList is None:
                return []
            
            return [
                order['clientOid'] for order in response.data.entrustedList 
                if order['planStatus'] == 'live'
            ]
        except Exception as e:
            raise Exception(f"Failed to fetch open trigger orders: {e}")

    async def cancel_open_sl_orders(self, symbol: str) -> None:
        try:
            response = await self.private_mix_session.fetch_pending_trigger_orders(
                symbol=symbol,
                plan_type='profit_loss'
            )
            if response.data.entrustedList is None:
                return
            
            tasks = [
                asyncio.create_task(
                    self.private_mix_session.cancel_plan_order(
                        symbol=symbol,
                        order_id=order['orderId'],
                        plan_type='loss_plan'
                    )
                )
                for order in response.data.entrustedList
                if order['planType'] == 'loss_plan'
            ]
              
        except Exception as e:
            raise Exception(f"Failed to fetch open trigger orders: {e}")

    async def place_exit_order(
        self, 
        oid: str, 
        symbol: str, 
        side: str, 
        size: str, 
        trigger_price: str, 
    ) -> Optional[Dict[str, Any]]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    trigger_price = self.price_to_precision(symbol, trigger_price)
                    size = self.amount_to_precision(symbol, size)
            
                    return await self.private_mix_session.place_trigger_order(
                        client_oid=oid,
                        symbol=symbol,
                        size=size,
                        side=side,
                        trigger_price=trigger_price,
                        trade_side="close",
                        reduce_only="yes",
                        order_type=OrderType.MARKET,
                        plan_type=TriggerPlanType.NORMAL,
                        trigger_type=TriggerType.FILL_PRICE,
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to place trigger market order: {e}")

    async def fetch_last_executed_tpsl_order(self, symbol: str) -> Optional[BitgetHistoricalOrderData]:
        try:
            response = await self.private_mix_session.fetch_trigger_orders_history(
                plan_type='profit_loss',
                symbol=symbol,
                plan_status='executed'
            )
            
            if response.data.entrustedList:
                return response.data.entrustedList[0] 
            return None
            
        except Exception as e:
            raise Exception(f"Failed to fetch TPSL orders history: {e}")
        
    async def set_hedge_mode(self) -> Dict[str, Any]:
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    return await self.private_mix_session.set_position_mode(
                        productType=self.futures_params['productType'],
                        posMode=PositionMode.HEDGE
                    )
                except Exception as e:
                    if (isinstance(e, DDoSProtection) or str(e).startswith('{"code":"429"')) and attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"Rate limit hit, attempt {attempt + 1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise e
                
        except Exception as e:
            raise Exception(f"Failed to set hedge mode: {e}")
                           
    @staticmethod
    def generate_order_id(prefix: str = '') -> str:
        """
        Generate a unique order ID
        
        Format: {prefix}{timestamp}_{uuid4_first_8_chars}
        Example: TP_1635724800_a1b2c3d4
        """
        timestamp = int(datetime.now().timestamp())
        unique_id = str(uuid4())[:8] 
        return f"{prefix}{timestamp}_{unique_id}"


class BitgetCcxtPrivateMix:
    """
    Wrapper for CCXT's private mix API methods for Bitget Futures
    
    https://github.com/ccxt/ccxt/blob/master/python/ccxt/abstract/bitget.py
    """
    
    def __init__(
            self,
            session: BitgetFuturesAsync,
            futures_params: Dict[str, Any],
        ):
        self.session = session
        self.product_type = futures_params['productType']
        self.margin_mode = futures_params['marginMode']
        self.margin_coin = futures_params['marginCoin']

    def _ccxt_to_bitget_symbol(self, symbol: str) -> str:
        """ex: 'BTC/USDT:USDT' -> 'BTCUSDT'"""
        try:
            return f"{symbol.split(':')[0].replace('/', '')}"
        except Exception as e:
            raise Exception(f"Failed to convert symbol {symbol} to v2 format: {e}")
    
    async def place_trigger_order(
        self,
        symbol: str,             
        size: str,               
        side: str,              
        trigger_price: str,      
        order_type: OrderType,   
        plan_type: TriggerPlanType,
        trigger_type: TriggerType,
        price: Optional[str] = None,
        callback_ratio: Optional[str] = None,
        trade_side: Optional[str] = None,  
        client_oid: Optional[str] = None,
        reduce_only: Optional[str] = None, 
        stop_surplus_trigger_price: Optional[str] = None,
        stop_surplus_execute_price: Optional[str] = None,
        stop_surplus_trigger_type: Optional[TriggerType] = None,  
        stop_loss_trigger_price: Optional[str] = None,
        stop_loss_execute_price: Optional[str] = None,
        stop_loss_trigger_type: Optional[TriggerType] = None,  
        stp_mode: Optional[str] = None, 
    ) -> BitgetPlanOrderResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/Place-Plan-Order
        POST /api/v2/mix/order/place-plan-order
        """
        params = {
            'planType': plan_type.value,
            'symbol': self._ccxt_to_bitget_symbol(symbol),
            'productType': self.product_type,
            'marginMode': self.margin_mode,
            'marginCoin': self.margin_coin,
            'size': size,
            'price': price,
            'callbackRatio': callback_ratio,
            'triggerPrice': trigger_price,
            'triggerType': trigger_type.value,
            'side': side,
            'tradeSide': trade_side,
            'orderType': order_type,
            'clientOid': client_oid,
            'reduceOnly': reduce_only,
            'stopSurplusTriggerPrice': stop_surplus_trigger_price,
            'stopSurplusExecutePrice': stop_surplus_execute_price,
            'stopSurplusTriggerType': stop_surplus_trigger_type,
            'stopLossTriggerPrice': stop_loss_trigger_price,
            'stopLossExecutePrice': stop_loss_execute_price,
            'stopLossTriggerType': stop_loss_trigger_type,
            'stpMode': stp_mode
        }
        
        response = await self.session.private_mix_post_v2_mix_order_place_plan_order(params)
        return BitgetPlanOrderResponse(**response)

    async def modify_trigger_order(
        self,
        order_id: Optional[str] = None,          
        client_oid: Optional[str] = None,        
        new_size: Optional[str] = None,           
        new_price: Optional[str] = None,          
        new_callback_ratio: Optional[str] = None, 
        new_trigger_price: Optional[str] = None,  
        new_trigger_type: Optional[TriggerType] = None,   
        new_stop_surplus_trigger_price: Optional[str] = None,  
        new_stop_surplus_execute_price: Optional[str] = None,  
        new_stop_surplus_trigger_type: Optional[TriggerType] = None,   
        new_stop_loss_trigger_price: Optional[str] = None,     
        new_stop_loss_execute_price: Optional[str] = None,     
        new_stop_loss_trigger_type: Optional[TriggerType] = None,      
    ) -> BitgetPlanOrderResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/Modify-Plan-Order
        POST /api/v2/mix/order/modify-plan-order
        """
        params = {
            'orderId': order_id,
            'clientOid': client_oid,
            'productType': self.product_type,
            'newSize': new_size,
            'newPrice': new_price,
            'newCallbackRatio': new_callback_ratio,
            'newTriggerPrice': new_trigger_price,
            'newTriggerType': new_trigger_type,
            'newStopSurplusTriggerPrice': new_stop_surplus_trigger_price,
            'newStopSurplusExecutePrice': new_stop_surplus_execute_price,
            'newStopSurplusTriggerType': new_stop_surplus_trigger_type,
            'newStopLossTriggerPrice': new_stop_loss_trigger_price,
            'newStopLossExecutePrice': new_stop_loss_execute_price,
            'newStopLossTriggerType': new_stop_loss_trigger_type
        }
        
        response = await self.session.private_mix_post_v2_mix_order_modify_plan_order(params)
        return BitgetPlanOrderResponse(**response)

    async def place_tpsl_order(
        self,
        symbol: str,
        trigger_price: str,
        hold_side: str,       
        plan_type: TpslPlanType,
        size: Optional[str] = None,
        trigger_type: Optional[TriggerType] = None,  
        execute_price: Optional[str] = None,  
        range_rate: Optional[str] = None,   
        client_oid: Optional[str] = None,
        stp_mode: Optional[str] = None,      
    ) -> BitgetPlanOrderResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/Place-TPSL-Order
        POST /api/v2/mix/order/place-tpsl-order
        """
        params = {
            'marginCoin': self.margin_coin,
            'productType': self.product_type,
            'symbol': self._ccxt_to_bitget_symbol(symbol),
            'planType': plan_type.value,
            'triggerPrice': trigger_price,
            'triggerType': trigger_type,
            'executePrice': execute_price,
            'holdSide': hold_side,
            'size': size,
            'rangeRate': range_rate,
            'clientOid': client_oid,
            'stpMode': stp_mode
        }
        
        response = await self.session.private_mix_post_v2_mix_order_place_tpsl_order(params)
        return BitgetPlanOrderResponse(**response)

    async def modify_tpsl_order(
            self,
            symbol: str,
            size: str,
            trigger_price: str,
            order_id: Optional[str] = None,
            client_oid: Optional[str] = None,
            trigger_type: Optional[TriggerType] = None,
            execute_price: Optional[str] = None,
            range_rate: Optional[str] = None,
        ) -> BitgetPlanOrderResponse:
            """
            https://www.bitget.com/api-doc/contract/plan/Modify-Tpsl-Order
            POST /api/v2/mix/order/modify-tpsl-order
            
            Modify the stop-profit and stop-loss plan order
            """
            params = {
                'marginCoin': self.margin_coin,
                'productType': self.product_type,
                'symbol': self._ccxt_to_bitget_symbol(symbol),
                'orderId': order_id,
                'clientOid': client_oid,
                'triggerPrice': trigger_price,
                'triggerType': trigger_type.value if trigger_type else None,
                'executePrice': execute_price,
                'size': size,
                'rangeRate': range_rate
            }
            
            response = await self.session.private_mix_post_v2_mix_order_modify_tpsl_order(params)
            return BitgetPlanOrderResponse(**response)
 
    async def fetch_pending_trigger_orders(
        self,
        plan_type: str,
        order_id: Optional[str] = None,
        client_oid: Optional[str] = None,
        symbol: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[str] = None,
        id_less_than: Optional[str] = None
    ) -> BitgetPendingOrdersResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/get-orders-plan-pending
        GET /api/v2/mix/order/orders-plan-pending
        """
        params = {
            'productType': self.product_type,
            'planType': plan_type,
            # 'orderId': order_id,
            # 'clientOid': client_oid,
            'symbol': self._ccxt_to_bitget_symbol(symbol),
            # 'startTime': start_time,
            # 'endTime': end_time,
            # 'limit': limit,
            # 'idLessThan': id_less_than
        }
        
        response = await self.session.private_mix_get_v2_mix_order_orders_plan_pending(params)
        return BitgetPendingOrdersResponse(**response)

    async def cancel_plan_order(
        self,
        order_id: Optional[str] = None,
        client_oid: Optional[str] = None,
        order_id_list: Optional[List[str]] = None,
        symbol: Optional[str] = None,
        plan_type: Optional[str] = None
    ) -> CancelPlanOrderResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/Cancel-Plan-Order
        POST /api/v2/mix/order/cancel-plan-order
        """
        params = {
            'productType': self.product_type,
            # 'marginCoin': self.margin_coin,
            # 'orderIdList': order_id_list,
            'orderId': order_id,
            # 'clientOid': client_oid,
            'symbol': self._ccxt_to_bitget_symbol(symbol),
            'planType': plan_type
        }
           
        response = await self.session.private_mix_post_v2_mix_order_cancel_plan_order(params)
        return CancelPlanOrderResponse(**response)

    async def set_position_mode(
        self,
        productType: str,
        posMode: PositionMode
    ) -> SetPositionModeResponse:
        """
        https://www.bitget.com/api-doc/contract/account/Change-Hold-Mode
        POST /api/v2/mix/account/set-position-mode

        :param str productType: Product type (USDT-FUTURES, COIN-FUTURES, etc)
        :param PositionMode posMode: Position mode (one_way_mode or hedge_mode)
        """
        params = {
            'productType': productType,
            'posMode': posMode.value
        }
        
        response = await self.session.private_mix_post_v2_mix_account_set_position_mode(params)
        return SetPositionModeResponse(**response)

    async def fetch_trigger_orders_history(
        self,
        plan_type: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        client_oid: Optional[str] = None,
        plan_status: Optional[str] = None,
        id_less_than: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[str] = None,
    ) -> BitgetHistoricalOrdersResponse:
        """
        https://www.bitget.com/api-doc/contract/plan/orders-plan-history
        GET /api/v2/mix/order/orders-plan-history
        
        Fetch historical trigger orders with various filters
        """
        params = {
            'productType': self.product_type,
            'planType': plan_type,
            'symbol': self._ccxt_to_bitget_symbol(symbol) if symbol else None,
            'orderId': order_id,
            'clientOid': client_oid,
            'planStatus': plan_status,
            'idLessThan': id_less_than,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        
        params = {k: v for k, v in params.items() if v is not None}
        
        response = await self.session.private_mix_get_v2_mix_order_orders_plan_history(params)
        
        try:
            return BitgetHistoricalOrdersResponse(**response)
        except Exception as e:
            raise

