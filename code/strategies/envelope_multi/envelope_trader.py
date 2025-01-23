import sys
import pandas as pd
import ta
import os
import aiofiles
import json
import asyncio
from pathlib import Path
from enum import Enum
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Set, Tuple

sys.path.append(str(Path(__file__).parents[2]))

from utilities.bitget_futures_async import BitgetFuturesAsync
from utilities.time_utils import get_timestamp
from utilities.timeframe_utils import filter_active_timeframes


#------------------------------------------------------------------------------
# ENUMS, CONSTANTS, UTILITIES
#------------------------------------------------------------------------------

class AverageType(Enum):
    DCM = "DCM"  
    SMA = "SMA"  
    EMA = "EMA"  
    WMA = "WMA" 

class TradingStatus(Enum):
    OK_TO_TRADE = "ok_to_trade"
    STOP_LOSS_TRIGGERED = "stop_loss_triggered"

class PositionSide(Enum):
    LONG = "long"
    SHORT = "short"

class AverageSource(str, Enum):
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    HL2 = "hl2"       
    HLC3 = "hlc3"      
    OHLC4 = "ohlc4"    
    HLCC4 = "hlcc4"    

def verbose_print(symbol: str, message: str, verbose: bool = False) -> None:
    if verbose:
        print(f"{get_timestamp()} - {symbol} - {message}")

#------------------------------------------------------------------------------
# CUSTOM EXCEPTIONS
#------------------------------------------------------------------------------

class TradingError(Exception):
    pass

class IndicatorError(TradingError):
    pass

class OrderError(TradingError):
    pass

class PositionError(TradingError):
    pass

class ConfigurationError(TradingError):
    pass

class TrackerError(TradingError):
    pass

#------------------------------------------------------------------------------
# DATA MODELS
#------------------------------------------------------------------------------

@dataclass
class TradingParameters:
    symbol: str
    timeframe: Optional[str] = None
    balance_fraction: Optional[float] = None
    average_type: Optional[str] = None
    average_period: Optional[int] = None
    stop_loss_pct: Optional[float] = None
    stop_loss_after_entry_pct: Optional[float] = None
    average_source: Optional[str] = "close"
    leverage: Optional[int] = 1
    margin_mode: Optional[str] = 'isolated'
    envelopes_longs: List[float] = field(default_factory=list)
    envelopes_shorts: List[float] = field(default_factory=list)
    envelopes_sizing_longs: Optional[List[float]] = None
    envelopes_sizing_shorts: Optional[List[float]] = None
    exit_type: Optional[str] = "market"   
    entry_type: Optional[str] = "market"  
    delta_ticks_factor: Optional[int] = 5
    ohlcv_source: Optional[str] = "default"
    verbose: Optional[bool] = False
    exit_offset_pct: Optional[float] = 0

    def __post_init__(self):
        if self.stop_loss_after_entry_pct is None:
            self.stop_loss_after_entry_pct = self.stop_loss_pct

        self.validate()

    def validate(self) -> None:
        if not self.symbol:
            raise ConfigurationError("Symbol must be specified")
            
        if self.balance_fraction is not None and not 0 < self.balance_fraction <= 1:
            raise ConfigurationError(
                f"Balance fraction must be between 0 and 1, got {self.balance_fraction}"
            )
        if self.leverage is not None and self.leverage < 1:
            raise ConfigurationError("Leverage must be greater than 0")
        
        if self.exit_offset_pct is not None and not 0 <= self.exit_offset_pct <= 1:
            raise ConfigurationError(
                f"Exit offset percentage must be between 0 and 1, got {self.exit_offset_pct}"
            )
        
        if self.envelopes_longs:
            if any(pct <= 0 for pct in self.envelopes_longs):
                raise ConfigurationError(
                    f"Long envelope percentages must be positive, got {self.envelopes_longs}"
                )
            if len(set(self.envelopes_longs)) != len(self.envelopes_longs):
                raise ConfigurationError(
                    f"Duplicate percentages in long envelopes: {self.envelopes_longs}"
                )
            if self.envelopes_sizing_longs:
                if len(self.envelopes_sizing_longs) != len(self.envelopes_longs):
                    raise ConfigurationError(
                        f"Long envelope sizing count ({len(self.envelopes_sizing_longs)}) "
                        f"must match envelope count ({len(self.envelopes_longs)})"
                    )
                if any(size <= 0 or size > 1 for size in self.envelopes_sizing_longs):
                    raise ConfigurationError(
                        f"Long envelope sizing values must be between 0 and 1, got {self.envelopes_sizing_longs}"
                    )
                if sum(self.envelopes_sizing_longs) > 1:
                    raise ConfigurationError(
                        f"Sum of long envelope sizing values must be <= 1, got {sum(self.envelopes_sizing_longs)}"
                    )
            
        if self.envelopes_shorts:
            if any(pct <= 0 for pct in self.envelopes_shorts):
                raise ConfigurationError(
                    f"Short envelope percentages must be positive, got {self.envelopes_shorts}"
                )
            if len(set(self.envelopes_shorts)) != len(self.envelopes_shorts):
                raise ConfigurationError(
                    f"Duplicate percentages in short envelopes: {self.envelopes_shorts}"
                )
            if self.envelopes_sizing_shorts:
                if len(self.envelopes_sizing_shorts) != len(self.envelopes_shorts):
                    raise ConfigurationError(
                        f"Short envelope sizing count ({len(self.envelopes_sizing_shorts)}) "
                        f"must match envelope count ({len(self.envelopes_shorts)})"
                    )
                if any(size <= 0 or size > 1 for size in self.envelopes_sizing_shorts):
                    raise ConfigurationError(
                        f"Short envelope sizing values must be between 0 and 1, got {self.envelopes_sizing_shorts}"
                    )
                if sum(self.envelopes_sizing_shorts) > 1:
                    raise ConfigurationError(
                        f"Sum of short envelope sizing values must be <= 1, got {sum(self.envelopes_sizing_shorts)}"
                    )
        
        valid_order_types = ["market", "limit"]
        if self.entry_type is not None and self.entry_type not in valid_order_types:
            raise ConfigurationError(f"Invalid entry_type: {self.entry_type}. Must be one of {valid_order_types}")
        if self.exit_type is not None and self.exit_type not in valid_order_types:
            raise ConfigurationError(f"Invalid exit_type: {self.exit_type}. Must be one of {valid_order_types}")

@dataclass
class TrackerInfo:
    status: str
    last_side: Optional[str]
    long_entry_order_ids: List[str]
    short_entry_order_ids: List[str]
    exit_order_id: Optional[str]
    long_stop_loss_price: Optional[float]
    short_stop_loss_price: Optional[float]
    position_first_update: bool
    leverage: float
    margin_mode: Optional[str]
    last_executed_sl_id: Optional[str]
    long_envelopes: List[float] = field(default_factory=list) 
    short_envelopes: List[float] = field(default_factory=list) 
    avg_position_price: Optional[float] = None

@dataclass(frozen=True)
class OrderChange:
    side: str
    percentage: float
    amount: float
    band_price: float
    stop_loss_price: float
    oid: Optional[str] = None

@dataclass(frozen=True)
class TradingStateChanges:
    orders_to_cancel: Set[str]
    orders_to_modify: List[OrderChange]
    orders_to_add: List[OrderChange]
    long_enabled: Optional[bool]
    short_enabled: Optional[bool]

#------------------------------------------------------------------------------
# TRACKER MANAGEMENT
#------------------------------------------------------------------------------

class TrackerFileManager:
    def __init__(self, symbol: str):
        self._symbol = symbol.replace('/', '-')
        self._file_path = Path(__file__).parent / f"tracker_{self._symbol}.json"

    async def load(self) -> TrackerInfo:
        try:
            
            async with aiofiles.open(self._file_path, 'r') as f:
                data = json.loads(await f.read())
                return TrackerInfo(**data)
        except Exception as e:
            raise TrackerError(f"{get_timestamp()} Failed to load tracker file for {self._symbol}: {str(e)}")

    async def update(self, tracker_info: TrackerInfo) -> None:
        try:
            async with aiofiles.open(self._file_path, 'w') as f:
                await f.write(json.dumps(asdict(tracker_info), indent=4))
        except Exception as e:
            raise TrackerError(f"{get_timestamp()} Failed to update tracker file for {self._symbol}: {str(e)}")

    async def ensure_exists(self, params: TradingParameters, margin_mode: Optional[str] = None) -> bool:
        try:
            if not self._file_path.exists():
                tracker_info = TrackerInfo(
                    status=TradingStatus.OK_TO_TRADE.value,
                    last_side=None,
                    long_entry_order_ids=[],
                    short_entry_order_ids=[],
                    exit_order_id=None,
                    long_stop_loss_price=None,
                    short_stop_loss_price=None,
                    position_first_update=True,
                    leverage=params.leverage,
                    margin_mode=margin_mode,
                    last_executed_sl_id=None,
                    long_envelopes=params.envelopes_longs or [],
                    short_envelopes=params.envelopes_shorts or []
                )
                await self.update(tracker_info)
                return True
            return False
        except Exception as e:
            raise TrackerError(f"{get_timestamp()} Failed to create tracker file for {self._symbol}: {str(e)}")

#------------------------------------------------------------------------------
# TECHNICAL ANALYSIS
#------------------------------------------------------------------------------

class IndicatorCalculator: 
    def __init__(
        self,
                 exchange: BitgetFuturesAsync,
                 params: TradingParameters,
        exchange_for_ohlcv: Optional[Any] = None
    ):
        self.exchange = exchange
        self.exchange_for_ohlcv = exchange_for_ohlcv
        self.params = params
        self.period = params.average_period
        self.avg_type = params.average_type
        self.envelopes_longs = params.envelopes_longs 
        self.envelopes_shorts = params.envelopes_shorts 

    async def calculate(self) -> pd.DataFrame:
        try:           
            data = await self._fetch_ohlcv_data()
            data = data.iloc[:-1]
            
            source_price = data['close'] if self.params.average_source == "close" else self._calculate_source_price(data)
            data['average'] = self._calculate_average(source_price, data)
            
            self._calculate_bands(data)
            
            return data
            
        except Exception as e:
            raise IndicatorError(f"{get_timestamp()} Failed to calculate indicators: {str(e)}")

    async def _fetch_ohlcv_data(self) -> pd.DataFrame:
        if self.exchange_for_ohlcv and self.params.ohlcv_source == 'other':
            return await self.exchange_for_ohlcv.fetch_recent_ohlcv(
                self.params.symbol,
                self.params.timeframe
            )
        return await self.exchange.fetch_recent_ohlcv(
            self.params.symbol,
            self.params.timeframe
        )

    def _calculate_source_price(self, data: pd.DataFrame) -> pd.Series:
        source = self.params.average_source.lower()
        match source:
            case "open":
                return data['open']
            case "high":
                return data['high']
            case "low":
                return data['low']
            case "close":
                return data['close']
            case "hl2":
                return (data['high'] + data['low']) / 2
            case "hlc3":
                return (data['high'] + data['low'] + data['close']) / 3
            case "ohlc4":
                return (data['open'] + data['high'] + data['low'] + data['close']) / 4
            case "hlcc4":
                return (data['high'] + data['low'] + data['close'] + data['close']) / 4

    def _calculate_average(self, source_price: pd.Series, data: pd.DataFrame) -> pd.Series:
        match self.params.average_type:
            case 'DCM':
                ta_obj = ta.volatility.DonchianChannel(
                    data['high'], 
                    data['low'], 
                    source_price,
                    window=self.period
                )
                return ta_obj.donchian_channel_mband()
            case 'SMA':
                return ta.trend.sma_indicator(source_price, window=self.period)
            case 'EMA':
                return ta.trend.ema_indicator(source_price, window=self.period)
            case 'WMA':
                return ta.trend.wma_indicator(source_price, window=self.period)
            case _:
                raise ConfigurationError(f"Unknown average type: {self.params.average_type}")

    def _calculate_bands(self, data: pd.DataFrame) -> None:
        if self.params.envelopes_longs:
            for pct in self.params.envelopes_longs:
                data[f'band_low_{pct}'] = data['average'] * (1 - pct)
        
        if self.params.envelopes_shorts:
            for pct in self.params.envelopes_shorts:
                data[f'band_high_{pct}'] = data['average'] * (1 + pct)

#------------------------------------------------------------------------------
# ORDER MANAGEMENT
#------------------------------------------------------------------------------

class OrderId:
    @staticmethod
    def from_percentage(side: str, pct: float, prefix: str = "") -> str:
        pct_str = str(pct).replace('.', 'p')
        return f"{prefix}{side.upper()}_{pct_str}_"
    
    @staticmethod
    def get_percentage(oid: str) -> float:
        return float(oid.split('_')[1].replace('p', '.'))
    
    @staticmethod
    def get_side(oid: str) -> str:
        return oid.split('_')[0].lower()

class OrderStateManager:
    def __init__(self, side: str, percentages: List[float], amounts: List[float]):
        self.side = side
        self.percentages = percentages
        self.amounts = amounts
        
    def determine_changes(self, existing_orders: Set[str], tracker_envelopes: List[float]) -> Tuple[Set[str], List[float], List[float]]:
        existing_pcts = {OrderId.get_percentage(oid) for oid in existing_orders}
        
        to_add = [
            pct for pct in self.percentages 
            if (pct not in tracker_envelopes) or (pct not in existing_pcts)
        ]
        
        to_modify = [
            pct for pct in self.percentages 
            if pct in existing_pcts
        ]
        
        to_cancel = {
            oid for oid in existing_orders 
            if OrderId.get_percentage(oid) not in self.percentages
        }
        
        return to_cancel, to_modify, to_add
    
class OrderManager:
    def __init__(self, exchange: BitgetFuturesAsync, params: TradingParameters, 
                 tracker: Optional[TrackerFileManager] = None, 
                 tracker_info: Optional[TrackerInfo] = None, 
                 verbose: bool = False):
        self.exchange = exchange
        self.params = params
        self.tracker = tracker
        self.tracker_info = tracker_info
        self.verbose = verbose
        self.trigger_price_delta = None
        self.exit_price = None
        self.stop_loss_price_longs = None
        self.stop_loss_price_shorts = None
        self.long_envelope_amounts = []
        self.short_envelope_amounts = []
        self.total_amount_longs = 0
        self.total_amount_shorts = 0
        self.last_executed_tpsl = None
            
    async def initialize(self, tracker_info: TrackerInfo) -> None:
        try:
            self.tracker_info = tracker_info
            tick_size = self.exchange.fetch_min_tick_size(self.params.symbol)
            self.trigger_price_delta = tick_size * self.params.delta_ticks_factor
            await self._fetch_and_close_orders()
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to initialize order manager for {self.params.symbol}: {str(e)}")

    def calculate_envelope_amounts(self, data: pd.DataFrame) -> None:
        try:
            price = data['close'].iloc[-1]
            base_amount = self.exchange.total_balance * self.params.balance_fraction * self.params.leverage / price

            self.long_envelope_amounts, self.total_amount_longs = self._calculate_side_amounts(
                self.params.envelopes_longs, 
                self.params.envelopes_sizing_longs,
                base_amount
            )
            
            self.short_envelope_amounts, self.total_amount_shorts = self._calculate_side_amounts(
                self.params.envelopes_shorts,
                self.params.envelopes_sizing_shorts,
                base_amount
            )

        except Exception as e:
            raise OrderError(
                f"{get_timestamp()} Failed to calculate envelope amounts for {self.params.symbol}: {str(e)}"
            )

    def _calculate_side_amounts(self, envelope_params, sizing_params, base_amount):
        amounts = [base_amount * (sizing_params[i] if sizing_params else 1/len(envelope_params)) 
                   for i in range(len(envelope_params))]
                
        return amounts, sum(amounts)

    async def calculate_exit_prices(self, data: pd.DataFrame, position_is_open: bool) -> None:
        try:
            base_exit_price = data['average'].iloc[-1]
            
            if bool(self.params.envelopes_longs):
                total_size = sum(self.long_envelope_amounts)
                avg_entry_price = sum(
                    data[f'band_low_{pct}'].iloc[-1] * self.long_envelope_amounts[i]
                    for i, pct in enumerate(self.params.envelopes_longs)
                ) / total_size
                
                offset = self.params.exit_offset_pct or 0
                self.exit_price = base_exit_price * (1 - offset)
                
                self.stop_loss_price_longs = avg_entry_price * (1 - self.params.stop_loss_pct)
                if not position_is_open:
                    self.tracker_info.long_stop_loss_price = self.stop_loss_price_longs
                            
            if bool(self.params.envelopes_shorts):
                total_size = sum(self.short_envelope_amounts)
                avg_entry_price = sum(
                    data[f'band_high_{pct}'].iloc[-1] * self.short_envelope_amounts[i]
                    for i, pct in enumerate(self.params.envelopes_shorts)
                ) / total_size
                
                offset = self.params.exit_offset_pct or 0
                self.exit_price = base_exit_price * (1 + offset)
                
                self.stop_loss_price_shorts = avg_entry_price * (1 + self.params.stop_loss_pct)
                if not position_is_open:
                    self.tracker_info.short_stop_loss_price = self.stop_loss_price_shorts
                
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to calculate TPSL prices: {str(e)}")
        
    async def handle_orders(self, data: pd.DataFrame, position: Dict) -> None:
        async with self.tracker_session():
            if position:
                await self._update_position_exits(position)
            changes = (
                self._determine_changes_with_position(data, self.tracker_info, position)
                if position else
                self._determine_changes_without_position(data, self.tracker_info)
            )
            
            await self._execute_order_changes(changes)

    @asynccontextmanager
    async def tracker_session(self):
        try:
            yield self.tracker_info
            await self.tracker.update(self.tracker_info)
        except Exception as e:
                raise TrackerError(f"{get_timestamp()} Failed to manage tracker session: {str(e)}")

    def _determine_changes_with_position(self, data: pd.DataFrame, tracker: TrackerInfo, position: Dict) -> TradingStateChanges:
        orders_to_cancel = set()
        orders_to_modify = []
        
        position_side = position['side']
        
        if position_side == 'long':
            orders_to_cancel.update(tracker.short_entry_order_ids)
            
            if self.params.envelopes_longs:
                long_manager = OrderStateManager(
                    'long', 
                    self.params.envelopes_longs,
                    self.long_envelope_amounts
                )
                to_cancel, to_modify, _ = long_manager.determine_changes(
                    tracker.long_entry_order_ids,
                    tracker.long_envelopes
                )
                orders_to_cancel.update(to_cancel)
                
                for pct in to_modify:
                    oid = next((oid for oid in tracker.long_entry_order_ids 
                              if OrderId.get_percentage(oid) == pct), None)
                    if oid:
                        orders_to_modify.append(OrderChange(
                            side='long',
                            percentage=pct,
                            amount=self.long_envelope_amounts[self.params.envelopes_longs.index(pct)],
                            band_price=data[f'band_low_{pct}'].iloc[-1],
                            stop_loss_price=self.stop_loss_price_longs,
                            oid=oid
                        ))
                    
        else:  
            orders_to_cancel.update(tracker.long_entry_order_ids)
            
            if self.params.envelopes_shorts:
                short_manager = OrderStateManager(
                    'short', 
                    self.params.envelopes_shorts,
                    self.short_envelope_amounts
                )
                to_cancel, to_modify, _ = short_manager.determine_changes(
                    tracker.short_entry_order_ids,
                    tracker.short_envelopes
                )
                orders_to_cancel.update(to_cancel)
                
                for pct in to_modify:
                    oid = next((oid for oid in tracker.short_entry_order_ids 
                              if OrderId.get_percentage(oid) == pct), None)
                    if oid:
                        orders_to_modify.append(OrderChange(
                            side='short',
                            percentage=pct,
                            amount=self.short_envelope_amounts[self.params.envelopes_shorts.index(pct)],
                            band_price=data[f'band_high_{pct}'].iloc[-1],
                            stop_loss_price=self.stop_loss_price_shorts,
                            oid=oid
                        ))

        return TradingStateChanges(
            orders_to_cancel=orders_to_cancel,
            orders_to_modify=orders_to_modify,
            orders_to_add=[],
            long_enabled=bool(self.params.envelopes_longs),
            short_enabled=bool(self.params.envelopes_shorts)
        )

    def _determine_changes_without_position(self, data: pd.DataFrame, tracker: TrackerInfo) -> TradingStateChanges:
        orders_to_cancel = set()
        orders_to_modify = []
        orders_to_add = []

        long_manager = OrderStateManager(
            'long', 
            self.params.envelopes_longs,
            self.long_envelope_amounts
        )
        to_cancel, to_modify, to_add = long_manager.determine_changes(
            tracker.long_entry_order_ids,
            tracker.long_envelopes
        )
        orders_to_cancel.update(to_cancel)
        
        if self.params.envelopes_longs:

            for pct in to_modify:
                oid = next((oid for oid in tracker.long_entry_order_ids 
                           if OrderId.get_percentage(oid) == pct), None)
                if oid:
                    orders_to_modify.append(OrderChange(
                        side='long',
                        percentage=pct,
                        amount=self.long_envelope_amounts[self.params.envelopes_longs.index(pct)],
                        band_price=data[f'band_low_{pct}'].iloc[-1],
                        stop_loss_price=self.stop_loss_price_longs,
                        oid=oid
                    ))

            for pct in to_add:
                orders_to_add.append(OrderChange(
                    side='long',
                    percentage=pct,
                    amount=self.long_envelope_amounts[self.params.envelopes_longs.index(pct)],
                    band_price=data[f'band_low_{pct}'].iloc[-1],
                    stop_loss_price=self.stop_loss_price_longs
                ))

            tracker.long_envelopes = self.params.envelopes_longs

        short_manager = OrderStateManager(
            'short', 
            self.params.envelopes_shorts, 
            self.short_envelope_amounts
        )
        to_cancel, to_modify, to_add = short_manager.determine_changes(
            tracker.short_entry_order_ids,
            tracker.short_envelopes
        )
        orders_to_cancel.update(to_cancel)
        
        if self.params.envelopes_shorts:
            for pct in to_modify:
                oid = next((oid for oid in tracker.short_entry_order_ids 
                           if OrderId.get_percentage(oid) == pct), None)
                if oid:
                    orders_to_modify.append(OrderChange(
                        side='short',
                        percentage=pct,
                        amount=self.short_envelope_amounts[self.params.envelopes_shorts.index(pct)],
                        band_price=data[f'band_high_{pct}'].iloc[-1],
                        stop_loss_price=self.stop_loss_price_shorts,
                        oid=oid
                    ))

            for pct in to_add:
                orders_to_add.append(OrderChange(
                    side='short',
                    percentage=pct,
                    amount=self.short_envelope_amounts[self.params.envelopes_shorts.index(pct)],
                    band_price=data[f'band_high_{pct}'].iloc[-1],
                    stop_loss_price=self.stop_loss_price_shorts
                ))

            tracker.short_envelopes = self.params.envelopes_shorts

        return TradingStateChanges(
            orders_to_cancel=orders_to_cancel,
            orders_to_modify=orders_to_modify,
            orders_to_add=orders_to_add,
            long_enabled=bool(self.params.envelopes_longs),
            short_enabled=bool(self.params.envelopes_shorts)
        )

    async def _execute_order_changes(self, changes: TradingStateChanges) -> None:
        tasks = []
        
        if changes.orders_to_cancel:
            tasks.append(asyncio.create_task(self._cancel_entry_orders(changes.orders_to_cancel)))
            
            self.tracker_info.long_entry_order_ids = [
                oid for oid in self.tracker_info.long_entry_order_ids 
                if oid not in changes.orders_to_cancel
            ]
            self.tracker_info.short_entry_order_ids = [
                oid for oid in self.tracker_info.short_entry_order_ids 
                if oid not in changes.orders_to_cancel
            ]
        
        if changes.orders_to_modify:
            tasks.append(asyncio.create_task(self._modify_entry_orders(changes.orders_to_modify)))
        
        if changes.orders_to_add:
            tasks.append(asyncio.create_task(self._place_entry_orders(changes.orders_to_add)))
        
        if tasks:
            await asyncio.gather(*tasks)

    async def _cancel_entry_orders(self, order_ids: Set[str]) -> None:
        try:
            response = await self.exchange.private_mix_session.fetch_pending_trigger_orders(
                symbol=self.params.symbol,
                plan_type='normal_plan'
            )
            if response.data.entrustedList:
                order_id_map = {
                    order['clientOid']: order['orderId'] 
                    for order in response.data.entrustedList
                }
                order_ids_to_cancel = [
                    order_id_map[oid] for oid in order_ids 
                    if oid in order_id_map
                ]
                if order_ids_to_cancel:
                    await self.exchange.cancel_trigger_orders(order_ids_to_cancel, self.params.symbol)
            
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to cancel orders for {self.params.symbol}: {str(e)}")

    async def _cancel_exit_order(self, client_oid: str) -> None:
        try:
            response = await self.exchange.private_mix_session.fetch_pending_trigger_orders(
                symbol=self.params.symbol,
                plan_type='normal_plan'
            )
            if response.data.entrustedList:
                order_id_map = {
                    order['clientOid']: order['orderId'] 
                    for order in response.data.entrustedList
                }
                if client_oid in order_id_map:
                    await self.exchange.cancel_trigger_orders([order_id_map[client_oid]], self.params.symbol)
            
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to cancel order {client_oid} for {self.params.symbol}: {str(e)}")
        
    async def _modify_entry_orders(self, orders: List[OrderChange]) -> None:
        try:
            for order in orders:
                if order.oid not in (self.tracker_info.long_entry_order_ids + self.tracker_info.short_entry_order_ids):
                    continue
                
                if self.params.entry_type == 'limit':
                    trigger_price = order.band_price + (
                        self.trigger_price_delta * (1 if order.side == 'long' else -1)
                    )
                    await self.exchange.modify_limit_trigger_order(
                        oid=order.oid,
                        symbol=self.params.symbol,
                        execution_price=str(order.band_price),
                        trigger_price=str(trigger_price),
                        size=str(order.amount),
                        stop_loss_price=str(order.stop_loss_price),
                    )
                else:
                    await self.exchange.modify_market_trigger_order(
                        oid=order.oid,
                        symbol=self.params.symbol,
                        trigger_price=str(order.band_price),
                        size=str(order.amount),
                        stop_loss_price=str(order.stop_loss_price),
                    )
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to modify orders for {self.params.symbol}: {str(e)}")

    async def _place_entry_orders(self, orders: List[OrderChange]) -> None:
        try:
            for order in orders:
                percentage_str = str(order.percentage).replace('.', 'p')
                oid = self.exchange.generate_order_id(
                    f"{'LONG' if order.side == 'long' else 'SHORT'}_{percentage_str}_"
                )
                
                if self.params.entry_type == 'limit':
                    trigger_price = order.band_price + (
                        self.trigger_price_delta * (1 if order.side == 'long' else -1)
                    )
                    await self.exchange.place_limit_trigger_order(
                        oid=oid,
                        symbol=self.params.symbol,
                        side='buy' if order.side == 'long' else 'sell',
                        size=str(order.amount),
                        execution_price=str(order.band_price),
                        trigger_price=str(trigger_price),
                        stop_loss_price=str(order.stop_loss_price),
                    )
                else:
                    await self.exchange.place_market_trigger_order(
                        oid=oid,
                        symbol=self.params.symbol,
                        side='buy' if order.side == 'long' else 'sell',
                        size=str(order.amount),
                        trigger_price=str(order.band_price),
                        stop_loss_price=str(order.stop_loss_price),
                    )
                
                if order.side == 'long':
                    self.tracker_info.long_entry_order_ids.append(oid)
                else:
                    self.tracker_info.short_entry_order_ids.append(oid)
                
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to place orders for {self.params.symbol}: {str(e)}")

    async def _fetch_and_close_orders(self) -> None:
        try:
            tasks = [
                asyncio.create_task(self.exchange.fetch_last_executed_tpsl_order(self.params.symbol)),
                asyncio.create_task(self.exchange.fetch_open_trigger_orders(self.params.symbol)),
                asyncio.create_task(self.exchange.cancel_open_sl_orders(self.params.symbol))
            ]
            
            self.last_executed_tpsl, open_orders, _ = await asyncio.gather(*tasks)
            
            self.tracker_info.long_entry_order_ids = [
                oid for oid in self.tracker_info.long_entry_order_ids 
                if oid in open_orders
            ]
            
            self.tracker_info.short_entry_order_ids = [
                oid for oid in self.tracker_info.short_entry_order_ids 
                if oid in open_orders
            ]
                    
        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to fetch and close orders for {self.params.symbol}: {str(e)}")

    async def _update_position_exits(self, position: Dict) -> None:
        try:
            position_side = position['side']
            position_amount = position['contracts']
            current_avg_price = float(position['info']['openPriceAvg'])

            if self.tracker_info.avg_position_price == current_avg_price:
                await self.exchange.modify_market_trigger_order(
                    oid=self.tracker_info.exit_order_id,
                    symbol=self.params.symbol,
                    trigger_price=str(self.exit_price),
                    size=position_amount,
                )
                return

            sl_price = self.exchange.price_to_precision(self.params.symbol, current_avg_price * (
                (1 - self.params.stop_loss_after_entry_pct) if position_side == 'long' 
                else (1 + self.params.stop_loss_after_entry_pct)
            ))
            
            if position_side == 'long':
                self.tracker_info.long_stop_loss_price = sl_price
            else:
                self.tracker_info.short_stop_loss_price = sl_price
                
            self.tracker_info.avg_position_price = current_avg_price

            if self.tracker_info.position_first_update:
                await self.exchange.place_position_stop_loss(
                    symbol=self.params.symbol,
                    hold_side=position_side,
                    price=str(sl_price)
                )
                
                oid = self.exchange.generate_order_id(f"EXIT_{position_side}_")
                order = await self.exchange.place_exit_order(
                    oid=oid,
                    symbol=self.params.symbol,
                    side="buy" if position_side == 'long' else "sell",
                    size=position_amount,
                    trigger_price=str(self.exit_price)
                )
                self.tracker_info.exit_order_id = oid
                self.tracker_info.position_first_update = False

            else:
                await self.exchange.place_position_stop_loss(
                    symbol=self.params.symbol,
                    hold_side=position_side,
                    price=str(sl_price)
                )
                await self.exchange.modify_market_trigger_order(
                    oid=self.tracker_info.exit_order_id,
                    symbol=self.params.symbol,
                    trigger_price=str(self.exit_price),
                    size=position_amount,
                )

        except Exception as e:
            raise OrderError(f"{get_timestamp()} Failed to update position exits for {self.params.symbol}: {str(e)}")

#------------------------------------------------------------------------------
# TRADER CLASS
#------------------------------------------------------------------------------

class SymbolTrader:
    def __init__(self, params: Dict[str, Any], margin_mode: Optional[str] = None, verbose: bool = False):
        self.params = TradingParameters(**params)
        self.margin_mode = margin_mode
        self.verbose = verbose
        self.tracker = TrackerFileManager(self.params.symbol)
        self.tracker_info = None
        self.position = None
        self.data = None
        self.order_manager = None
        self.is_first_run = False

    async def initialize(self, exchange: BitgetFuturesAsync, exchange_for_ohlcv: Optional[Any] = None) -> None:
        try:
            self.exchange = exchange
            self.exchange_for_ohlcv = exchange_for_ohlcv
            self.is_first_run = await self.tracker.ensure_exists(self.params, self.margin_mode)
            self.tracker_info = await self.tracker.load()
            await self._set_position_mode()
            self.order_manager = OrderManager(self.exchange, self.params, self.tracker, self.tracker_info, self.verbose)
            await self.order_manager.initialize(self.tracker_info)
            
        except Exception as e:
            raise TradingError(f"{get_timestamp()} Failed to initialize {self.params.symbol}: {str(e)}")

    async def run(self, positions: List[Dict[str, Any]]) -> None:
        try:
            self.data = await self._calculate_indicators()
            if await self._check_stop_loss():
                await self._check_position(positions)
                self.order_manager.calculate_envelope_amounts(self.data)
                await self.order_manager.calculate_exit_prices(self.data, position_is_open=bool(self.position))
                await self.order_manager.handle_orders(self.data, self.position)
 
        except (IndicatorError, OrderError, PositionError, TrackerError, TradingError):
            raise
        except Exception as e:
            raise TradingError(f"{get_timestamp()} Failed to verify and place orders for {self.params.symbol}: {str(e)}")

    async def _set_position_mode(self) -> None:
        try:
            if self.is_first_run:
                await self.exchange.set_hedge_mode()
                await self.exchange.set_margin_mode(self.params.symbol, self.margin_mode)
                await self.exchange.set_leverage(symbol=self.params.symbol, leverage=self.params.leverage)
                self.tracker_info.leverage = self.params.leverage
                self.tracker_info.margin_mode = self.margin_mode
                await self.tracker.update(self.tracker_info)
                return
            
            if self.tracker_info.margin_mode != self.margin_mode:
                await self.exchange.set_margin_mode(self.params.symbol, self.margin_mode)
                await self.exchange.set_leverage(symbol=self.params.symbol, leverage=self.params.leverage)
                self.tracker_info.leverage = self.params.leverage                    
                self.tracker_info.margin_mode = self.margin_mode
                await self.tracker.update(self.tracker_info)
                
            if self.tracker_info.leverage != self.params.leverage:
                await self.exchange.set_leverage(symbol=self.params.symbol, leverage=self.params.leverage)
                self.tracker_info.leverage = self.params.leverage
                await self.tracker.update(self.tracker_info)
                            
        except Exception as e:
            raise TradingError(f"{get_timestamp()} Failed to set position mode for {self.params.symbol}: {str(e)}")
        
    async def _calculate_indicators(self) -> pd.DataFrame:
        try:
            indicator_calc = IndicatorCalculator(
                self.exchange, 
                self.params,
                exchange_for_ohlcv=self.exchange_for_ohlcv
            )
            self.data = await indicator_calc.calculate()
            return self.data
        except Exception as e:
            raise IndicatorError(f"{get_timestamp()} Failed to calculate indicators for {self.params.symbol}: {str(e)}")
        
    async def _check_stop_loss(self) -> bool:
        try:
            if self.position:
                return True
            
            if self.is_first_run:
                if self.order_manager.last_executed_tpsl:
                    self.tracker_info.last_executed_sl_id = self.order_manager.last_executed_tpsl.executeOrderId
                    await self.tracker.update(self.tracker_info)
                return True

            if (self.order_manager.last_executed_tpsl and
                self.order_manager.last_executed_tpsl.executeOrderId != self.tracker_info.last_executed_sl_id):
                self.tracker_info.status = TradingStatus.STOP_LOSS_TRIGGERED.value
                self.tracker_info.last_side = self.order_manager.last_executed_tpsl.posSide
                self.tracker_info.last_executed_sl_id = self.order_manager.last_executed_tpsl.executeOrderId
                
                if self.tracker_info.exit_order_id:
                    await self.order_manager._cancel_exit_order(self.tracker_info.exit_order_id)
                
                await self.tracker.update(self.tracker_info)
                return False

            if self.tracker_info.status == TradingStatus.STOP_LOSS_TRIGGERED.value:
                last_price = self.data['close'].iloc[-1]
                resume_price = self.data['average'].iloc[-1]
                
                should_resume = (
                    (self.tracker_info.last_side == PositionSide.LONG.value and last_price >= resume_price) or
                    (self.tracker_info.last_side == PositionSide.SHORT.value and last_price <= resume_price)
                )
                
                if should_resume:
                    self.tracker_info.status = TradingStatus.OK_TO_TRADE.value
                    await self.tracker.update(self.tracker_info)
                    return True

                if self.tracker_info.short_entry_order_ids:
                    await self.order_manager._cancel_entry_orders(set(self.tracker_info.short_entry_order_ids))
                    self.tracker_info.short_entry_order_ids = []

                if self.tracker_info.long_entry_order_ids:
                    await self.order_manager._cancel_entry_orders(set(self.tracker_info.long_entry_order_ids))
                    self.tracker_info.long_entry_order_ids = []
                                                        
                return False
                    
            return True
            
        except Exception as e:
            raise TradingError(f"{get_timestamp()} Failed to check stop loss for {self.params.symbol}: {str(e)}")

    async def _check_position(self, position_data) -> None:
        try:
            if not position_data:
                self.position = None
                self.tracker_info.avg_position_price = None
                self.tracker_info.position_first_update = True
                return

            if not isinstance(position_data, list):
                self.position = position_data
                self.tracker_info.last_side = self.position['side']
                return

            elif len(position_data) > 1:
                sorted_positions = sorted(position_data, key=lambda x: x['timestamp'], reverse=True)
                self.position = sorted_positions[0]
                self.tracker_info.last_side = self.position['side']
                
                for pos in sorted_positions[1:]:
                    await self.exchange.flash_close_position(
                        pos['symbol'], 
                        side=pos['side']
                    )
                    verbose_print(self.params.symbol, f"Multiple positions detected, closed {pos['side']}", self.verbose)
            else:
                self.position = position_data[0]
                self.tracker_info.last_side = self.position['side']
                
        except Exception as e:
            raise PositionError(f"{get_timestamp()} Failed to check position for {self.params.symbol}: {str(e)}")

#------------------------------------------------------------------------------
# MULTI COIN HANDLER
#------------------------------------------------------------------------------

@asynccontextmanager
async def null_semaphore():
    yield

class EnvelopeTrader:
    def __init__(
        self, 
        params: Dict[str, Dict[str, Any]],
        exchange: Optional[BitgetFuturesAsync] = None,
        exchange_for_ohlcv: Optional[Any] = None,
        margin_mode: str = 'isolated',
        verbose: bool = False,
        max_concurrent: Optional[int] = None,
    ):
        self.original_params = params
        self.exchange = exchange
        self.exchange_for_ohlcv = exchange_for_ohlcv
        self.margin_mode = margin_mode
        self.verbose = verbose
        self.symbol_traders = {}
        self.params = {} 
        self.semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent else null_semaphore()

    async def run(self) -> None:
        try:
            async with self.semaphore:
                
                self.params = await filter_active_timeframes(self.original_params)
                
                tasks = [
                    asyncio.create_task(self.exchange.fetch_open_positions([symbol for symbol in self.params.keys()])),
                ]
                for symbol, coin_params in self.params.items():
                    coin_params['symbol'] = symbol
                    coin_params['verbose'] = self.verbose
                    self.symbol_traders[symbol] = SymbolTrader(coin_params, self.margin_mode, self.verbose)
                    tasks.append(asyncio.create_task(
                        self.symbol_traders[symbol].initialize(
                            self.exchange, 
                            exchange_for_ohlcv=self.exchange_for_ohlcv
                        )
                    ))
                    
                positions, *_ = await asyncio.gather(*tasks)
                
                tasks = []
                for symbol, trader in self.symbol_traders.items():
                    symbol_positions = [p for p in positions if p['symbol'] == symbol]
                    task = asyncio.create_task(trader.run(symbol_positions))
                    tasks.append(task)
                
                await asyncio.gather(*tasks)

                await self._cleanup_removed_symbols()               
                
        except IndicatorError as e:
            print(f"{get_timestamp()} Indicator calculation error: {str(e)}")
            raise
        except OrderError as e:
            print(f"{get_timestamp()} Order execution error: {str(e)}")
            raise
        except PositionError as e:
            print(f"{get_timestamp()} Position management error: {str(e)}")
            raise
        except TrackerError as e:
            print(f"{get_timestamp()} Tracker file error: {str(e)}")
            raise
        except TradingError as e:
            print(f"{get_timestamp()} Trading error: {str(e)}")
            raise
        except Exception as e:
            print(f"{get_timestamp()} Unknown error in trading cycle: {str(e)}")
            raise
        finally:
            await self.exchange.close()
            if self.exchange_for_ohlcv:
                await self.exchange_for_ohlcv.close()

    async def _cleanup_removed_symbols(self) -> None:
            try:
                tracker_files = list(Path(__file__).parent.glob("tracker_*.json"))
                all_symbols = set()
                for timeframe_symbols in self.original_params.values():
                    all_symbols.update(timeframe_symbols.keys())
                
                cleanup_tasks = []
                
                for tracker_file in tracker_files:
                    symbol = tracker_file.stem.replace('tracker_', '').replace('-', '/')
                    
                    if symbol not in all_symbols:
                        async with aiofiles.open(tracker_file, 'r') as f:
                            tracker_data = json.loads(await f.read())
                    
                        order_manager = OrderManager(
                            exchange=self.exchange,
                            params=TradingParameters(symbol=symbol),
                            verbose=self.verbose
                        )
                
                        entry_order_ids = set(
                            tracker_data.get('long_entry_order_ids', []) + 
                            tracker_data.get('short_entry_order_ids', [])
                        )
                        
                        if tracker_data.get('exit_order_id'):
                            cleanup_tasks.append(
                                asyncio.create_task(
                                    order_manager._cancel_exit_order(tracker_data['exit_order_id'])
                                )
                            )
                        
                        if entry_order_ids:
                            cleanup_tasks.append(
                                asyncio.create_task(
                                    order_manager._cancel_entry_orders(entry_order_ids)
                                )
                            )

                        try:
                            await self.exchange.flash_close_position(symbol, 'long')
                        except:
                            pass
                            
                        try:
                            await self.exchange.flash_close_position(symbol, 'short')
                        except:
                            pass
                        
                
                        os.remove(tracker_file)
                    
                if cleanup_tasks:
                    await asyncio.gather(*cleanup_tasks)
                    
            except Exception as e:
                raise TradingError(f"Failed to clean up removed symbols: {str(e)}") 