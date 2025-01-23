from datetime import datetime
from typing import Dict, Any

def should_execute_timeframe(timeframe: str) -> bool:
    now = datetime.now()
    
    if timeframe.endswith('m'):
        minutes = int(timeframe[:-1])
        if minutes > 240: 
            raise ValueError(f"Timeframe {timeframe} exceeds maximum allowed (4h)")
        
        minutes_from_hour_start = now.minute
        return minutes_from_hour_start % minutes == 0
        
    elif timeframe.endswith('h'):
        hours = int(timeframe[:-1])
        if hours > 4:
            raise ValueError(f"Timeframe {timeframe} exceeds maximum allowed (4h)")

        hours_from_midnight = now.hour
        return now.minute == 0 and hours_from_midnight % hours == 0
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

async def filter_active_timeframes(params: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result = {}
    for timeframe, symbols in params.items():
        if should_execute_timeframe(timeframe): 
            for symbol, config in symbols.items():
                result[symbol] = {**config, 'timeframe': timeframe}
    return result