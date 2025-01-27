# Coin Parameters Documentation


## Basic Parameters

| Parameter | Type | Default | Required | Description |
|-----------|------|---------|----------|-------------|
| `symbol` | str | None | Yes | Trading pair symbol (e.g., 'BTC/USDT:USDT') |
| `balance_fraction` | float | None | Yes | Fraction of total balance to use (0.0-1.0) |
| `leverage` | int | None | Yes | Trading leverage to use |
| `average_type` | str | None | Yes | Type of moving average ('DCM', 'SMA', 'EMA', 'WMA') |
| `average_period` | int | None | Yes | Period for average calculation |
| `average_source` | str | "close" | No | Price source for average ("open", "low", "high", "close", "hl2", "hlc3", "ohlc4", "hlcc4") |
| `stop_loss_pct` | float | None | Yes | Stop loss percentage (0.0-1.0) |
| `stop_loss_after_entry_pct` | float | stop_loss_pct | No | Stop loss after position is opened |


## Envelope Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `envelopes_longs` | List[float] | None | List of envelope percentages for longs |
| `envelopes_shorts` | List[float] | None | List of envelope percentages for shorts |
| `envelopes_sizing_longs` | List[float] | None | Position size multiplier for long envelopes |
| `envelopes_sizing_shorts` | List[float] | None | Position size multiplier for short envelopes |


## Order Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exit_type` | str | "market" | Order type for exits ("market" or "limit") |
| `delta_ticks_factor` | int | 5 | Min tick size factor between trigger and executiuon prices for entry trigger limit orders  |


## Advanced Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `verbose` | bool | False | Enable detailed logging |