import yfinance as yf
europe = yf.MarketSummary("EUROPE")

par = europe["^N100"]

par.name
par.region
par.market
par.quote_type
par.type_display
par.symbol
        
# Market data
par.price
par.change
par.change_percent
par.previous_close
par.market_time
par.market_state
par.price_hint
        
# Exchange information
par.exchange
par.full_exchange_name
par.timezone
par.timezone_short
par.gmt_offset
par.exchange_delay
        
# Quote information
par.quote_source
par.source_interval
        
# Trading properties
par.triggerable
par.tradeable
par.crypto_tradeable
par.has_pre_post_market
par.first_trade_date
        
# Additional properties
par.esg_populated
par.price_alert_confidence