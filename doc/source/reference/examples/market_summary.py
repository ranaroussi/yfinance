import yfinance as yf

EUROPE = yf.Summary("EUROPE")
PARIS = EUROPE["^N100"]

PARIS.name
PARIS.region
PARIS.market
PARIS.quote_type
PARIS.type_display
PARIS.symbol
        
# Market data
PARIS.price
PARIS.change
PARIS.change_percent
PARIS.previous_close
PARIS.market_time
PARIS.market_state
PARIS.price_hint
        
# Exchange information
PARIS.exchange
PARIS.full_exchange_name
PARIS.timezone
PARIS.timezone_short
PARIS.gmt_offset
PARIS.exchange_delay
        
# Quote information
PARIS.quote_source
PARIS.source_interval
        
# Trading properties
PARIS.triggerable
PARIS.tradeable
PARIS.crypto_tradeable
PARIS.has_pre_post_market
PARIS.first_trade_date
        
# Additional properties
PARIS.esg_populated
PARIS.price_alert_confidence