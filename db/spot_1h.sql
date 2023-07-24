CREATE TABLE spot_1h
(
    id                                      BIGINT,
    symbol                                  VARCHAR(20) NOT NULL,
    open_time                               TIMESTAMP NOT NULL,
    open_price                              DECIMAL(24,8),
    high_price                              DECIMAL(24,8),
    low_price                               DECIMAL(24,8),
    close_price                             DECIMAL(24,8),
    volume                                  DECIMAL(24,8),
    close_time                              TIMESTAMP,
    quote_volume                            DECIMAL(24,8),
    trades                                  INTEGER,
    taker_buy_volume                        DECIMAL(24,8),
    taker_buy_quote_volume                  DECIMAL(24,8),

    PRIMARY KEY (id),
    UNIQUE (symbol, open_time)
);
CREATE INDEX idx_spot_1h_symbol_open_time ON spot_1h(symbol, open_time);