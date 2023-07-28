CREATE TABLE spot_1h_latest
(
    symbol                                  VARCHAR(20),
    id                                      BIGINT,
    latest_close                            TIMESTAMP,
    active                                  BOOLEAN,
    source                                  VARCHAR(20),

    PRIMARY KEY(symbol)
);