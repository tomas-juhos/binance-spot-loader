CREATE TABLE latest_spot_1h
(
    symbol                                  VARCHAR(20),
    id                                      BIGINT,
    open_time                               TIMESTAMP,
    source                                  VARCHAR(20),

    PRIMARY KEY(symbol),
    FOREIGN KEY(id) REFERENCES spot_1h(id)
);