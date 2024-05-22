CREATE TABLE kafka (
    event_type int,
    person ROW<
        id  BIGINT,
        name  VARCHAR,
        emailAddress  VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        dateTime TIMESTAMP(3),
        extra  VARCHAR>,
    auction ROW<
        id  BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        dateTime  TIMESTAMP(3),
        expires  TIMESTAMP(3),
        seller  BIGINT,
        category  BIGINT,
        extra  VARCHAR>,
    bid ROW<
        auction  BIGINT,
        bidder  BIGINT,
        price  BIGINT,
        channel  VARCHAR,
        url  VARCHAR,
        dateTime  TIMESTAMP(3),
        extra  VARCHAR>,
    dateTime AS
        CASE
            WHEN event_type = 0 THEN person.dateTime
            WHEN event_type = 1 THEN auction.dateTime
            ELSE bid.dateTime
        END
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'nexmark',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'round-robin',
    'format' = 'json'
);