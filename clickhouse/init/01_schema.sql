CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.user_events
(
    event_time       DateTime,
    user_id          String,
    gender           LowCardinality(String),
    age              UInt8,
    city             LowCardinality(String),
    country          LowCardinality(String),
    registered_date  DateTime,
    raw              String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id);

CREATE TABLE IF NOT EXISTS analytics.user_events_hourly
(
    hour         DateTime,
    country      LowCardinality(String),
    gender       LowCardinality(String),
    event_count  AggregateFunction(count, UInt64),
    avg_age      AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, country, gender);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_user_events_hourly
TO analytics.user_events_hourly
AS
SELECT
    toStartOfHour(event_time) AS hour,
    country,
    gender,
    countState()              AS event_count,
    avgState(toFloat64(age))  AS avg_age
FROM analytics.user_events
GROUP BY hour, country, gender;

CREATE TABLE IF NOT EXISTS analytics.user_events_daily
(
    day          Date,
    country      LowCardinality(String),
    gender       LowCardinality(String),
    event_count  AggregateFunction(count, UInt64),
    avg_age      AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, country, gender);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_user_events_daily
TO analytics.user_events_daily
AS
SELECT
    toDate(event_time)        AS day,
    country,
    gender,
    countState()              AS event_count,
    avgState(toFloat64(age))  AS avg_age
FROM analytics.user_events
GROUP BY day, country, gender;

CREATE OR REPLACE VIEW analytics.v_hourly AS
SELECT
    hour,
    country,
    gender,
    countMerge(event_count) AS events,
    avgMerge(avg_age)       AS avg_age
FROM analytics.user_events_hourly
GROUP BY hour, country, gender;

CREATE OR REPLACE VIEW analytics.v_daily AS
SELECT
    day,
    country,
    gender,
    countMerge(event_count) AS events,
    avgMerge(avg_age)       AS avg_age
FROM analytics.user_events_daily
GROUP BY day, country, gender;

CREATE OR REPLACE VIEW analytics.v_events AS
SELECT
    event_time,
    user_id,
    gender,
    age,
    city,
    country,
    registered_date
FROM analytics.user_events;