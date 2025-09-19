-- Create database and tables for ClickHouse testing

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS testdb;

USE testdb;

-- Create events table (typical ClickHouse use case)
CREATE TABLE IF NOT EXISTS events
(
    event_id UUID DEFAULT generateUUIDv4(),
    event_time DateTime DEFAULT now(),
    event_date Date DEFAULT toDate(event_time),
    user_id UInt32,
    event_type String,
    event_name String,
    properties String,  -- JSON string for flexible properties
    session_id String,
    ip_address IPv4,
    user_agent String,
    country String,
    city String,
    device_type String,
    browser String,
    os String,
    referrer String,
    duration_ms UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_time)
SETTINGS index_granularity = 8192;

-- Create metrics table for time-series data
CREATE TABLE IF NOT EXISTS metrics
(
    metric_time DateTime,
    metric_date Date DEFAULT toDate(metric_time),
    metric_name String,
    metric_value Float64,
    tags Array(String),
    host String,
    service String,
    environment String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_name, metric_time)
SETTINGS index_granularity = 8192;

-- Create aggregated daily stats table
CREATE TABLE IF NOT EXISTS daily_stats
(
    stat_date Date,
    total_events UInt64,
    unique_users UInt32,
    unique_sessions UInt32,
    avg_duration_ms Float64,
    total_page_views UInt64,
    total_clicks UInt64,
    bounce_rate Float32,
    conversion_rate Float32
)
ENGINE = MergeTree()
ORDER BY stat_date
SETTINGS index_granularity = 8192;

-- Create user profiles table
CREATE TABLE IF NOT EXISTS user_profiles
(
    user_id UInt32,
    username String,
    email String,
    created_date Date,
    last_seen DateTime,
    total_events UInt64,
    total_sessions UInt32,
    country String,
    preferred_language String,
    user_segment String
)
ENGINE = MergeTree()
ORDER BY user_id
SETTINGS index_granularity = 8192;

-- Insert sample events data
INSERT INTO events (event_time, user_id, event_type, event_name, session_id, ip_address, country, city, device_type, browser, os, duration_ms)
SELECT
    now() - INTERVAL number HOUR as event_time,
    rand() % 1000 + 1 as user_id,
    arrayElement(['page_view', 'click', 'scroll', 'form_submit', 'purchase'], rand() % 5 + 1) as event_type,
    concat('event_', toString(number)) as event_name,
    concat('session_', toString(rand() % 100)) as session_id,
    toIPv4(concat(toString(rand() % 256), '.', toString(rand() % 256), '.', toString(rand() % 256), '.', toString(rand() % 256))) as ip_address,
    arrayElement(['US', 'UK', 'DE', 'FR', 'JP', 'CN', 'IN', 'BR', 'CA', 'AU'], rand() % 10 + 1) as country,
    arrayElement(['New York', 'London', 'Berlin', 'Paris', 'Tokyo', 'Beijing', 'Mumbai', 'Sao Paulo', 'Toronto', 'Sydney'], rand() % 10 + 1) as city,
    arrayElement(['desktop', 'mobile', 'tablet'], rand() % 3 + 1) as device_type,
    arrayElement(['Chrome', 'Firefox', 'Safari', 'Edge'], rand() % 4 + 1) as browser,
    arrayElement(['Windows', 'macOS', 'Linux', 'iOS', 'Android'], rand() % 5 + 1) as os,
    rand() % 10000 as duration_ms
FROM system.numbers LIMIT 10000;

-- Insert sample metrics data
INSERT INTO metrics (metric_time, metric_name, metric_value, host, service, environment)
SELECT
    now() - INTERVAL number MINUTE as metric_time,
    arrayElement(['cpu_usage', 'memory_usage', 'disk_io', 'network_throughput', 'request_count', 'response_time'], rand() % 6 + 1) as metric_name,
    rand() % 100 + rand() / 100000000 as metric_value,
    concat('host-', toString(rand() % 10 + 1)) as host,
    arrayElement(['api', 'web', 'database', 'cache', 'queue'], rand() % 5 + 1) as service,
    arrayElement(['production', 'staging', 'development'], rand() % 3 + 1) as environment
FROM system.numbers LIMIT 5000;

-- Insert sample user profiles
INSERT INTO user_profiles (user_id, username, email, created_date, last_seen, total_events, total_sessions, country, preferred_language, user_segment)
SELECT
    number + 1 as user_id,
    concat('user_', toString(number + 1)) as username,
    concat('user', toString(number + 1), '@example.com') as email,
    today() - rand() % 365 as created_date,
    now() - INTERVAL (rand() % 72) HOUR as last_seen,
    rand() % 1000 + 1 as total_events,
    rand() % 100 + 1 as total_sessions,
    arrayElement(['US', 'UK', 'DE', 'FR', 'JP', 'CN', 'IN', 'BR', 'CA', 'AU'], rand() % 10 + 1) as country,
    arrayElement(['en', 'es', 'fr', 'de', 'ja', 'zh', 'pt'], rand() % 7 + 1) as preferred_language,
    arrayElement(['free', 'premium', 'enterprise'], rand() % 3 + 1) as user_segment
FROM system.numbers LIMIT 1000;

-- Insert aggregated daily stats
INSERT INTO daily_stats (stat_date, total_events, unique_users, unique_sessions, avg_duration_ms, total_page_views, total_clicks, bounce_rate, conversion_rate)
SELECT
    today() - number as stat_date,
    rand() % 100000 + 50000 as total_events,
    rand() % 10000 + 5000 as unique_users,
    rand() % 15000 + 7000 as unique_sessions,
    rand() % 5000 + 1000 as avg_duration_ms,
    rand() % 50000 + 20000 as total_page_views,
    rand() % 30000 + 10000 as total_clicks,
    rand() % 40 + 10 as bounce_rate,
    rand() % 10 + 1 as conversion_rate
FROM system.numbers LIMIT 30;

-- Create materialized views for common queries
CREATE MATERIALIZED VIEW IF NOT EXISTS events_by_hour
ENGINE = AggregatingMergeTree()
ORDER BY (event_hour, event_type)
AS
SELECT
    toStartOfHour(event_time) as event_hour,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users,
    avg(duration_ms) as avg_duration
FROM events
GROUP BY event_hour, event_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_activity_summary
ENGINE = AggregatingMergeTree()
ORDER BY user_id
AS
SELECT
    user_id,
    count() as total_events,
    uniq(event_date) as active_days,
    uniq(session_id) as total_sessions,
    max(event_time) as last_activity,
    min(event_time) as first_activity
FROM events
GROUP BY user_id;