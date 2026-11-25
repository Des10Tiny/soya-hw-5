CREATE TABLE IF NOT EXISTS default.kafka_movie_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4,
        'LIKED' = 5,
        'SEARCHED' = 6
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8(
        'MOBILE' = 1,
        'DESKTOP' = 2,
        'TV' = 3,
        'TABLET' = 4
    ),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker1:29092,broker2:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse_movie_events_consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    input_format_avro_null_as_default = 1;

CREATE TABLE IF NOT EXISTS default.movie_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4,
        'LIKED' = 5,
        'SEARCHED' = 6
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8(
        'MOBILE' = 1,
        'DESKTOP' = 2,
        'TV' = 3,
        'TABLET' = 4
    ),
    session_id String,
    progress_seconds Nullable(Int32),
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, timestamp, event_type, session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.movie_events_mv TO default.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds,
    now() AS inserted_at
FROM default.kafka_movie_events;

CREATE TABLE IF NOT EXISTS default.daily_aggregates
(
    metric_date Date,
    dau UInt64,
    avg_watch_time Float64,
    views_started UInt64,
    views_finished UInt64,
    conversion_rate Float64,
    retention_d1 Float64,
    retention_d7 Float64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS default.top_movies_daily
(
    metric_date Date,
    movie_id String,
    views_count UInt64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, views_count, movie_id);

CREATE TABLE IF NOT EXISTS default.cohort_retention_daily
(
    cohort_date Date,
    day_offset UInt8,
    cohort_size UInt64,
    active_users UInt64,
    retention_rate Float64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_offset);
