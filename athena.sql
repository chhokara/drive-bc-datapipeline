CREATE DATABASE IF NOT EXISTS drivebc;

DROP TABLE IF EXISTS event;

CREATE EXTERNAL TABLE IF NOT EXISTS event (
    id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    severity STRING,
    event_subtype STRING,
    road STRING,
    area STRING
)
STORED AS PARQUET
LOCATION 's3://<bucket-path>'
tblproperties ("parquet.compress"="SNAPPY");

SELECT COUNT(id) as count, severity
FROM event
GROUP BY severity;

SELECT COUNT(id) as count, event_subtype
FROM event
GROUP BY event_subtype;

SELECT COUNT(id) as count, MONTH(event_timestamp) as month, YEAR(event_timestamp) as YEAR
FROM event
GROUP BY MONTH(event_timestamp), YEAR(event_timestamp)
ORDER BY YEAR(event_timestamp), MONTH(event_timestamp);
