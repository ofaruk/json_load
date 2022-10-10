WITH t AS
(SELECT
  instrument_id,
  event_ts,
  TIMESTAMP_TRUNC(event_ts_5s, SECOND) as event_ts_5s,
  TIMESTAMP_TRUNC(event_ts_1m, SECOND) as event_ts_1m,
  TIMESTAMP_TRUNC(event_ts_30m, SECOND) as event_ts_30m,
  TIMESTAMP_TRUNC(event_ts_60m, SECOND) as event_ts_60m
  FROM (
    SELECT
      instrument_id,
      TIMESTAMP_TRUNC(event_timestamp, SECOND) event_ts,
      last_value(event_timestamp)
        OVER (ORDER BY UNIX_MICROS(event_timestamp) RANGE BETWEEN CURRENT ROW AND 5000000 FOLLOWING) as event_ts_5s,
      last_value(event_timestamp)
        OVER (ORDER BY UNIX_MICROS(event_timestamp) RANGE BETWEEN CURRENT ROW AND 60000000 FOLLOWING) as event_ts_1m,
      last_value(event_timestamp)
        OVER (ORDER BY UNIX_MICROS(event_timestamp) RANGE BETWEEN CURRENT ROW AND 1800000000 FOLLOWING) as event_ts_30m,
      last_value(event_timestamp)
        OVER (ORDER BY UNIX_MICROS(event_timestamp) RANGE BETWEEN CURRENT ROW AND 3600000000 FOLLOWING) as event_ts_60m
    FROM `engineering-assets-319815.poc_ds.tradedata`
    ORDER BY event_timestamp
  )
)
SELECT
  t.instrument_id,
  t.event_ts,
  v.when_timestamp,
  MAX(v.gamma) as gamma_5s,
  MAX(v.vega) as vega_5s,
  MAX(v.theta) as theta_5s,
  MAX(v1.gamma) as gamma_1m,
  MAX(v1.vega) as vega_1m,
  MAX(v1.theta) as theta_1m,
  MAX(v2.gamma) as gamma_30m,
  MAX(v2.vega) as vega_30m,
  MAX(v2.theta) as theta_30m,
  MAX(v3.gamma) as gamma_60m,
  MAX(v3.vega) as vega_60m,
  MAX(v3.theta) as theta_60m
FROM t
LEFT JOIN `engineering-assets-319815.poc_ds.valuedata` v ON v.instrument_id=t.instrument_id AND v.when_timestamp=t.event_ts_5s
LEFT JOIN `engineering-assets-319815.poc_ds.valuedata` v1 ON v1.instrument_id=t.instrument_id AND v1.when_timestamp=t.event_ts_1m
LEFT JOIN `engineering-assets-319815.poc_ds.valuedata` v2 ON v2.instrument_id=t.instrument_id AND v2.when_timestamp=t.event_ts_30m
LEFT JOIN `engineering-assets-319815.poc_ds.valuedata` v3 ON v3.instrument_id=t.instrument_id AND v3.when_timestamp=t.event_ts_60m
GROUP BY 1, 2, 3
