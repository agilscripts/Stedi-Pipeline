CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user       STRING,
    timestamp  BIGINT,
    x          FLOAT,
    y          FLOAT,
    z          FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Landing Zone/accelerometer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
