CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_trusted (
    user       STRING,
    timestamp  BIGINT,
    x          FLOAT,
    y          FLOAT,
    z          FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Trusted Zone/accelerometer/'
TBLPROPERTIES ('has_encrypted_data'='false');
