CREATE EXTERNAL TABLE IF NOT EXISTS stedi.machine_learning_curated (
    sensorreadingtime  BIGINT,
    serialnumber       STRING,
    distancefromobject INT,
    user               STRING,
    timestamp          BIGINT,
    x                  FLOAT,
    y                  FLOAT,
    z                  FLOAT
    -- (Optionally add more fields if you included them in the final JSON)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Curated Zone/machine_learning/'
TBLPROPERTIES ('has_encrypted_data'='false');
