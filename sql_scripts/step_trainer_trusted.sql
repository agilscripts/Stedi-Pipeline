CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_trusted (
    sensorreadingtime  BIGINT,
    serialnumber       STRING,
    distancefromobject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Trusted Zone/step_trainer/'
TBLPROPERTIES ('has_encrypted_data'='false');
