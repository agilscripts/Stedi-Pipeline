CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    serialnumber                STRING,
    sharewithpublicasofdate     BIGINT,
    birthday                    STRING,   -- or DATE if you prefer
    registrationdate            BIGINT,
    sharewithresearchasofdate   BIGINT,
    customername                STRING,
    sharewithfriendsasofdate    BIGINT,
    email                       STRING,
    lastupdatedate              BIGINT,
    phone                       STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Landing Zone/customer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
