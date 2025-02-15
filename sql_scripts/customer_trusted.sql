CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_trusted (
    serialnumber                STRING,
    sharewithpublicasofdate     BIGINT,
    birthday                    STRING,
    registrationdate            BIGINT,
    sharewithresearchasofdate   BIGINT,
    customername                STRING,
    sharewithfriendsasofdate    BIGINT,
    email                       STRING,
    lastupdatedate              BIGINT,
    phone                       STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project/Trusted Zone/customer/'
TBLPROPERTIES ('has_encrypted_data'='false');
