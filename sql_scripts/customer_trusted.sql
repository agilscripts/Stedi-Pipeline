CREATE EXTERNAL TABLE stedi.customer_trusted (
    serialnumber                string,
    sharewithpublicasofdate     bigint,
    birthday                    date,
    registrationdate            bigint,
    sharewithresearchasofdate   bigint,
    customername                string,
    sharewithfriendsasofdate    bigint,
    email                       string,
    lastupdatedate              bigint,
    phone                       string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json'='true'
)
LOCATION 's3://stedi-project/customer_trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
