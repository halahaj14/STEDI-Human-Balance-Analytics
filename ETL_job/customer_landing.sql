CREATE EXTERNAL TABLE IF NOT EXISTS `stdi`.`customer_landing` (
  `customername` STRING,
  `email` STRING,
  `phone` STRING,
  `birthday` STRING,
  `serialnumber` STRING,
  `registrationdate` BIGINT,
  `lastupdatedate` BIGINT,
  `sharewithresearchasofdate` BIGINT,
  `sharewithpublicasofdate` BIGINT,
  `sharewithfriendsasofdate` BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-halah/customer_landing/landing/'
TBLPROPERTIES ('classification' = 'json');
