CREATE EXTERNAL TABLE IF NOT EXISTS `stdi`.`accelerometer_landing` (
  `user` STRING,
  `timestamp` BIGINT,
  `x` DOUBLE,
  `y` DOUBLE,
  `z` DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-halah/accelerometer_landing/landing/'
TBLPROPERTIES ('classification' = 'json');



