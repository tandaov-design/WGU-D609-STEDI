CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.step_trainer_landing (
  sensorReadingTime  bigint,
  serialNumber       string,
  distanceFromObject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://wgutdao20/step_trainer/landing/';