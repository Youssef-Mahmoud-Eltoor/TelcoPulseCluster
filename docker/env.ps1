$JOBS = (Resolve-Path (Join-Path $PSScriptRoot "..\jobs")).Path
$conf = @(
  "--master","spark://192.168.1.11:7077",
  "--total-executor-cores","8","--executor-cores","2","--executor-memory","1g",
  "--conf","spark.driver.host=192.168.1.11",
  "--conf","spark.driver.bindAddress=0.0.0.0",
  "--conf","spark.driver.port=7080",
  "--conf","spark.driver.blockManager.port=7081",
  "--conf","spark.hadoop.fs.s3a.endpoint=http://192.168.1.12:9000",
  "--conf","spark.hadoop.fs.s3a.path.style.access=true",
  "--conf","spark.hadoop.fs.s3a.connection.ssl.enabled=false",
  "--conf","spark.hadoop.fs.s3a.access.key=TelcoPulseAdmin2026",
  "--conf","spark.hadoop.fs.s3a.secret.key=TelcoPulseAdmin2026"
)
"JOBS = $JOBS | conf count = $($conf.Count)"
dir $JOBS
