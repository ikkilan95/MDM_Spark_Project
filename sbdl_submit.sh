ENV=${1:-qa}
JOB_DATE=${2:-2022-08-02}

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files sdbl_lib.zip \
  --files conf/sdbl.conf,conf/spark.conf,log4j.properties \
  --conf "spark.executorEnv.CONFLUENT_API_KEY=${CONFLUENT_API_KEY}" \
  --conf "spark.executorEnv.CONFLUENT_API_SECRET=${CONFLUENT_API_SECRET}" \
  --conf "spark.yarn.appMasterEnv.CONFLUENT_API_KEY=${CONFLUENT_API_KEY}" \
  --conf "spark.yarn.appMasterEnv.CONFLUENT_API_SECRET=${CONFLUENT_API_SECRET}" \
  sbdl_main.py $ENV $JOB_DATE