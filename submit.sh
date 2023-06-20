
partner=$1
num_args=$#
args="${@:3:$num_args}"

docker run \
--network=$1_pyNet \
-v $(pwd):/app \
--rm cluster-image \
/opt/bitnami/spark/bin/spark-submit \
--conf "spark.pyspark.python=python3" \
--conf "spark.driver.memory=2g" \
--conf "spark.executor.memory=1g" \
--master spark://master:7077 \
--deploy-mode client \
--name my_pyspark_job /app/$2 $args