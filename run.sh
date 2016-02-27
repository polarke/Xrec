executorMemory=10G
numExecutors=20

spark-submit \
    --class xrec.Datacenter \
    --master yarn-cluster \
    --executor-memory $executorMemory \
    --num-executors $numExecutors \
    target/scala-2.10/xrec_2.10-0.1.jar \
