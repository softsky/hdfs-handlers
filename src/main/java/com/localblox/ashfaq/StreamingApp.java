package com.localblox.ashfaq;

import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 *
 */
/*
deploy:
mvn clean package -DskipTests && cp -v target/hdfs-handlers-1.0-SNAPSHOT.jar ../hdfsdata/apps/

run:
spark-submit --class com.localblox.ashfaq.StreamingApp --master local --driver-memory 1g --executor-memory 1g \
--executor-cores 1 tmp/hdfs-handlers-1.0-SNAPSHOT.jar

check:
// put file to hdfs 1.csv:
hdfs dfs -appendToFile /tmp/J_AddDist.csv /in/4.txt

then out file appers with name like:
hdfs dfs -ls /out
-rw-r--r--   1 spark spark        384 2017-12-14 22:37 /out/part-00000-d463015b-b85e-47bf-85b1-48fa67d440b6-c000.csv

 */
public class StreamingApp {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
            .builder()
            .appName("FileProcessor")
            .getOrCreate();

//        stream-to-batch joins

        // TODO - create schema manually to be consistent! this is just for test
        spark.conf().set("spark.sql.streaming.schemaInference", true);

        Dataset<Row> lines = spark.readStream()
//                                  .option()
                                  .csv("hdfs:///in");

        // TODO refer to real prediction logic.
        Dataset<Row> predicted = lines.withColumn("prediction", lit("predicted"));

        // FIXME - used fro debug purposes
        // Start running the query that prints the running counts to the console
//        StreamingQuery query = lines.writeStream()
//                                    .outputMode("append")
//                                    .format("console")
//                                    .start();

        StreamingQuery flushPredicted = predicted.writeStream().outputMode("append")
                                                 .format("csv")
                                                 .option("path", "hdfs://master/out")
                                                 .option("checkpointLocation", "hdfs://master/spark-checkpoint")
                                                 .start();

        flushPredicted.awaitTermination();

    }

}
