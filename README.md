
# How to test locally

1. Create docker compose file like this (file name shoul be `docker-compose.yml`):
```
version: "2"

services:
  master:
    image: softsky/spark
    command: start-spark master
    hostname: master
    ports:
      - "6066:6066"
      - "7070:7070"
      - "8080:8080"
      - "50070:50070"
    volumes:
      - <LOCAL_HDFS_MASTER_PATH>:/opt/hdfs
      - <LOCAL_APPLICATION_PATH>:/tmp
  worker:
    image: softsky/spark
    command: start-spark worker master
    environment:
      SPARK_WORKER_CORES: 1
      SPARK_WORKER_MEMORY: 2g
    links:
      - master
    volumes:
      - <LOCAL_HDFS_WORKER_PATH>:/opt/hdfs

```

Note: 

- All local path should be created before start docker compose
- <LOCAL_HDFS_MASTER_PATH> should point to any directory to save HDFS Master state between container deletions
- <LOCAL_APPLICATION_PATH> should point to any directory to save HDFS Worker state between container deletions
- <LOCAL_APPLICATION_PATH> should point to path where you will copy your app jar (hdfs-handlers-1.0-SNAPSHOT.jar)

2. run `docker-compose up` in folder with `docker-compose.yml`

Note:
- First time you need to run `docker-compose up` it actually creates the container. Then you need CTRL-C to stop.
- All subsequent start stop should be done with:
  `docker-compose start`
  `docker-compose stop`
- To delete containers created by compose run `docker-compose down`

3. Build and copy app jar to <LOCAL_APPLICATION_PATH> with command from project root:
```
mvn clean package -DskipTests && cp -v target/hdfs-handlers-1.0-SNAPSHOT.jar <LOCAL_APPLICATION_PATH>
```

4. Login to master spark node inside docker (let it be Terminal-1):
```
docker exec -it sparkdocker_master_1 bash
```

4.1 Create in/out directories in HDFS inside master:
```
hdfs dfs -mkdir /in
hdfs dfs -mkdir /out
hdfs dfs -ls /
```

4.2 Run spark application inside docker (output will be redirected to console, CTRL-C to exit):
```
spark-submit --class com.localblox.ashfaq.App \
    --master local \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    tmp/hdfs-handlers-1.0-SNAPSHOT.jar hdfs:///in
```

5. Login to master spark node inside docker (let it be Terminal-2):
```
docker exec -it sparkdocker_master_1 bash
```

5.1 Put new file to in folder:
```
hdfs dfs -put tmp/test_file_name.csv /in/new_file_name_to_process.csv
```

5.2 See the result of file processing in Terminal-1
