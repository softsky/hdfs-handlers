FROM softsky/spark
MAINTAINER SOFTSKY Arsen A. Gutsal <a.gutsal@softsky.com.ua>

# Copy start script
COPY src/main/docker/start-handlers /opt/util/bin/start-handlers
RUN chmod +x /opt/util/bin/start-handlers
ENV PATH=$PATH:/opt/util/bin

# add jar
ADD target/hdfs-handlers-1.0-SNAPSHOT.jar /app.jar

RUN useradd -ms /bin/bash spark

USER spark
