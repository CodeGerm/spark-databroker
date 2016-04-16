FROM yanlinw/realtimespark_jobserver

# Expose driver port range (spark.driver.port + 16)

ADD ./spark-databroker.conf /app/
ADD ./channel_cluster_server.conf /
ADD ./*.sh /app/
ADD ./org.cg.spark-databroker-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /app/
ADD ./org.cg.spark-databroker-0.0.1-SNAPSHOT.jar /app/
ADD ./akka-contrib_2.10-2.3.11.jar /app/
ADD ./akka-cluster_2.10-2.3.11.jar /app/

ENV YARN_CONF_DIR=/app/cluster-config
# Set env variables for spark

ENV HADOOP_CONF_DIR=/app/cluster-config
# Set env variables for spark

ENTRYPOINT "/app/start_all.sh"