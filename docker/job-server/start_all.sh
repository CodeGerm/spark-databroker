#!/bin/bash

set -e

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh




MAIN="org.cg.spark.databroker.ChannelCluster"


$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS" --driver-class-path $appdir/akka-remote_2.10-2.3.11.jar:$appdir/akka-cluster_2.10-2.3.11.jar:$appdir/akka-contrib_2.10-2.3.11.jar   $appdir/org.cg.spark-databroker-0.0.1-SNAPSHOT-jar-with-dependencies.jar /channel_cluster_server.conf  2>&1 < /dev/null &

/app/server_start.sh