
memory="1024m"
cwd=`pwd`
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.95.x86_64
export HBASE_CONF_DIR=/etc/hbase/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export CLASSPATH="./:/etc/hbase/conf:/etc/hadoop/conf"

for file in `ls /usr/hdp/2.2.9.0-3393/hadoop/client/*.jar`
do
  export CLASSPATH=$CLASSPATH:$file
done

for file in `ls /usr/hdp/2.2.9.0-3393/hbase/lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$file
done

export LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/amd64/server/:/usr/hdp/current/hadoop-client/lib/native/"
export LIBHBASE_OPTS="-Xmx$memory"
