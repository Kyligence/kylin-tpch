#!/bin/bash

# Note: this script is for AWS ec2 instances
set -e

# ============= Utils function ============
function info() {
  # shellcheck disable=SC2145
  echo -e "\033[32m$@\033[0m"
}

function warn() {
  # shellcheck disable=SC2145
  echo -e "\033[33m$@\033[0m"
}

function error() {
  # shellcheck disable=SC2145
  echo -e "\033[31m$@\033[0m"
}

function logging() {
  case $1 in
  "info")
    shift
    # shellcheck disable=SC2068
    info $@
    ;;
  "warn")
    shift
    # shellcheck disable=SC2068
    warn $@
    ;;
  "error")
    shift
    # shellcheck disable=SC2068
    error $@
    ;;
  *)
    # shellcheck disable=SC2068
    echo -e $@
    ;;
  esac
}

set +e

# =============== Env Parameters =================
# Prepare Steps
### Parameters for Spark and Kylin
#### ${SPARK_VERSION:0:1} get 2 from 2.4.7
HADOOP_VERSION=3.2.0
SPARK_VERSION=3.1.1
KYLIN_VERSION=4.0.0
HIVE_VERSION=2.3.9

### File name
KYLIN_PACKAGE=apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}.tar.gz
SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION:0:3}.tgz
HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}.tar.gz
HIVE_PACKAGE=apache-hive-${HIVE_VERSION}-bin.tar.gz
NODE_EXPORTER_PACKAGE=node_exporter-1.3.1.linux-amd64.tar.gz

### Parameter for DB
CURRENT_HOST=$(hostname -i)

### Parameter for JDK 1.8
JDK_PACKAGE=jdk-8u301-linux-x64.tar.gz
JDK_DECOMPRESS_NAME=jdk1.8.0_301

HOME_DIR=/home/ec2-user

function init_env() {
  HADOOP_DIR=${HOME_DIR}/hadoop
  if [[ ! -d $HADOOP_DIR ]]; then
    mkdir ${HADOOP_DIR}
  fi

  JAVA_HOME=/usr/local/java
  JRE_HOME=${JAVA_HOME}/jre
  HADOOP_HOME=${HADOOP_DIR}/hadoop-${HADOOP_VERSION}
  HIVE_HOME=${HADOOP_DIR}/hive
  KYLIN_TPCH_HOME=${HOME_DIR}/kylin-tpch

  KYLIN_HOME=${HOME_DIR}/apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}
  SPARK_HOME=${HADOOP_DIR}/spark
  OUT_LOG=${HOME_DIR}/shell.stdout

  # extra prometheus env
  NODE_EXPORTER_HOME=/home/ec2-user/node_exporter

  cat <<EOF >>~/.bash_profile
## Set env variables
### jdk env
export JAVA_HOME=${JAVA_HOME}
export JRE_HOME=${JRE_HOME}
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

#### hadoop env
export HADOOP_HOME=${HADOOP_HOME}

### hive env
export HIVE_HOME=${HIVE_HOME}

### prometheus related env
export NODE_EXPORTER_HOME=${NODE_EXPORTER_HOME}

### export all path
export PATH=$HIVE_HOME/bin:$HIVE_HOME/conf:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH

### other
export HOME_DIR=${HOME_DIR}
export KYLIN_HOME=${KYLIN_HOME}
export SPARK_HOME=${SPARK_HOME}
export OUT_LOG=${OUT_LOG}
export KYLIN_TPCH_HOME=${KYLIN_TPCH_HOME}
EOF
}

if [[ ! -f ~/.inited_env ]]; then
  logging info "Env variables not init, init it first ..."
  init_env
  touch ~/.inited_env
else
  logging warn "Env variables already inited, source it ..."
fi
# Init env variables
source ~/.bash_profile

exec 2>>${OUT_LOG}
set -o pipefail
# ================ Main Functions ======================
function help() {
  logging warn "Invalid input."
  logging warn "Usage: ${BASH_SOURCE[0]}
                    --bucket-url /path/to/bucket/without/prefix
                    --region region-for-s3
                    --db-host db-host-for-kylin
                    --db-password db-password-for-kylin
                    --db-user db-user-for-kylin
                    --zookeeper-host zookeeper-host-for-kylin
                    --kylin-mode mode-for-kylin[all|query|job]
                    --waiting-time time-for-start-services"
  exit 0
}

if [[ $# -ne 16 ]]; then
  help
fi

while [[ $# != 0 ]]; do
  if [[ $1 == "--bucket-url" ]]; then
    # url same as: /xxx/kylin
    BUCKET_SUFFIX=$2
  elif [[ $1 == "--region" ]]; then
    CURRENT_REGION=$2
  elif [[ $1 == "--db-host" ]]; then
    DATABASE_HOST=$2
  elif [[ $1 == "--db-password" ]]; then
    DATABASE_PASSWORD=$2
  elif [[ $1 == "--db-user" ]]; then
    DATABASE_USER=$2
  elif [[ $1 == "--zookeeper-host" ]]; then
    ZOOKEEPER_HOST=$2
  elif [[ $1 == "--kylin-mode" ]]; then
    # kylin mode in [ 'all', 'query', 'job' ]
    KYLIN_MODE=$2
  elif [[ $1 == "--waiting-time" ]]; then
    WAITING_TIME=$2
  else
    help
  fi
  shift
  shift
done

PATH_TO_BUCKET=s3:/${BUCKET_SUFFIX}
CONFIG_PATH_TO_BUCKET=s3a:/${BUCKET_SUFFIX}

# Main Functions and Steps
## prepare jdk env
function prepare_jdk() {
  logging info "Preparing Jdk ..."
  if [[ -f ${HOME_DIR}/.prepared_jdk ]]; then
    logging warn "Jdk already prepared, enjoy it."
    return
  fi

  # copy jdk from s3 bucket to ec2 instances, so user need to upload jdk package first
  aws s3 cp ${PATH_TO_BUCKET}/tar/${JDK_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
  # unzip jdk: tar -C /extract/to/path -xzvf /path/to/archive.tar.gz
  tar -zxf ${JDK_PACKAGE}
  sudo mv ${JDK_DECOMPRESS_NAME} ${JAVA_HOME}
  if [[ $? -ne 0 ]]; then
    logging error "Java package was not installed well, pleas check ..."
    exit 0
  fi
  logging info "Jdk inited ..."
  touch ${HOME_DIR}/.prepared_jdk
  logging info "Jdk is ready ..."
}

function init_jdk() {
  if [[ -f ${HOME_DIR}/.inited_jdk ]]; then
    logging warn "Jdk already inited, skip init ..."
    return
  fi
  # this function is remove the unsupport tls rules in java which version of 1.8.291 and above
  ## backup
  cp -f $JAVA_HOME/jre/lib/security/java.security $JAVA_HOME/jre/lib/security/java.security.bak

  ## modify the java.security file
  sed -e "s/\ TLSv1,\ TLSv1.1,//g" -i $JAVA_HOME/jre/lib/security/java.security
  logging info "Jdk inited ..."
  touch ${HOME_DIR}/.inited_jdk
}

function prepare_hadoop() {
  if [[ -f ${HOME_DIR}/.prepared_hadoop ]]; then
    logging warn "Hadoop already prepared, skip init ..."
    return
  fi

  if [[ -f ${HOME_DIR}/${HADOOP_PACKAGE} ]]; then
    logging warn "Hadoop package ${HADOOP_PACKAGE} already downloaded, skip download ..."
  else
    logging info "Downloading Hadoop package ${HADOOP_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${HADOOP_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/hadoop-${HADOOP_VERSION} ]]; then
    logging warn "Hadoop Package decompressed, skip decompress ..."
  else
    logging info "Decompress Hadoop package ..."
    tar -zxf ${HADOOP_PACKAGE}
  fi

  logging info "Hadoop prepared ..."
  touch ${HOME_DIR}/.prepared_hadoop
}

function init_hadoop() {
  # match correct region endpoints for hadoop's core-site.xml
  if [[ ${CURRENT_REGION} == "cn-northwest-1" ]]; then
    S3_ENDPOINT=s3.cn-northwest-1.amazonaws.com.cn
  elif [[ ${CURRENT_REGION} == "cn-north-1" ]]; then
    S3_ENDPOINT=s3.cn-north-1.amazonaws.com.cn
  else
    S3_ENDPOINT=s3.${CURRENT_REGION}.amazonaws.com
  fi

  if [[ -f ${HOME_DIR}/.inited_hadoop ]]; then
    logging warn "Hadoop already inited, skip init ..."
  else
    logging info "Init hadoop config ..."
    # replace jars for hadoop, although it don't need to start
    cp hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/
    cp hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/

    cat <<EOF >${HOME_DIR}/hadoop-${HADOOP_VERSION}/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
	    <name>fs.default.name</name>
	    <value>${CONFIG_PATH_TO_BUCKET}/working_dir</value>
	</property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
</configuration>
EOF

    logging info "Moving hadoop package to ${HADOOP_HOME} ..."
    mv ${HOME_DIR}/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

    logging warn "touch ${HOME_DIR}/.inited_hadoop ... "
    touch ${HOME_DIR}/.inited_hadoop
  fi

  logging info "Hadoop is ready ..."
}

function prepare_hive() {
  logging info "Preparing hive ..."
  if [[ -f ${HOME_DIR}/.prepared_hive ]]; then
    logging warn "Hive already prepared, enjoy it."
    return
  fi

  if [[ -f ${HOME_DIR}/${HIVE_PACKAGE} ]]; then
    logging warn "${HIVE_PACKAGE} already downloaded, skip download it ..."
  else
    logging info "Downloading ${HIVE_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${HIVE_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget https://downloads.apache.org/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/apache-hive-${HIVE_VERSION}-bin ]]; then
    logging warn "Hive Package already decompressed, skip decompress ..."
  else
    logging info "Decompress Hive package ..."
    tar -zxf ${HIVE_PACKAGE}
  fi

  if [[ -d ${HIVE_HOME} ]]; then
    logging warn "Hive package already exists, skip moving ..."
  else
    logging info "Moving hive package ${HIVE_PACKAGE%*.*.*} to ${HIVE_HOME} ..."
    mv ${HIVE_PACKAGE%*.*.*} ${HIVE_HOME}
    if [[ $? -ne 0 ]]; then
      logging error " Moving hive package failed, please check ..."
      exit 0
    fi
  fi

  # execute command
  hive --version
  if [[ $? -eq 0 ]]; then
    logging info "Hive ${HIVE_VERSION} is ready ..."
  else
    logging error "Hive not prepared well, please check ..."
    exit 0
  fi

  logging info "Hive prepared successfully ..."
  touch ${HOME_DIR}/.prepared_hive
}

function init_hive() {
  if [[ -f ${HOME_DIR}/.inited_hive ]]; then
    logging warn "Hive already init, skip init ..."
    return
  fi

  cat <<EOF >${HIVE_HOME}/conf/hive-site.xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${DATABASE_PASSWORD}</value>
    <description>password to use against metastore database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://${DATABASE_HOST}:3306/hive?createDatabaseIfNotExist=true&amp;useSSL=false</value>
    <description>JDBC connect string for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${DATABASE_USER}</value>
    <description>Username to use against metastore database;default is root</description>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
    <description>
      Enforce metastore schema version consistency.
      True: Verify that version information stored in metastore matches with one from Hive jars.  Also disable automatic
            schema migration attempt. Users are required to manually migrate schema after Hive upgrade which ensures
            proper metastore schema migration. (Default)
      False: Warn if the version information stored in metastore doesn't match with one from in Hive jars.
    </description>
  </property>
</configuration>
EOF

  # resolve jars conflict
  if [[ ! -d $HIVE_HOME/spark_jar ]]; then
    mkdir -p $HIVE_HOME/spark_jar
    mv $HIVE_HOME/lib/spark-* $HIVE_HOME/spark_jar/
    mv $HIVE_HOME/lib/jackson-module-scala_2.11-2.6.5.jar $HIVE_HOME/spark_jar/
  fi

  if [[ ! -f ${HIVE_HOME}/lib/mysql-connector-java-5.1.40.jar ]]; then
    logging warn "${HIVE_HOME}/lib/mysql-connector-java-5.1.40.jar not exist, download it ..."
    aws s3 cp ${PATH_TO_BUCKET}/jars/mysql-connector-java-5.1.40.jar ${HIVE_HOME}/lib/ --region ${CURRENT_REGION}
  fi

  if [[ ! -f ${HOME_DIR}/.inited_hive_metadata ]]; then
    bash -vx $HIVE_HOME/bin/schematool -dbType mysql -initSchema
    if [[ $? -ne 0 ]]; then
      logging error "Init hive metadata failed, please check ..."
      exit 0
    fi
    logging "Hive metadata inited successfully ..."
    touch ${HOME_DIR}/.inited_hive_metadata
  fi

  if [[ ! -d ${HIVE_HOME}/logs ]]; then
    logging info "Making dir ${HIVE_HOME}/logs"
    mkdir -p $HIVE_HOME/logs
  fi

  logging warn "touch ${HOME_DIR}/.inited_hive ..."
  # make a tag for hive inited
  touch ${HOME_DIR}/.inited_hive
  logging info "Hive is ready ..."
}

function start_hive() {
  nohup $HIVE_HOME/bin/hive --service metastore >>$HIVE_HOME/logs/hivemetastorelog.log 2>&1 &
  logging info "Hive was logging in $HIVE_HOME/logs, you can check ..."
}

function prepare_spark() {
  if [[ -f ${HOME_DIR}/.prepared_spark ]]; then
    logging warn "Spark already prepared, enjoy it."
    return
  fi

  logging info "Downloading Spark-${SPARK_VERSION} ..."
  ## download spark
  if [[ -f ${HOME_DIR}/${SPARK_PACKAGE} ]]; then
    logging warn "${SPARK_PACKAGE} already download, skip download it."
  else
    logging warn "Downloading ${SPARK_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${SPARK_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/${SPARK_PACKAGE%*.*} ]]; then
    logging warn "Spark package already decompressed, skip decompress ..."
  else
    logging info "Decompressing ${SPARK_PACKAGE} ..."
    ### unzip spark tar file
    tar -zxf ${SPARK_PACKAGE}
  fi

  if [[ -d ${SPARK_HOME} ]]; then
    logging warn "${SPARK_HOME} already exists, skip moving ..."
  else
    logging info "Moving ${SPARK_PACKAGE%*.*} to ${SPARK_HOME}"
    mv ${SPARK_PACKAGE%*.*} ${SPARK_HOME}
  fi

  logging info "Prepare Spark-${SPARK_VERSION} success."
  touch ${HOME_DIR}/.prepared_spark
}

function init_spark() {
  if [[ -f ${HOME_DIR}/.inited_spark ]]; then
    logging warn "Spark already inited, enjoy it."
    return
  fi

  # copy needed jars
  if [[ ! -f $SPARK_HOME/jars/hadoop-aws-${HADOOP_VERSION}.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
  fi

  if [[ ! -f $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.375.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $SPARK_HOME/jars/
  fi

  if [[ ! -f $SPARK_HOME/jars/mysql-connector-java-5.1.40.jar ]]; then
    cp $HIVE_HOME/lib/mysql-connector-java-5.1.40.jar $SPARK_HOME/jars/
  fi

  # hive-site.xml for spark
  if [[ ! -f $SPARK_HOME/conf/hive-site.xml ]]; then
    cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/
  fi

  logging info "Spark inited ..."
  touch ${HOME_DIR}/.inited_spark
  logging info "Spark is ready ..."
}

function start_spark_master() {
  $SPARK_HOME/sbin/start-master.sh
  if [[ $? -ne 0 ]]; then
    logging error "spark start master failed, please check ..."
    exit 0
  fi
  logging info "Start Spark master successfully ..."
}

function prepare_kylin() {
  logging info "Preparing kylin ..."

  if [[ -f ${HOME_DIR}/.prepared_kylin ]]; then
    logging warn "Kylin already prepared ..."
    return
  fi

  if [[ -f ${HOME_DIR}/${KYLIN_PACKAGE} ]]; then
    logging warn "Kylin package already downloaded, skip download it ..."
  else
    logging info "Kylin-${KYLIN_VERSION} downloading ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${KYLIN_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget https://archive.apache.org/dist/kylin/apache-kylin-${KYLIN_VERSION}/${KYLIN_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1} ]]; then
    logging warn "Kylin package already decompress, skip decompress ..."
  else
    logging warn "Kylin package decompressing ..."
    ### unzip kylin tar file
    tar -zxf ${KYLIN_PACKAGE}
  fi

  logging info "Kylin inited ..."
  touch ${HOME_DIR}/.prepared_kylin
  logging info "Kylin prepared ..."
}

function init_kylin() {
  if [[ -f ${HOME_DIR}/.inited_kylin ]]; then
    logging warn "Kylin already inited ..."
    return
  fi

  if [[ ! -d ${KYLIN_HOME}/ext ]]; then
    ### create dir directory for other dependency used by kylin
    mkdir -p ${KYLIN_HOME}/ext
  fi

  if [[ ! -f $KYLIN_HOME/ext/mysql-connector-java-5.1.40.jar ]]; then
    aws s3 cp ${PATH_TO_BUCKET}/jars/mysql-connector-java-5.1.40.jar $KYLIN_HOME/ext/ --region ${CURRENT_REGION}
  fi

  if [[ ! -f $KYLIN_HOME/ext/slf4j-log4j12-1.7.25.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar $KYLIN_HOME/ext/
  fi

  if [[ ! -f $KYLIN_HOME/ext/log4j-1.2.17.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/common/lib/log4j-1.2.17.jar $KYLIN_HOME/ext/
  fi

  if [[ ${KYLIN_MODE} == "all" ]]; then
    cat <<EOF >${KYLIN_HOME}/conf/kylin.properties
kylin.server.mode=${KYLIN_MODE}
kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://${DATABASE_HOST}:3306/kylin,username=root,password=${DATABASE_PASSWORD},maxActive=10,maxIdle=10
kylin.env.zookeeper-connect-string=${ZOOKEEPER_HOST}
kylin.env.hdfs-working-dir=${CONFIG_PATH_TO_BUCKET}/working_dir/
kylin.cube.cubeplanner.enabled=true
## Build Engine Resource
kylin.engine.spark-conf.spark.eventLog.dir=${CONFIG_PATH_TO_BUCKET}/working_dir/spark-history
kylin.engine.spark-conf.spark.history.fs.logDirectory=${CONFIG_PATH_TO_BUCKET}/working_dir/spark-history
kylin.engine.spark-conf.spark.master=spark://${CURRENT_HOST}:7077

kylin.engine.spark-conf.spark.executor.cores=3
kylin.engine.spark-conf.spark.executor.instances=20
kylin.engine.spark-conf.spark.executor.memory=12GB
kylin.engine.spark-conf.spark.executor.memoryOverhead=1GB

## Parquet Column Index
kylin.engine.spark-conf.spark.hadoop.parquet.page.size=1048576
kylin.engine.spark-conf.spark.hadoop.parquet.page.row.count.limit=100000
kylin.engine.spark-conf.spark.hadoop.parquet.block.size=268435456
kylin.query.spark-conf.spark.hadoop.parquet.filter.columnindex.enabled=true

## Query Engine Resource
kylin.query.spark-conf.spark.master=spark://${CURRENT_HOST}:7077
kylin.query.spark-conf.spark.driver.cores=1
kylin.query.spark-conf.spark.driver.memory=8GB
kylin.query.spark-conf.spark.driver.memoryOverhead=1G
kylin.query.spark-conf.spark.executor.instances=30
kylin.query.spark-conf.spark.executor.cores=2
kylin.query.spark-conf.spark.executor.memory=7G
kylin.query.spark-conf.spark.executor.memoryOverhead=1G
kylin.query.spark-conf.spark.sql.parquet.filterPushdown=false

## Disable canary
kylin.canary.sparder-context-canary-enabled=false
## Query Cache
kylin.query.cache-enabled=true

EOF
  elif [[ ${KYLIN_MODE} == "query" ]]; then
    cat <<EOF >${KYLIN_HOME}/conf/kylin.properties
# Kylin server mode, valid value [all, query, job]
kylin.server.mode=${KYLIN_MODE}
kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://${DATABASE_HOST}:3306/kylin,username=root,password=${DATABASE_PASSWORD},maxActive=10,maxIdle=10
kylin.env.zookeeper-connect-string=${ZOOKEEPER_HOST}
kylin.env.hdfs-working-dir=${CONFIG_PATH_TO_BUCKET}/working_dir/
## Query Engine Resource
kylin.query.spark-conf.spark.master=spark://${CURRENT_HOST}:7077
kylin.query.spark-conf.spark.driver.cores=1
kylin.query.spark-conf.spark.driver.memory=8GB
kylin.query.spark-conf.spark.driver.memoryOverhead=1G
kylin.query.spark-conf.spark.executor.instances=30
kylin.query.spark-conf.spark.executor.cores=2
kylin.query.spark-conf.spark.executor.memory=7G
kylin.query.spark-conf.spark.executor.memoryOverhead=1G
kylin.query.spark-conf.spark.sql.parquet.filterPushdown=false

## Disable canary
kylin.canary.sparder-context-canary-enabled=false
## Query Cache
kylin.query.cache-enabled=true
EOF

  elif [[ ${KYLIN_MODE} == "job" ]]; then
    cat <<EOF >${KYLIN_HOME}/conf/kylin.properties
kylin.server.mode=${KYLIN_MODE}
kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://${DATABASE_HOST}:3306/kylin,username=root,password=${DATABASE_PASSWORD},maxActive=10,maxIdle=10
kylin.env.zookeeper-connect-string=${ZOOKEEPER_HOST}
kylin.env.hdfs-working-dir=${CONFIG_PATH_TO_BUCKET}/working_dir/
kylin.cube.cubeplanner.enabled=true
## Build Engine Resource
kylin.engine.spark-conf.spark.eventLog.dir=${CONFIG_PATH_TO_BUCKET}/working_dir/spark-history
kylin.engine.spark-conf.spark.history.fs.logDirectory=${CONFIG_PATH_TO_BUCKET}/working_dir/spark-history
kylin.engine.spark-conf.spark.master=spark://${CURRENT_HOST}:7077

kylin.engine.spark-conf.spark.executor.cores=3
kylin.engine.spark-conf.spark.executor.instances=20
kylin.engine.spark-conf.spark.executor.memory=12GB
kylin.engine.spark-conf.spark.executor.memoryOverhead=1GB

## Parquet Column Index
kylin.engine.spark-conf.spark.hadoop.parquet.page.size=1048576
kylin.engine.spark-conf.spark.hadoop.parquet.page.row.count.limit=100000
kylin.engine.spark-conf.spark.hadoop.parquet.block.size=268435456
kylin.query.spark-conf.spark.hadoop.parquet.filter.columnindex.enabled=true
EOF

  fi

  logging info "Kylin already inited ..."
  touch ${HOME_DIR}/.inited_kylin
  logging info "Kylin is ready ..."
}

function after_start_kylin() {
  KYLIN_WEB_LIB_PATH=$KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib
  if [[ ! -f $KYLIN_WEB_LIB_PATH/commons-collections-3.2.2.jar ]]; then
    cp ${HIVE_HOME}/lib/commons-collections-3.2.2.jar $KYLIN_WEB_LIB_PATH/
  fi

  if [[ ! -f $KYLIN_WEB_LIB_PATH/commons-configuration-1.3.jar ]]; then
    aws s3 cp ${PATH_TO_BUCKET}/jars/commons-configuration-1.3.jar $KYLIN_WEB_LIB_PATH/ --region ${CURRENT_REGION}
  fi

  if [[ ! -f $KYLIN_WEB_LIB_PATH/aws-java-sdk-bundle-1.11.375.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/common/lib/aws-java-sdk-bundle-1.11.375.jar $KYLIN_WEB_LIB_PATH/
  fi

  if [[ ! -f $KYLIN_WEB_LIB_PATH/hadoop-aws-3.2.0.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/common/lib/hadoop-aws-3.2.0.jar $KYLIN_WEB_LIB_PATH/
  fi
}

function start_kylin() {
  ${KYLIN_HOME}/bin/kylin.sh start
  sleep 30
}

function sample_for_kylin() {
  ${KYLIN_HOME}/bin/sample.sh
  if [[ $? -ne 0 ]]; then
    logging error "Sample for kylin is failed, please check ..."
    exit 0
  else
    logging info "Sample for kylin is successful, enjoy it ..."
  fi
}

function restart_kylin() {
  ${KYLIN_HOME}/bin/kylin.sh restart
}

function prepare_node_exporter() {
  logging info "Preparing node_exporter ..."
  if [[ -f ${HOME_DIR}/.prepared_node_exporter ]]; then
    logging warn "NODE_EXPORTER already prepared, skip prepare ... "
    return
  fi

  if [[ ! -f ${HOME_DIR}/${NODE_EXPORTER_PACKAGE} ]]; then
    logging info "NODE_EXPORTER package ${NODE_EXPORTER_PACKAGE} not downloaded, downloading it ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${NODE_EXPORTER_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
  else
    logging warn "NODE_EXPORTER package ${NODE_EXPORTER_PACKAGE} already download, skip download it."
  fi
  touch ${HOME_DIR}/.prepared_prometheus
  logging info "NODE_EXPORTER prepared ..."
}

function init_node_exporter() {
  logging info "Initializing node_exporter ..."
  if [[ -f ${HOME_DIR}/.inited_node_exporter ]]; then
    logging warn "NODE_EXPORTER already inited, skip init ... "
    return
  fi

  if [[ ! -f ${NODE_EXPORTER_HOME} ]]; then
    logging info "NODE_EXPORTER home ${NODE_EXPORTER_HOME} not ready, decompressing ${NODE_EXPORTER_PACKAGE} ..."
    tar -zxf ${HOME_DIR}/${NODE_EXPORTER_PACKAGE}
    mv ${HOME_DIR}/${NODE_EXPORTER_PACKAGE%.tar.gz} ${NODE_EXPORTER_HOME}
  else
    logging warn "NODE_EXPORTER home ${PROMETHEUS_PACKAGE} already ready."
  fi
  touch ${HOME_DIR}/.inited_prometheus
  logging info "NODE_EXPORTER inited ..."
}

function start_node_exporter() {
  # NOTE: default node_exporter port 9100
  logging info "Start node_exporter ..."
  nohup ${NODE_EXPORTER_HOME}/node_exporter >> ${NODE_EXPORTER_HOME}/node.log 2>&1 &
}

function prepare_packages() {
  if [[ -f ${HOME_DIR}/.prepared_packages ]]; then
    logging warn "Packages already prepared, skip prepare ..."
    return
  fi

  prepare_jdk
  init_jdk

  # add extra monitor service
  prepare_node_exporter
  init_node_exporter

  prepare_hadoop
  init_hadoop
  # TODO: fix this hard code sleep time
  # sleep time to ensure mysql service in distribution node is ready
  sleep ${WAITING_TIME}
  prepare_hive
  init_hive

  prepare_spark
  init_spark

  prepare_kylin
  init_kylin

  touch ${HOME_DIR}/.prepared_packages
  logging info "All need packages are ready ..."
}

function start_services_on_master() {
  # start node_exporter
  start_node_exporter

  start_hive
  start_spark_master

  # special step for compatible jars, details in after_start_kylin
  sample_for_kylin
  start_kylin
  after_start_kylin
  restart_kylin
}

function main() {
  prepare_packages
  start_services_on_master
}

main
