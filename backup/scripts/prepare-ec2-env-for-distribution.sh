#!/bin/bash

# Note: this script is for Creating Zookeeper and DB to hold cluster
# This Script contains services which are Zookeeper, JDK and Mysql DB
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
## Parameter
EC2_DEFAULT_USER=ec2-user

### Parameters for Spark and Kylin
#### ${SPARK_VERSION:0:1} get 2 from 2.4.7
ZOOKEEPER_VERSION=3.4.13

### File name
ZOOKEEPER_PACKAGE=zookeeper-${ZOOKEEPER_VERSION}.tar.gz
METADADA_FILE=metadata-backup.sql
MYSQL_RPM=mysql-community-server-5.7.35-1.el7.x86_64.rpm

### Parameter for DB
DATABASE_NAME=kylin
DATABASE_PASSWORD=123456
CHARACTER_SET_SERVER=utf8mb4
COLLATION_SERVER=utf8mb4_unicode_ci
MYSQL_VERSION=5.7

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
  ZOOKEEPER_HOME=${HADOOP_DIR}/zookeeper
  OUT_LOG=${HOME_DIR}/shell.stdout

  cat <<EOF >>~/.bash_profile
## Set env variables
### jdk env
export JAVA_HOME=${JAVA_HOME}
export JRE_HOME=${JRE_HOME}
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

### zookeeper env
export ZOOKEEPER_HOME=${ZOOKEEPER_HOME}

### export all path
export PATH=${JAVA_HOME}/bin:${ZOOKEEPER_HOME}/bin:$PATH

### other
export HOME_DIR=${HOME_DIR}
export OUT_LOG=$OUT_LOG
EOF
}

if [[ ! -f ~/.inited_env ]]; then
  logging info "Env variables not init, init it first ..."
  init_env
  touch ~/.inited_env
else
  logging warn "Env variables already inited, source it ..."
fi
source ~/.bash_profile
exec 2>>${OUT_LOG}
set -o pipefail
# ================ Main Functions ======================
function help() {
  logging warn "Invalid input."
  logging warn "Usage: ${BASH_SOURCE[0]}
                       --bucket-url /path/to/bucket/without/prefix
                       --region region-for-current-instance"
  exit 0
}

if [[ $# -ne 4 ]]; then
  help
fi

while [[ $# != 0 ]]; do
  if [[ $1 == "--bucket-url" ]]; then
    # url same as: /xiaoxiang-yu/kylin-xtt
    BUCKET_SUFFIX=$2
  elif [[ $1 == "--region" ]]; then
    CURRENT_REGION=$2
  else
    help
  fi
  shift
  shift
done

PATH_TO_BUCKET=s3:/${BUCKET_SUFFIX}

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

## install mysql db with docker
function start_docker() {
  # check docker whether is running
  docker_status=$(systemctl is-active docker)
  if [[ $docker_status == "active" ]]; then
    logging warn "Docker service is already running, don't need to start it ..."
  else
    logging warn "Docker service is stopped, starting it ..."
    sudo systemctl start docker
    docker_status=$(systemctl is-active docker)
    if [[ $docker_status == "inactive" ]]; then
      logging error "Start docker failed, please check."
      exit 0
    fi
    logging info "Start docker success ..."
  fi
}

function prepare_docker() {
  logging info "Preparing docker ..."

  if [[ -f ${HOME_DIR}/.prepared_docker ]]; then
    logging warn "Docker service already installed, restart it."
    return
  fi

  # check docker whether is installed
  if [[ -x "$(command -v docker)" ]]; then
    docker_version=$(sudo docker -v)
    logging info "Docker is already installed, version is ${docker_version}"
  else
    logging warn "Docker was not installed, now install docker ..."
    sudo yum install docker -y
  fi

  if [[ $(getent group docker) ]]; then
    logging warn "docker group exists, don't need create it."
  else
    logging warn "docker does not exist, create it."
    sudo groupadd docker
  fi

  if id -Gn ${EC2_DEFAULT_USER} | grep docker; then
    logging warn "${EC2_DEFAULT_USER} already in group of docker"
  else
    logging info "Group of docker add user ${EC2_DEFAULT_USER}"
    sudo usermod -aG docker $EC2_DEFAULT_USER
  fi

  touch ${HOME_DIR}/.prepared_docker
  logging info "docker is ready ..."
}

function prepare_mysql() {
  logging info "Preparing mysql ..."
  if [[ -f ${HOME_DIR}/.prepared_mysql ]]; then
    logging warn "Mysql service already installed, check it."
    return
  fi

  start_docker

  if [[ $(sudo docker ps -q -f name=mysql-${MYSQL_VERSION}) ]]; then
    logging warn "Mysql-${MYSQL_VERSION} already running, skip this ..."
  else
    # default user is root !!!
    sudo docker run --name mysql-${MYSQL_VERSION} \
      --restart=always \
      --health-cmd='mysqladmin ping --silent' \
      -e MYSQL_ROOT_PASSWORD=${DATABASE_PASSWORD} \
      -e MYSQL_DATABASE=${DATABASE_NAME} \
      -d -p 3306:3306 mysql:${MYSQL_VERSION} \
      --character-set-server=${CHARACTER_SET_SERVER} \
      --collation-server=${COLLATION_SERVER}

    if [[ $? -ne 0 ]]; then
      logging error "Mysql start in docker was failed, please check ..."
      exit 0
    fi
  fi
  # install mysql server for backup and test
  # Note: don't need to start
  if [[ ! -f ${HOME_DIR}/.prepared_mysql_server ]]; then
    logging warn "mysql server not installed, install it now ..."
        if [[ ! -f ${HOME_DIR}/mysql57-community-release-el7-8.noarch.rpm ]]; then
            wget http://repo.mysql.com/mysql57-community-release-el7-8.noarch.rpm
        fi
        sudo rpm -ivh mysql57-community-release-el7-8.noarch.rpm
        sudo yum install mysql -y
    touch ${HOME_DIR}/.prepared_mysql_server
  else
    logging info "mysql server was installed, skip install it ..."
  fi
  logging warn "touch ${HOME_DIR}/.prepared_mysql"
  touch ${HOME_DIR}/.prepared_mysql
  logging info "Mysql is ready ..."
}

function prepare_metadata() {
  logging info "Check history metadata whether exists ..."
  aws s3 cp ${PATH_TO_BUCKET}/backup/ec2/${METADADA_FILE} ${HOME_DIR} --region ${CURRENT_REGION}
  if [[ $? -ne 0 ]]; then
    logging warn "Metadata file: ${METADADA_FILE} not exists, so skip restore step ..."
    return
  fi

  logging info "Restoring metadata to mysql ..."
  # default user is root !
  mysql -h$(hostname -i) -uroot -p${DATABASE_PASSWORD} <${HOME_DIR}/${METADADA_FILE}
  logging info "Restored metadata to mysql ..."
}

function prepare_zookeeper() {
  logging info "Preparing zookeeper ..."
  if [[ -f ${HOME_DIR}/.prepared_zookeeper ]]; then
    logging warn "Zookeeper service already installed, restart it."
    return
  fi

  if [[ -f ./${ZOOKEEPER_PACKAGE} ]]; then
    logging warn "Zookeeper package already download, skip download it"
  else
    logging info "Downloading Zookeeper package ${ZOOKEEPER_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${ZOOKEEPER_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget http://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} ]]; then
    logging warn "Zookeeper Package decompressed, skip decompress ..."
  else
    logging info "Decompress Zookeeper package ..."
    tar -zxf ${ZOOKEEPER_PACKAGE}
  fi

  logging info "Zookeeper prepared ..."
  touch ${HOME_DIR}/.prepared_zookeeper
}

function init_zookeeper() {
  if [[ -f ${HOME_DIR}/.inited_zookeeper ]]; then
    logging warn "Zookeeper already inited ..."
  else
    logging info "Init Zookeeper config ..."
    # copy cfg to set fake zk cluster
    cp zookeeper-${ZOOKEEPER_VERSION}/conf/zoo_sample.cfg zookeeper-${ZOOKEEPER_VERSION}/conf/zoo1.cfg
    cp zookeeper-${ZOOKEEPER_VERSION}/conf/zoo_sample.cfg zookeeper-${ZOOKEEPER_VERSION}/conf/zoo2.cfg
    cp zookeeper-${ZOOKEEPER_VERSION}/conf/zoo_sample.cfg zookeeper-${ZOOKEEPER_VERSION}/conf/zoo3.cfg

    for i in {1..3}; do
      cat <<EOF >zookeeper-${ZOOKEEPER_VERSION}/conf/zoo${i}.cfg
# zoo${i}.cfg
tickTime=2000
initLimit=10
syncLimit=5
server.1=localhost:2287:3387
server.2=localhost:2288:3388
server.3=localhost:2289:3389
dataDir=/tmp/zookeeper/zk${i}/data
dataLogDir=/tmp/zookeeper/zk${i}/log
clientPort=218${i}
EOF
      mkdir -p /tmp/zookeeper/zk${i}/log
      mkdir -p /tmp/zookeeper/zk${i}/data
      echo ${i} >>/tmp/zookeeper/zk${i}/data/myid
    done

    logging info "Moving ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} to ${ZOOKEEPER_HOME} ..."
    mv ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} ${ZOOKEEPER_HOME}

    logging warn "touch ${HOME_DIR}/.inited_zookeeper ..."
    touch ${HOME_DIR}/.inited_zookeeper
  fi
  logging info "Zookeeper is ready ..."
}

function start_zookeeper() {
  for i in {1..3}; do
    ${ZOOKEEPER_HOME}/bin/zkServer.sh start ${ZOOKEEPER_HOME}/conf/zoo${i}.cfg

    if [[ $? -ne 0 ]]; then
      logging error "Zookeeper start from zoo${i}.cfg failed, please check ..."
      exit 0
    fi
    ${ZOOKEEPER_HOME}/bin/zkServer.sh status ${ZOOKEEPER_HOME}/conf/zoo${i}.cfg
  done
  logging info "Zookeeper started properly ..."
}

function prepare_packages() {
  if [[ -f ${HOME_DIR}/.prepared_packages ]]; then
    logging warn "Packages already prepared, skip prepare ..."
    return
  fi

  prepare_jdk
  init_jdk

  prepare_docker
  prepare_mysql
  prepare_metadata

  prepare_zookeeper
  init_zookeeper

  touch ${HOME_DIR}/.prepared_packages
  logging info "All need packages are ready ..."
}

function start_services_on_other() {
  start_zookeeper
}

function main() {
  prepare_packages
  start_services_on_other
}

main
