# How to deploy Kylin4 on EC2

Target: deploy Kylin4 on Ec2 with Spark Standalone mode

## Prerequisite

##### I. Clone or Download this repo

##### II. Download Packages & Upload them to S3 Path which suffix is */tars, example: `s3://xxx/kylin/tars`

> Note: Download packages for decreasing time of installation.

1. Download Kylin4 package by [official website](https://kylin.apache.org/download/)

2. Download Hadoop, [version 3.2.0](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz)

3. Download Spark with hadoop3.2, [version 3.1.1](https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz)

4. Download Hive, [version 2.3.9](https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz)

5. Download Zookeeper, [version 3.4.9](https://archive.apache.org/dist/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz)

6. Download JDK, [version 1.8_301](https://www.oracle.com/java/technologies/downloads/#java8),

> Note: if you download not match jdk version, please check the scripts/*.sh which variables about jdk!

##### III. Check dependent jars of Kylin4 in `./backup/jars` & Upload them to S3 Path which suffix is */jars, example: `s3://xxx/kylin/jars`

Kylin4 needed extra jars

- commons-configuration-1.3.jar
- mysql-connector-java-5.1.40.jar

##### IV. Check needed deploy scripts in `./backup/scripts` & Upload them to S3 Path which suffix is */scripts, example: `s3://xxx/kylin/scripts`

- prepare-ec2-env-for-distribution.sh
- prepare-ec2-env-for-master.sh
- prepare-ec2-env-for-slave.sh

##### V. Initialize `./kylin_configs.yaml`

Configure parameters in `./kylin_configs.yaml`

##### VI. Initialize needed IAM role and Used User which have access to aws

> Note: if IAM role has created, then set the role name to `cloudformation_templates/ec2-cluster-distribution.yaml`'s `Ec2OperationRole`

##### VII. Initialize needed `SecurityGroupIngress` in `cloudformation_templates/ec2_or_emr_vpc.yaml`

## Deploy

1. cd /path/to/deploy-kylin-on-aws

2. make virtual env for this repo

> Note: 
>   1. Use `source ./venv/bin/activate` to activate virtual env
>   2. Use `pip install -r ./requirements.txt` to install dependencies

2. configure aws account which has the access to aws

> Note: Use `aws configure` on terminal

3. Use `python ./deploy.py --type [deploy|destroy]` to deploy or destroy cluster.
