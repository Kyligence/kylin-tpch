from enum import Enum


class File(Enum):
    CONFIG_YAML = 'kylin_configs.yaml'
    VPC_YAML = 'ec2-or-emr-vpc.yaml'
    RDS_YAML = 'ec2-cluster-rds.yaml'
    STATIC_SERVICE_YAML = 'ec2-cluster-static-services.yaml'
    ZOOKEEPERS_SERVICE_YAML = 'ec2-cluster-zk.yaml'
    KYLIN4_YAML = 'ec2-cluster-kylin4.yaml'
    KYLIN_SCALE_YAML = 'ec2-cluster-kylin4-template.yaml'
    SPARK_MASTER_YAML = 'ec2-cluster-spark-master.yaml'
    SPARK_WORKER_YAML = 'ec2-cluster-spark-slave.yaml'
    SPARK_WORKER_SCALE_YAML = 'ec2-cluster-spark-slave-template.yaml'
