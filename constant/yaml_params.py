from enum import Enum


class Params(Enum):
    # global params
    ASSOSICATED_PUBLIC_IP = 'ASSOSICATED_PUBLIC_IP'
    ALWAYS_DESTROY_ALL = 'ALWAYS_DESTROY_ALL'
    S3_FULL_BUCKET_PATH = 'S3_FULL_BUCKET_PATH'
    INSTANCE_ID = 'IdOfInstance'

    # bucket path of stack
    BUCKET_FULL_PATH = 'BucketFullPath'
    BUCKET_PATH = 'BucketPath'

    # Vpc params
    VPC_ID = 'VpcId'
    SUBNET_ID = 'SubnetId'
    SECURITY_GROUP = 'SecurityGroupId'
    INSTANCE_PROFILE = 'InstanceProfileId'
    SUBNET_GROUP_NAME = 'SubnetGroupName'

    # Rds params
    DB_HOST = 'DbHost'

    # Static services params
    STATIC_SERVICES_PRIVATE_IP = 'StaticServicesNodePrivateIp'
    STATIC_SERVICES_PUBLIC_IP = 'StaticServicesNodePublicIp'
    STATIC_SERVICES_NAME = 'StaticServices Node'

    # Zookeeper params
    ZOOKEEPER_NAME = 'ZookeeperNode0{num}'
    ZOOKEEPER_IP = 'ZookeeperNode0{num}PrivateIp'
    ZOOKEEPER_PUB_IP = 'ZookeeperNode0{num}PublicIp'
    ZOOKEEPER_INSTANCE_ID = 'IdOfInstanceZookeeper0{num}'
    ZOOKEEPER_HOSTS = 'ZookeepersHost'

    # Kylin 4 params
    KYLIN_NAME = 'Kylin Node'
    KYLIN4_PUBLIC_IP = 'Kylin4Ec2InstancePublicIp'
    KYLIN4_PRIVATE_IP = 'Kylin4Ec2InstancePrivateIp'
    KYLIN_SCALE_STACK_NAME = 'ec2-kylin-scale-{num}'
    KYLIN_SCALE_NODE_NAME = 'Kylin Node Scale {num}'

    # Spark master params
    SPARK_MASTER_HOST = 'SparkMasterNodeHost'
    SPARK_MASTER_NAME = 'Spark Master Node'
    SPARK_PUB_IP = 'SparkMasterEc2InstancePublicIp'

    # Spark slave params
    SPARK_WORKER_NAME = 'Spark Slave {num}'
    SPARK_SCALE_WORKER_NAME = 'Spark Slave Node Scale {num}'
    SPARK_WORKER_ID = 'IdOfInstanceSlave0{num}'
    SPARK_WORKER_PRIVATE_IP = 'Slave0{num}Ec2InstancePrivateIp'
    SPARK_WORKER_PUBLIC_IP = 'Slave0{num}Ec2InstancePublicIp'
    SPARK_SCALED_WORKER_PRIVATE_IP = 'SlaveEc2InstancePrivateIp'
    SPARK_SCALED_WORKER_PUBLIC_IP = 'SlaveEc2InstancePublicIp'

    # Spark scaled slave params
    SPARK_WORKER_NUM = 'WorkerNum'
    SPARK_WORKER_SCALE_STACK_NAME = 'ec2-spark-slave-scale-{num}'
