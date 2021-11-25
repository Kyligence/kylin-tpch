from enum import Enum


class File(Enum):
    VPC_YAML = 'ec2_or_emr_vpc.yaml'
    DISTRIBUTION_YAML = 'ec2-cluster-distribution.yaml'
    # master yaml and slave yaml already contain the kylin4 and other needed services installed and run
    MASTER_YAML = 'ec2-cluster-master.yaml'
    SLAVE_YAML = 'ec2-cluster-slave.yaml'
    SLAVE_SCALE_YAML = 'ec2-cluster-slave-template.yaml'

    EMR_FOR_KYLIN4_YAML = 'emr-5.33-for-kylin4.yaml'
    KYLIN4_ON_EMR_YAML = 'kylin4-on-emr-5.33.yaml'
