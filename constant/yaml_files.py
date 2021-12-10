from enum import Enum


class File(Enum):
    VPC_YAML = 'ec2_or_emr_vpc.yaml'
    DISTRIBUTION_YAML = 'ec2-cluster-distribution.yaml'
    # master yaml and slave yaml already contain the kylin4 and other needed services installed and run
    MASTER_YAML = 'ec2-cluster-master.yaml'
    SLAVE_YAML = 'ec2-cluster-slave.yaml'
    SLAVE_SCALE_YAML = 'ec2-cluster-slave-template.yaml'
