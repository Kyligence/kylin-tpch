from enum import Enum


class Config(Enum):
    # DEBUG params
    CLOUD_ADDR = 'CLOUD_ADDR'

    # Deploy params
    DEPLOY_KYLIN_VERSION = 'DEPLOY_KYLIN_VERSION'
    DEPLOY_PLATFORM = 'DEPLOY_PLATFORM'

    # Stack Names
    VPC_STACK = 'VPC_STACK'
    DISTRIBUTION_STACK = 'DISTRIBUTION_STACK'
    MASTER_STACK = 'MASTER_STACK'
    SLAVE_STACK = 'SLAVE_STACK'
    SLAVE_SCALE_WORKER = 'SLAVE_SCALE_{}_STACK'

    # Params
    EC2_DISTRIBUTION_PARAMS = 'EC2_DISTRIBUTION_PARAMS'
    EC2_MASTER_PARAMS = 'EC2_MASTER_PARAMS'
    EC2_SLAVE_PARAMS = 'EC2_SLAVE_PARAMS'
    EC2_SCALE_SLAVE_PARAMS = 'EC2_SCALE_SLAVE_PARAMS'