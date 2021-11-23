from enum import Enum


class Client(Enum):
    CLOUD_FORMATION = 'cloudformation'
    EC2 = 'ec2'
    EMR = 'emr'


class StackStatus(Enum):
    CREATE_FAILED = 'CREATE_FAILED'
    ROLLBACK_IN_PROGRESS = 'ROLLBACK_IN_PROGRESS'
    ROLLBACK_FAILED = 'ROLLBACK_FAILED'
    ROLLBACK_COMPLETE = 'ROLLBACK_COMPLETE'
