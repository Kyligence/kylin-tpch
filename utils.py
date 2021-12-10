"""
These mapping is for stack outputs params mapping to next-step stack inputs params
"""
from typing import List, Dict, Tuple

# ================ ec2 for kylin4 =====================
vpc_to_ec2_distribution = {
    'Subnet02ID': 'PublicSubnetID',
    'SecurityGroupID': 'SecurityGroupID'
}
ec2_distribution_from_vpc = {
    'PublicSubnetID': 'Subnet02ID',
    'SecurityGroupID': 'SecurityGroupId'
}

ec2_distribution_to_master = {
    'DistributionNodePrivateIp': 'Ec2ZookeeperkHost',
    'InstanceProfileId': 'Ec2InstanceProfile',
    'PublicSubnetIDDependsOnDNode': 'PublicSubnetID',
    'SecurityGroupIDDependsOnDNode': 'SecurityGroupID',
}

master_from_ec2_distribution = {
    'Ec2ZookeeperkHost': 'DistributionNodePrivateIp',
    'Ec2InstanceProfile': 'InstanceProfileId',
    'PublicSubnetID': 'PublicSubnetIDDependsOnDNode',
    'SecurityGroupID': 'SecurityGroupIDDependsOnDNode',
    'Ec2DbHost': 'DistributionNodePrivateIp',
}

master_to_slave = {
    'MasterEc2InstancePrivateIp': 'MasterNodeHost',
    'MasterSubnetIdDependsOnDNode': 'PublicSubnetID',
    'MasterSecurityGroupIdDependsOnDNode': 'SecurityGroupID',
    'MasterEc2InstanceProfileId': 'Ec2InstanceProfile',
}

slave_from_master = {
    'MasterNodeHost': 'MasterEc2InstancePrivateIp',
    'PublicSubnetID': 'MasterSubnetIdDependsOnDNode',
    'SecurityGroupID': 'MasterSecurityGroupIdDependsOnDNode',
    'Ec2InstanceProfile': 'MasterEc2InstanceProfileId',
}

step_for_ec2_to_scale = {
    'MasterNodeHost': 'MasterEc2InstancePrivateIp',
    'PublicSubnetID': 'MasterSubnetIdDependsOnDNode',
    'SecurityGroupID': 'MasterSecurityGroupIdDependsOnDNode',
    'Ec2InstanceProfile': 'MasterEc2InstanceProfileId',
}

# =========== util map ============
stack_to_map = {
    # Note: these keys must be matched in ./kylin_configs.yaml
    'ec2-or-emr-vpc-stack': ec2_distribution_from_vpc,
    'ec2-distribution-stack': master_from_ec2_distribution,
    'ec2-master-stack': slave_from_master,
}


def expand_stack(scale_nodes: Tuple) -> Dict:
    stack_keys = scaled_stacks(scale_nodes)
    if not stack_keys:
        return stack_to_map
    scaled_map = {k: step_for_ec2_to_scale for k in stack_keys}
    stack_to_map.update(scaled_map)
    return stack_to_map


def scaled_stacks(scale_nodes: Tuple) -> List:
    if not scale_nodes:
        return []

    scaled_stack_names = [f'ec2-slave-{i}' for i in scale_nodes]
    return scaled_stack_names


def read_template(file_path: str):
    with open(file=file_path, mode='r') as template:
        res_template = template.read()
    return res_template
