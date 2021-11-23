"""
These mapping is for stack outputs params mapping to next-step stack inputs params
"""
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

# =========== emr for kylin4 ============
vpc_to_emr_for_kylin4 = {
    'Subnet01ID': 'SubnetId',
    'SecurityGroupId': 'EmrSecurityGroupId',
}

emr_from_vpc_for_kylin4 = {
    'SubnetId': 'Subnet01ID',
    'EmrSecurityGroupId': 'SecurityGroupId'
}

emr_to_step_for_kylin4 = {
    'ClusterId': 'EMRClusterId'
}

step_from_emr_for_kylin4 = {
    'EMRClusterId': 'ClusterId'
}

step_for_ec2_to_scale = {
    'MasterNodeHost': 'MasterEc2InstancePrivateIp',
    'PublicSubnetID': 'MasterSubnetIdDependsOnDNode',
    'SecurityGroupID': 'MasterSecurityGroupIdDependsOnDNode',
    'Ec2InstanceProfile': 'MasterEc2InstanceProfileId',
}

step_for_raven_client = {
    'MasterNodeHost': 'MasterEc2InstancePrivateIp',
    'PublicSubnetID': 'MasterSubnetIdDependsOnDNode',
    'SecurityGroupID': 'MasterSecurityGroupIdDependsOnDNode',
    'Ec2InstanceProfile': 'MasterEc2InstanceProfileId',
}
# =========== emr for kylin3 ==============
# TODO: fill this

# =========== util map ============
stack_to_map = {
    # Note: these keys must be matched in ./kylin_configs.yaml
    'ec2-or-emr-vpc-stack': ec2_distribution_from_vpc,
    'ec2-distribution-stack': master_from_ec2_distribution,
    'ec2-master-stack': slave_from_master,
    'ec2-slave-04': step_for_ec2_to_scale,
    'ec2-slave-05': step_for_ec2_to_scale,
    'ec2-slave-06': step_for_ec2_to_scale,
}


def read_template(file_path: str):
    with open(file=file_path, mode='r') as template:
        cf_template = template.read()
    return cf_template
