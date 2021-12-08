import logging
import os
import time
from typing import Optional, Dict, List

import boto3
from botocore.exceptions import ClientError, WaiterError

from constant.client import Client
from constant.config import Config
from constant.yaml_files import File
from utils import stack_to_map, read_template

logger = logging.getLogger(__name__)


class AWSInstance:

    def __init__(self, config):
        # DEPLOY_PLATFORM
        self.config = config
        self.region = config['AWS_REGION']
        self.cf_client = boto3.client(Client.CLOUD_FORMATION.value, region_name=self.region)
        self._init_ec2_env()

    def _init_ec2_env(self):
        self.ec2_client = boto3.client(Client.EC2.value, region_name=self.region)
        self.ssm_client = boto3.client('ssm', region_name=self.region)
        self.cur_dir = os.path.dirname(__file__)
        self.yaml_path = os.path.join(self.cur_dir, 'cloudformation_templates')
        self.create_complete_waiter = self.cf_client.get_waiter('stack_create_complete')
        self.delete_complete_waiter = self.cf_client.get_waiter('stack_delete_complete')

    def create_vpc_stack(self) -> Optional[Dict]:
        if self._stack_complete(self.config[Config.VPC_STACK.value]):
            logger.warning(f"{self.config[Config.VPC_STACK.value]} already created complete.")
            return
        resp = self.create_stack(
            stack_name=self.config[Config.VPC_STACK.value],
            file_path=os.path.join(self.yaml_path, File.VPC_YAML.value),
            params={}
        )
        return resp

    def terminate_vpc_stack(self) -> Optional[Dict]:
        if self._stack_delete_complete(self.config[Config.VPC_STACK.value]):
            logger.warning(f"{self.config[Config.VPC_STACK.value]} already terminated complete.")
            return

        resp = self.delete_stack(self.config[Config.VPC_STACK.value])
        return resp

    def create_distribution_stack(self) -> Optional[Dict]:
        if self._stack_complete(self.config[Config.DISTRIBUTION_STACK.value]):
            logger.warning(f"{self.config[Config.DISTRIBUTION_STACK.value]} already created complete.")
            return
        if not self._stack_complete(self.config[Config.VPC_STACK.value]):
            logger.warning(f"{self.config[Config.VPC_STACK.value]} Must be created complete "
                           f"before create {self.config[Config.DISTRIBUTION_STACK.value]}.")
            raise Exception(f"{self.config[Config.VPC_STACK.value]} Must be created complete "
                            f"before create {self.config[Config.DISTRIBUTION_STACK.value]}.")
        # Note: the stack name must be pre-step's
        params: dict = self._merge_params(
            stack_name=self.config[Config.VPC_STACK.value],
            param_name=Config.EC2_DISTRIBUTION_PARAMS.value,
            config=self.config
        )
        resp = self.create_stack(
            stack_name=self.config[Config.DISTRIBUTION_STACK.value],
            file_path=os.path.join(self.yaml_path, File.DISTRIBUTION_YAML.value),
            params=params,
            capability='CAPABILITY_NAMED_IAM'
        )
        return resp

    def terminate_distribution_stack(self) -> Optional[Dict]:
        if self._stack_delete_complete(self.config[Config.DISTRIBUTION_STACK.value]):
            logger.warning(f"{self.config[Config.DISTRIBUTION_STACK.value]} already terminated complete.")
            return
        self.backup_metadata_before_ec2_terminate(
            stack_name=self.config[Config.DISTRIBUTION_STACK.value],
            config=self.config
        )
        resp = self.delete_stack(stack_name=self.config[Config.DISTRIBUTION_STACK.value])
        return resp

    def create_master_stack(self) -> Optional[Dict]:
        if self._stack_complete(self.config[Config.MASTER_STACK.value]):
            logger.warning(f"{self.config[Config.MASTER_STACK.value]} already created complete.")
            return
        if not self._stack_complete(self.config[Config.DISTRIBUTION_STACK.value]):
            logger.warning(f"{self.config[Config.DISTRIBUTION_STACK.value]} Must be created complete "
                           f"before create {self.config[Config.MASTER_STACK.value]}.")
            raise Exception(f"{self.config[Config.DISTRIBUTION_STACK.value]} Must be created complete "
                            f"before create {self.config[Config.MASTER_STACK.value]}.")
        # Note: the stack name must be pre-step's
        params: dict = self._merge_params(
            stack_name=self.config[Config.DISTRIBUTION_STACK.value],
            param_name=Config.EC2_MASTER_PARAMS.value,
            config=self.config,
        )
        resp = self.create_stack(
            stack_name=self.config[Config.MASTER_STACK.value],
            file_path=os.path.join(self.yaml_path, File.MASTER_YAML.value),
            params=params
        )
        return resp

    def terminate_master_stack(self) -> Optional[Dict]:
        if self._stack_delete_complete(self.config[Config.MASTER_STACK.value]):
            logger.warning(f"{self.config[Config.MASTER_STACK.value]} already terminated complete.")
            return

        resp = self.delete_stack(stack_name=self.config[Config.MASTER_STACK.value])
        return resp

    def create_slave_stack(self) -> Optional[Dict]:
        if self._stack_complete(self.config[Config.SLAVE_STACK.value]):
            logger.warning(f"{self.config[Config.SLAVE_STACK.value]} already created complete.")
            return
        if not self._stack_complete(self.config[Config.MASTER_STACK.value]):
            logger.warning(f"{self.config[Config.MASTER_STACK.value]} Must be created complete "
                           f"before create {self.config[Config.SLAVE_STACK.value]}.")
            raise Exception(f"{self.config[Config.MASTER_STACK.value]} Must be created complete "
                            f"before create {self.config[Config.SLAVE_STACK.value]}.")
        # Note: the stack name must be pre-step's
        params: dict = self._merge_params(
            stack_name=self.config[Config.MASTER_STACK.value],
            param_name=Config.EC2_SLAVE_PARAMS.value,
            config=self.config,
        )
        resp = self.create_stack(
            stack_name=self.config[Config.SLAVE_STACK.value],
            file_path=os.path.join(self.yaml_path, File.SLAVE_YAML.value),
            params=params
        )
        return resp

    def terminate_slave_stack(self) -> Optional[Dict]:
        if self._stack_delete_complete(self.config[Config.SLAVE_STACK.value]):
            logger.warning(f"{self.config[Config.SLAVE_STACK.value]} already terminated complete.")
            return

        resp = self.delete_stack(stack_name=self.config[Config.SLAVE_STACK.value])
        return resp

    def backup_metadata_before_ec2_terminate(self, stack_name: str, config: dict) -> Optional[Dict]:
        if stack_name != config[Config.DISTRIBUTION_STACK.value]:
            logger.warning(f"Only {config[Config.DISTRIBUTION_STACK.value]} should backup before terminate.")
            return
        tn = time.time_ns()
        backup_command = f'mysqldump -h$(hostname -i) -uroot -p123456 --databases kylin hive ' \
                         f'--add-drop-database >  /home/ec2-user/metadata-backup-{tn}.sql'
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        self.exec_script_instance_and_return(name_or_id=instance_id, script=backup_command)
        cp_to_s3_command = f"aws s3 cp /home/ec2-user/metadata-backup-{tn}.sql {config['BackupMetadataBucketFullPath']} " \
                           f"--region {config['AWS_REGION']}"
        self.exec_script_instance_and_return(name_or_id=instance_id, script=cp_to_s3_command)

    def create_stack(self, stack_name: str, file_path: str, params: dict, capability: str = None) -> Dict:
        if capability:
            resp = self.cf_client.create_stack(
                StackName=stack_name,
                TemplateBody=read_template(file_path),
                Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
                Capabilities=[capability]
            )
        else:
            resp = self.cf_client.create_stack(
                StackName=stack_name,
                TemplateBody=read_template(file_path),
                Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
            )

        assert self._stack_complete(stack_name=stack_name), \
            f"Stack {stack_name} not create complete, pls check."
        return resp

    def delete_stack(self, stack_name: str) -> Dict:
        resp = self.cf_client.delete_stack(StackName=stack_name)
        return resp

    def start_prometheus(self, stack_name: str) -> None:
        if stack_name != self.config[Config.DISTRIBUTION_STACK.value]:
            logger.warning(f"Only {self.config[Config.DISTRIBUTION_STACK.value]} has the prometheus server.")
            return
        self.refresh_prometheus_config(stack_name=stack_name)
        self.start_prometheus_server(stack_name=stack_name)

    def start_prometheus_server(self, stack_name: str) -> None:
        start_command = 'nohup /home/ec2-user/prometheus/prometheus --config.file=/home/ec2-user/prometheus/prometheus.yml >> /home/ec2-user/prometheus/nohup.log 2>&1 &'
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        self.exec_script_instance_and_return(name_or_id=instance_id, script=start_command)

    def stop_prometheus_server(self, stack_name: str) -> None:
        stop_command = 'lsof -t -i:9090 | xargs kill -9'
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        self.exec_script_instance_and_return(name_or_id=instance_id, script=stop_command)

    def restart_prometheus_server(self, stack_name: str) -> None:
        self.stop_prometheus_server(stack_name=stack_name)
        self.start_prometheus_server(stack_name=stack_name)

    def refresh_prometheus_config(self, stack_name: str) -> None:
        refresh_config_commands = self.refresh_prometheus_commands()
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        for command in refresh_config_commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    def refresh_prometheus_commands(self) -> List:
        # TODO: fill all command for prometheus server config
        params = self.prometheus_config()
        # NOTE: the spaces in template is for prometheus config's indent
        command_template = """echo '  - job_name: "{node}"\n    static_configs:\n    - targets: ["{host}:9100"]' >> /home/ec2-user/prometheus/prometheus.yml """
        commands = [command_template.format(node=node, host=host) for node, host in params.items()]
        return commands

    def prometheus_config(self) -> Dict:
        ips = self.get_all_node_ips()
        params = ['distribution_node', 'master_node', 'slave01_node', 'slave02_node', 'slave03_node']
        param_map = dict(zip(params, ips))
        # TODO: fill all prometheus server config
        return param_map

    def get_all_node_ips(self) -> List:
        res = []
        distribution_private_ip = self.get_specify_resource_from_output(
            self.config[Config.DISTRIBUTION_STACK.value], 'DistributionNodePrivateIp')
        master_private_ip = self.get_specify_resource_from_output(
            self.config[Config.MASTER_STACK.value], 'MasterEc2InstancePrivateIp'
        )
        slaves_private_ips = [
            self.get_specify_resource_from_output(
                self.config[Config.SLAVE_STACK.value], f'Slave0{i}Ec2InstancePrivateIp'
            )
            for i in range(1, 4)  # default slave nodes' num is 3
        ]
        res.append(distribution_private_ip)
        res.append(master_private_ip)
        res.extend(slaves_private_ips)
        return res

    def get_specify_resource_from_output(self, stack_name: str, resource_type: str) -> str:
        output = self.get_stack_output(stack_name)
        return output[resource_type]

    def get_stack_output(self, stack_name: str) -> Dict:
        is_complete = self._stack_complete(stack_name)
        if not is_complete:
            raise Exception(f"{stack_name} is not complete, please check.")
        output = self.cf_client.describe_stacks(StackName=stack_name)
        """current output format:
        {
            'Stacks' : [{
                            ...,
                            Outputs: [
                                        {
                                            'OutputKey': 'xxx',
                                            'OutputValue': 'xxx',
                                            'Description': ...    
                                        }, 
                                        ...]
                        }],
            'ResponseMEtadata': {...}
        }
        """
        handled_outputs = {entry['OutputKey']: entry['OutputValue']
                           for entry in list(output['Stacks'][0]['Outputs'])}

        return handled_outputs

    def is_ec2_stack_ready(self) -> bool:
        if not (
            self._stack_complete(self.config[Config.VPC_STACK.value])
            and self._stack_complete(self.config[Config.DISTRIBUTION_STACK.value])
            and self._stack_complete(self.config[Config.MASTER_STACK.value])
            and self._stack_complete(self.config[Config.SLAVE_STACK.value])
        ):
            return False
        return True

    def is_ec2_stack_terminated(self) -> bool:
        deleted_cost_stacks: bool = (
                self._stack_delete_complete(self.config[Config.DISTRIBUTION_STACK.value])
                and self._stack_delete_complete(self.config[Config.MASTER_STACK.value])
                and self._stack_delete_complete(self.config[Config.SLAVE_STACK.value]))
        if deleted_cost_stacks and \
                ((not self.config['ALWAYS_DESTROY_ALL'])
                 or (self._stack_delete_complete(self.config[Config.VPC_STACK.value]))):
            return True
        return False

    def send_command(self, **kwargs) -> Dict:
        instance_ids = kwargs['vm_name']
        script = kwargs['script']
        document_name = "AWS-RunShellScript"
        parameters = {'commands': [script]}
        response = self.ssm_client.send_command(
            InstanceIds=instance_ids,
            DocumentName=document_name,
            Parameters=parameters
        )
        return response

    def get_command_invocation(self, command_id, instance_id) -> Dict:
        response = self.ssm_client.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        return response

    def exec_script_instance_and_return(self, name_or_id: str, script: str, timeout: int = 20) -> Dict:
        vm_name = None
        if isinstance(name_or_id, str):
            vm_name = [name_or_id]
        response = self.send_command(vm_name=vm_name, script=script)
        command_id = response['Command']['CommandId']
        time.sleep(5)
        start = time.time()
        output = None
        while time.time() - start < timeout * 60:
            output = self.get_command_invocation(
                command_id=command_id,
                instance_id=name_or_id,
            )
            if output['Status'] in ['Delayed', 'Success', 'Cancelled', 'TimedOut', 'Failed']:
                break
            time.sleep(10)
        assert output and output['Status'] == 'Success', \
            f"execute script failed, failed info: {output['StandardErrorContent']}"

    def stop_ec2_instance(self, instance_id: str):
        self.ec2_client.stop_instances(
            InstanceIds=[
                instance_id,
            ],
            Force=True
        )

    def stop_ec2_instances(self, instance_ids: list):
        self.ec2_client.stop_instances(
            InstanceIds=instance_ids,
            Force=True
        )

    def start_ec2_instance(self, instance_id: str) -> Dict:
        resp = self.ec2_client.start_instances(
            InstanceIds=[instance_id]
        )
        return resp

    def start_ec2_instances(self, instance_ids: list) -> Dict:
        resp = self.ec2_client.start_instances(
            InstanceIds=instance_ids,
        )
        return resp

    def _stack_exists(self, stack_name: str, required_status: str = 'CREATE_COMPLETE') -> bool:
        return self._stack_status_check(name_or_id=stack_name, status=required_status)

    def _stack_deleted(self, stack_name: str, required_status: str = 'DELETE_COMPLETE') -> bool:
        return self._stack_status_check(name_or_id=stack_name, status=required_status)

    def _stack_status_check(self, name_or_id: str, status: str) -> bool:
        try:
            resp: dict = self.cf_client.describe_stacks(StackName=name_or_id)
        except ClientError:
            return False
        return resp['Stacks'][0]['StackStatus'] == status

    def _stack_complete(self, stack_name: str) -> bool:
        try:
            self.create_complete_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 120
                }
            )
        except WaiterError as wx:
            logger.error(wx)
            return False
        return True

    def _stack_delete_complete(self, stack_name: str) -> bool:
        try:
            self.delete_complete_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 60,
                    'MaxAttempts': 2
                }
            )
        except WaiterError as wx:
            logger.error(wx)
            return False
        return True

    def _merge_params(self, stack_name: str, param_name: str, config: dict) -> dict:
        # this stack name is pre-stack
        output: dict = self.get_stack_output(stack_name)
        params: dict = config[param_name]

        # stack output mapping relationship
        relate_map = stack_to_map[stack_name]
        for k, v in params.items():
            # if params hasn't default value, use the pre-step output value to fill the param
            if v:
                continue
            params[k] = output[relate_map[k]]
        return params


class AWS:
    @staticmethod
    def aws_ec2_cluster(config) -> Optional[Dict]:
        cloud_instance = AWSInstance(config)
        if not cloud_instance.is_ec2_stack_ready():
            cloud_instance.create_vpc_stack()
            cloud_instance.create_distribution_stack()
            cloud_instance.create_master_stack()
            cloud_instance.create_slave_stack()
        # now start monitor of prometheus because of every node_exporter ips are known
        cloud_instance.start_prometheus(config[Config.DISTRIBUTION_STACK.value])
        # return the master stack resources
        resources = cloud_instance.get_stack_output(config[Config.MASTER_STACK.value])
        return resources

    @staticmethod
    def terminate_ec2_cluster(config) -> Optional[Dict]:
        cloud_instance = AWSInstance(config)
        if cloud_instance.is_ec2_stack_terminated():
            logger.warning('ec2 stack already deleted.')
            return
        cloud_instance.terminate_slave_stack()
        cloud_instance.terminate_master_stack()
        cloud_instance.terminate_distribution_stack()
        if config['ALWAYS_DESTROY_ALL'] is True:
            cloud_instance.terminate_vpc_stack()
        # don't need to terminate vpc stack, because it's free resource on your aws if don't use it.
        # after terminated all node check again.
        assert cloud_instance.is_ec2_stack_terminated() is True

    @staticmethod
    def aws_cloud(config: Dict) -> str:
        if config[Config.DEPLOY_PLATFORM.value] != 'ec2':
            msg = f'Not supported platform: {config[Config.DEPLOY_PLATFORM.value]}.'
            logger.error(msg)
            raise Exception(msg)

        resource = AWS.aws_ec2_cluster(config)
        # only get the master dns
        # FIXME: fix hard code and get method
        if config[Config.EC2_MASTER_PARAMS.value]['AssociatedPublicIp'] == 'true':
            return resource.get('MasterEc2InstancePublicIp')
        return resource.get('MasterEc2InstancePrivateIp')

    @staticmethod
    def destroy_aws_cloud(config):
        if config[Config.DEPLOY_PLATFORM.value] != 'ec2':
            msg = f'Not supported platform: {config[Config.DEPLOY_PLATFORM.value]}.'
            logger.error(msg)
            raise Exception(msg)

        AWS.terminate_ec2_cluster(config)
