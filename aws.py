import logging
import os
import time
from typing import Optional, Dict

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
        self.yaml_path = os.path.join(os.path.dirname(__file__), 'cloudformation_templates')
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
        backup_command = 'mysqldump -h$(hostname -i) -uroot -p123456 --databases kylin hive ' \
                         '--add-drop-database >  /home/ec2-user/metadata-backup.sql'
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        self.exec_script_instance_and_return(name_or_id=instance_id, script=backup_command)
        cp_to_s3_command = f"aws s3 cp /home/ec2-user/metadata-backup.sql {config['BackupMetadataBucketFullPath']} " \
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

    def scale_up_workers(self, worker_num: int) -> Optional[Dict]:
        """
        add workers for kylin to scale spark worker
        :param worker_num: the worker mark
        :param master_addr: which master node to associated
        :return: worker private ip
        """
        if self._stack_complete(self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)]):
            logger.warning(f"{self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)]} "
                           f"already created complete.")
            return

            # Note: the stack name must be pre-step's
        params: dict = self._merge_params(
            stack_name=self.config[Config.MASTER_STACK.value],
            param_name=Config.EC2_SCALE_SLAVE_PARAMS.value,
            config=self.config,
        )
        params.update({'WorkerNum': worker_num})

        resp = self.create_stack(
            stack_name=self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)],
            file_path=os.path.join(self.yaml_path, File.SLAVE_SCALE_YAML.value),
            params=params
        )
        return resp

    def scale_down_worker(self, worker_num: int) -> Optional[Dict]:
        if not self._stack_delete_complete(self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)]):
            logger.warning(f"{self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)]} "
                           f"already terminated complete.")
            return
        stack_name = self.config[Config.SLAVE_SCALE_WORKER.value.format(worker_num)]
        resource_type = 'SlaveEc2InstanceId'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)

        backup_command = 'source ~/.bash_profile && ${SPARK_HOME}/sbin/decommission-worker.sh'
        self.exec_script_instance_and_return(name_or_id=instance_id, script=backup_command)
        # FIXME: hard code for sleep spark worker to execute remaining jobs
        # sleep 5 min to ensure all jobs in decommissioned workers are done
        time.sleep(60 * 5)

        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.delete_stack(stack_name)
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

    @staticmethod
    def scale_worker_to_ec2(worker_num: int, config: dict):
        if config[Config.DEPLOY_PLATFORM.value] != 'ec2':
            msg = f'Not supported platform: {config[Config.DEPLOY_PLATFORM.value]}.'
            logger.error(msg)
            raise Exception(msg)
        cloud_instance = AWSInstance(config)
        cloud_instance.scale_up_workers(worker_num)

    @staticmethod
    def scale_down_worker(worker_num: int, config: dict):
        if config[Config.DEPLOY_PLATFORM.value] != 'ec2':
            msg = f'Not supported platform: {config[Config.DEPLOY_PLATFORM.value]}.'
            logger.error(msg)
            raise Exception(msg)
        cloud_instance = AWSInstance(config)
        cloud_instance.scale_down_worker(worker_num)
