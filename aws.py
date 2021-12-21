import logging
import os
import re
import time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError, WaiterError

from constant.client import Client
from constant.config import Config
from constant.yaml_files import File
from utils import stack_to_map, read_template, expand_stack, scaled_stacks

logger = logging.getLogger(__name__)


class AWSInstance:
    SCALE_STACK_NAME_TEMPLATE = 'ec2-slave-{}'
    # NOTE: the spaces in template is for prometheus config's indent
    COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    static_configs:\n    - targets: ["{host}:9100"]' >> /home/ec2-user/prometheus/prometheus.yml """
    # Special COMMAND_TEMPLATE for spark metrics into prometheus
    SPARK_MASTER_COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/master/prometheus\n    static_configs:\n    - targets: ["{host}:8080"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_APPLICATIONS_COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/applications/prometheus\n    static_configs:\n    - targets: ["{host}:8080"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_DRIVER_COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/prometheus\n    static_configs:\n    - targets: ["{host}:4040"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_WORKER_COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/prometheus\n    static_configs:\n    - targets: ["{host}:4041"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_EXECUTORS_COMMAND_TEMPLATE = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/executors/prometheus\n    static_configs:\n    - targets: ["{host}:4040"]' >> /home/ec2-user/prometheus/prometheus.yml """

    cf_client = None

    def __init__(self, config):
        # DEPLOY_PLATFORM
        self.config = config
        self.region = config['AWS_REGION']
        self.cf_client = boto3.client(Client.CLOUD_FORMATION.value, region_name=self.region)
        self._init_ec2_env()
        self.scaled_nodes: Tuple = literal_eval(config['SCALE_NODES']) if config['SCALE_NODES'] else []
        self.stack_map = expand_stack(self.scaled_nodes) if self.scaled_nodes else stack_to_map

    def _init_ec2_env(self):
        self.ec2_client = boto3.client(Client.EC2.value, region_name=self.region)
        self.ssm_client = boto3.client('ssm', region_name=self.region)
        self.cur_dir = os.path.dirname(__file__)
        self.yaml_path = os.path.join(self.cur_dir, 'cloudformation_templates')
        self.create_complete_waiter = self.cf_client.get_waiter('stack_create_complete')
        self.delete_complete_waiter = self.cf_client.get_waiter('stack_delete_complete')
        self.exists_waiter = self.cf_client.get_waiter('stack_exists')

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

        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)

        if self.is_ec2_instance_stopped(instance_id):
            msg = f'Current instance {instance_id} of {stack_name} is not running, ' \
                  f'please start the instance to backup metadata.'
            logger.warning(msg)
            raise Exception(msg)

        tn = time.time_ns()
        backup_command = f'mysqldump -h$(hostname -i) -uroot -p123456 --databases kylin hive ' \
                         f'--add-drop-database >  /home/ec2-user/metadata-backup-{tn}.sql'

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
        self.refresh_prometheus_param_map(stack_name=stack_name)
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

    def refresh_prometheus_param_map(self, stack_name: str) -> None:
        refresh_config_commands = self.refresh_prometheus_commands()
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        for command in refresh_config_commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

        # Special support spark metrics into prometheus
        spark_config_commands = self.refresh_spark_metrics_commands()
        for command in spark_config_commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    def refresh_prometheus_commands(self) -> List:
        # TODO: fill all command for prometheus server config
        params = self.prometheus_param_map()
        # NOTE: the spaces in template is for prometheus config's indent
        commands = [self.COMMAND_TEMPLATE.format(node=node, host=host) for node, host in params.items()]
        return commands

    def refresh_spark_metrics_commands(self) -> List:
        master_private_ip = self._get_node_ip(self.config[Config.MASTER_STACK.value], 'MasterEc2InstancePrivateIp')
        commands = [
            self.SPARK_DRIVER_COMMAND_TEMPLATE.format(node='spark_driver', host=master_private_ip),
            self.SPARK_WORKER_COMMAND_TEMPLATE.format(node='spark_worker', host=master_private_ip),
            self.SPARK_APPLICATIONS_COMMAND_TEMPLATE.format(node='spark_applications', host=master_private_ip),
            self.SPARK_MASTER_COMMAND_TEMPLATE.format(node='spark_master', host=master_private_ip),
            self.SPARK_EXECUTORS_COMMAND_TEMPLATE.format(node='spark_executors', host=master_private_ip),
        ]
        return commands

    def refresh_prometheus_commands_after_scale(self, worker_nums: List) -> List:
        params = self.after_scale_prometheus_params_map(worker_nums)
        commands = [self.COMMAND_TEMPLATE.format(node=node, host=host) for node, host in params.items()]
        return commands

    def prometheus_param_map(self) -> Dict:
        ips = self.get_all_node_ips()
        params = ['distribution_node', 'master_node', 'slave01_node', 'slave02_node', 'slave03_node']
        param_map = dict(zip(params, ips))
        # TODO: fill all prometheus server config
        return param_map

    def after_scale_prometheus_params_map(self, worker_nums: List) -> Dict:
        ips = self.get_scale_node_ips(worker_nums)
        params = [f'slave{worker}-node' for worker in worker_nums]
        param_map = dict(zip(params, ips))
        return param_map

    def check_prometheus_config_after_scale_up(self, stack_name: str, worker_nums: List) -> List:
        workers_param_map = self.after_scale_prometheus_params_map(worker_nums)
        exists_workers_command = '''grep -Fq "{node}" /home/ec2-user/prometheus/prometheus.yml; echo $?'''
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        not_exists_workers = []
        for worker in workers_param_map.keys():
            command = exists_workers_command.format(node=worker)
            output = self.exec_script_instance_and_return(name_or_id=instance_id, script=command)
            # output['StandardOutputContent'] = '1\n' means the node not exists in the prometheus config
            if output['StandardOutputContent'] == '1\n':
                not_exists_workers.extend(re.findall(r'\d+', worker))
        return not_exists_workers

    def check_prometheus_config_after_scale_down(self, stack_name: str, worker_nums: List) -> List:
        workers_param_map = self.after_scale_prometheus_params_map(worker_nums)
        exists_workers_command = '''grep -Fq "{node}" /home/ec2-user/prometheus/prometheus.yml; echo $?'''
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        exists_workers = []
        for worker in workers_param_map.keys():
            command = exists_workers_command.format(node=worker)
            output = self.exec_script_instance_and_return(name_or_id=instance_id, script=command)
            # output['StandardOutputContent'] = '0\n' means the node exists in the prometheus config
            if output['StandardOutputContent'] == '0\n':
                exists_workers.append(worker)
        return exists_workers

    def refresh_prometheus_config_after_scale_up(self, stack_name: str, worker_nums: List) -> None:
        commands = self.refresh_prometheus_commands_after_scale(worker_nums)
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    def refresh_prometheus_config_after_scale_down(self, stack_name: str, exists_workers: List) -> None:
        commands_template = '''sed -i "/{node}/,+2d" /home/ec2-user/prometheus/prometheus.yml'''
        resource_type = 'Ec2InstanceIdOfDistributionNode'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)
        commands = [commands_template.format(node=worker) for worker in exists_workers]
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    def get_all_node_ips(self) -> List:
        res = []
        distribution_private_ip = self._get_node_ip(
            self.config[Config.DISTRIBUTION_STACK.value],
            'DistributionNodePrivateIp'
        )
        master_private_ip = self._get_node_ip(self.config[Config.MASTER_STACK.value], 'MasterEc2InstancePrivateIp')
        slaves_private_ips = [
            self._get_node_ip(self.config[Config.SLAVE_STACK.value], f'Slave0{i}Ec2InstancePrivateIp')
            for i in range(1, 4)  # default slave nodes' num is 3
        ]
        res.append(distribution_private_ip)
        res.append(master_private_ip)
        res.extend(slaves_private_ips)
        return res

    def _get_node_ip(self, stack_name: str, private_ip_key: str) -> str:
        return self.get_specify_resource_from_output(stack_name, private_ip_key)

    def get_scale_node_ips(self, worker_nums: List) -> List:
        scale_private_ips = [
            self.get_specify_resource_from_output(
                self.SCALE_STACK_NAME_TEMPLATE.format(worker),
                'SlaveEc2InstancePrivateIp'
            ) for worker in worker_nums
        ]
        return scale_private_ips

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

    def alive_workers(self) -> None:
        workers = []
        basic_stack_names = [
            self.config[Config.DISTRIBUTION_STACK.value],
            self.config[Config.MASTER_STACK.value],
            self.config[Config.SLAVE_STACK.value],
        ]
        # basic instances check
        distribution_stack_name = self.config[Config.DISTRIBUTION_STACK.value]
        if self._stack_exists(stack_name=distribution_stack_name):
            instance_id = self.get_specify_resource_from_output(
                distribution_stack_name, 'Ec2InstanceIdOfDistributionNode'
            )
            workers.append(f'{distribution_stack_name}\t\t{instance_id}\n')

        master_stack_name = self.config[Config.MASTER_STACK.value]
        if self._stack_exists(stack_name=master_stack_name):
            instance_id = self.get_specify_resource_from_output(
                master_stack_name, 'MasterEc2InstanceId'
            )
            workers.append(f'{master_stack_name}\t\t{instance_id}\n')

        slave_stack_name = self.config[Config.SLAVE_STACK.value]
        if self._stack_exists(stack_name=slave_stack_name):
            for index, resource_type in enumerate(['Slave01Ec2InstanceId', 'Slave02Ec2InstanceId', 'Slave03Ec2InstanceId']):
                instance_id = self.get_specify_resource_from_output(
                    slave_stack_name, resource_type
                )
                workers.append(f'{slave_stack_name}-slave0{index+1}\t\t{instance_id}\n')

        scale_nodes = scaled_stacks(self.scaled_nodes)
        for scale_stack in scale_nodes:
            if self._stack_exists(stack_name=scale_stack):
                instance_id = self.get_specify_resource_from_output(
                    scale_stack, 'SlaveEc2InstanceId'
                )
                workers.append(f'{scale_stack}\t\t{instance_id}\n')

        result = f"Alive Nodes List:\nStack Name\t\tInstance ID\n"
        for w in workers:
            result += w
        logger.info(result)
        logger.info("Alive Nodes List Done!")

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
        if not deleted_cost_stacks:
            return False
        if not self.config['ALWAYS_DESTROY_ALL'] \
                or self._stack_delete_complete(self.config[Config.VPC_STACK.value]):
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
        return output

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
    
    def ec2_instance_statuses(self, instance_ids: List) -> Dict:
        resp = self.ec2_client.describe_instance_status(
            Filters=[{
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'shutting-down', 'terminated', 'stopping', 'stopped'],
            }],
            InstanceIds=instance_ids,
            # Note: IncludeAllInstances (boolean), Default is false.
            # When true , includes the health status for all instances.
            # When false , includes the health status for running instances only.
            IncludeAllInstances=True,
        )
        return resp['InstanceStatuses']

    def ec2_instance_status(self, instance_id: str) -> str:
        resp = self.ec2_instance_statuses(instance_ids=[instance_id])
        assert resp, 'Instance statuses must be not empty.'
        return resp[0]['InstanceState']['Name']

    def is_ec2_instance_running(self, instance_id: str) -> bool:
        return self.ec2_instance_status(instance_id) == 'running'

    def is_ec2_instance_stopped(self, instance_id: str) -> bool:
        return self.ec2_instance_status(instance_id) == 'stopped'

    def scale_up_worker(self, worker_num: int) -> Optional[Dict]:
        """
        add workers for kylin to scale spark worker
        """
        self._validate_stack(worker_num)
        stack_name = self.SCALE_STACK_NAME_TEMPLATE.format(worker_num)
        if self._stack_complete(stack_name):
            logger.warning(f"{stack_name} already created complete.")
            return

        # Note: the stack name must be pre-step's
        params: dict = self._merge_params(
            stack_name=self.config[Config.MASTER_STACK.value],
            param_name=Config.EC2_SCALE_SLAVE_PARAMS.value,
            config=self.config,
        )

        params.update({'WorkerNum': str(worker_num)})

        resp = self.create_stack(
            stack_name=stack_name,
            file_path=os.path.join(self.yaml_path, File.SLAVE_SCALE_YAML.value),
            params=params
        )

        return resp

    def scale_down_worker(self, worker_num: int) -> Optional[Dict]:
        self._validate_stack(worker_num)
        stack_name = self.SCALE_STACK_NAME_TEMPLATE.format(worker_num)

        if self._stack_delete_complete(stack_name):
            logger.warning(f"{stack_name} already deleted.")
            return

        resource_type = 'SlaveEc2InstanceId'
        # NOTE: name_or_id must be instance id!
        instance_id = self.get_specify_resource_from_output(stack_name, resource_type)

        # spark decommission feature start to be supported in spark 3.1.x.
        # refer: https://issues.apache.org/jira/browse/SPARK-20624.
        backup_command = 'source ~/.bash_profile && ${SPARK_HOME}/sbin/decommission-worker.sh'
        self.exec_script_instance_and_return(name_or_id=instance_id, script=backup_command)
        # FIXME: hard code for sleep spark worker to execute remaining jobs
        # sleep 5 min to ensure all jobs in decommissioned workers are done
        time.sleep(60 * 3)

        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.delete_stack(stack_name)
        return resp

    def _validate_stack(self, worker_num: int) -> None:
        stack_name = self.SCALE_STACK_NAME_TEMPLATE.format(worker_num)
        if stack_name not in self.stack_map.keys():
            msg = f'{stack_name} not in scaled list, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _stack_status(self, stack_name: str, required_status: str = 'CREATE_COMPLETE') -> bool:
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

    def _stack_exists(self, stack_name: str) -> bool:
        try:
            self.exists_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 2
                }
            )
        except WaiterError:
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
        relate_map = self.stack_map[stack_name]
        for k, v in params.items():
            # if params hasn't default value, use the pre-step output value to fill the param
            # special skip for scaling nodes
            if v or k == 'WorkerNum':
                continue
            params[k] = output[relate_map[k]]
        return params


class AWS:

    def __init__(self, config) -> None:
        self.cloud_instance = AWSInstance(config)
        self.config = config

    def is_cluster_ready(self) -> bool:
        return self.cloud_instance.is_ec2_stack_ready()

    def aws_ec2_cluster(self) -> Optional[Dict]:
        if not self.cloud_instance.is_ec2_stack_ready():
            self.cloud_instance.create_vpc_stack()
            self.cloud_instance.create_distribution_stack()
            self.cloud_instance.create_master_stack()
            self.cloud_instance.create_slave_stack()
        # now start monitor of prometheus because of every node_exporter ips are known
        self.cloud_instance.start_prometheus(self.config[Config.DISTRIBUTION_STACK.value])
        # return the master stack resources
        resources = self.cloud_instance.get_stack_output(self.config[Config.MASTER_STACK.value])
        return resources

    def terminate_ec2_cluster(self) -> Optional[Dict]:
        if self.cloud_instance.is_ec2_stack_terminated():
            logger.warning('ec2 stack already deleted.')
            return
        self.cloud_instance.terminate_slave_stack()
        self.cloud_instance.terminate_master_stack()
        self.cloud_instance.terminate_distribution_stack()
        if self.config['ALWAYS_DESTROY_ALL'] is True:
            self.cloud_instance.terminate_vpc_stack()
        # don't need to terminate vpc stack, because it's free resource on your aws if don't use it.
        # after terminated all node check again.
        assert self.cloud_instance.is_ec2_stack_terminated() is True

    def is_aws_cloud_ready(self) -> bool:
        if not self.is_cluster_ready():
            msg = f'Current Cluster is not ready, please deploy cluster first.'
            logger.warning(msg)
            raise Exception(msg)
        return True

    def aws_cloud(self) -> str:
        resource = self.aws_ec2_cluster()
        # only get the master dns
        # FIXME: fix hard code and get method
        if self.config[Config.EC2_MASTER_PARAMS.value]['AssociatedPublicIp'] == 'true':
            return resource.get('MasterEc2InstancePublicIp')
        return resource.get('MasterEc2InstancePrivateIp')

    def destroy_aws_cloud(self):
        self.terminate_ec2_cluster()

    def alive_workers(self):
        self.cloud_instance.alive_workers()

    @staticmethod
    def scale_up_down(config: Dict, worker_nums: List, scale_type: str) -> None:
        AWS.validate_scale(scale_type)
        cloud_instance = AWSInstance(config)
        exec_pool = ThreadPoolExecutor(max_workers=10)
        with exec_pool as pool:
            if scale_type == 'up':
                pool.map(cloud_instance.scale_up_worker, worker_nums)
            elif scale_type == 'down':
                pool.map(cloud_instance.scale_down_worker, worker_nums)

    def after_scale(self, workers_nums: List, scale_type: str) -> None:
        if scale_type == 'up':
            logging.info(f"Checking exists prometheus config after scale up.")
            expected_workers = self.cloud_instance.check_prometheus_config_after_scale_up(
                self.config[Config.DISTRIBUTION_STACK.value], workers_nums
            )
            logger.info("Refresh prometheus config after scale up.")
            if expected_workers:
                self.cloud_instance.refresh_prometheus_config_after_scale_up(
                    self.config[Config.DISTRIBUTION_STACK.value], expected_workers
                )
        else:
            logging.info(f"Checking exists prometheus config after scale down.")
            exists_workers = self.cloud_instance.check_prometheus_config_after_scale_down(
                self.config[Config.DISTRIBUTION_STACK.value], workers_nums
            )
            logger.info("Refresh prometheus config after scale down.")
            if exists_workers:
                self.cloud_instance.refresh_prometheus_config_after_scale_down(
                    self.config[Config.DISTRIBUTION_STACK.value], exists_workers
                )
        self.cloud_instance.restart_prometheus_server(self.config[Config.DISTRIBUTION_STACK.value])

    @staticmethod
    def validate_scale(scale_type: str) -> None:
        if scale_type is None or scale_type not in ['up', 'down']:
            msg = f'Not supported scale type: {scale_type}'
            logger.error(msg)
            raise Exception(msg)
