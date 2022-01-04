import logging
import time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError, WaiterError, ParamValidationError

from constant.client import Client
from constant.commands import Commands
from constant.config import Config
from constant.yaml_files import File
from constant.yaml_params import Params
from utils import read_template, full_path_of_yaml, generate_nodes

logger = logging.getLogger(__name__)


class AWSInstance:

    def __init__(self, config):
        # DEPLOY_PLATFORM
        self.config = config
        # global params
        self.vpc_id = None
        self.instance_profile = None
        self.security_group = None
        self.subnet_group = None
        self.subnet_id = None
        self.db_host = None

    @property
    def is_associated_public_ip(self) -> bool:
        return self.config[Params.ASSOSICATED_PUBLIC_IP.value] == 'true'

    @property
    def scaled_spark_workers(self) -> Optional[Tuple]:
        if self.config[Config.SPARK_WORKER_SCALE_NODES.value]:
            return literal_eval(self.config[Config.SPARK_WORKER_SCALE_NODES.value])
        return ()

    @property
    def scaled_spark_workers_stacks(self) -> List:
        return [Params.SPARK_WORKER_SCALE_STACK_NAME.value.format(num=i)
                for i in generate_nodes(self.scaled_spark_workers)]

    @property
    def scaled_kylin_nodes(self) -> Optional[Tuple]:
        if self.config[Config.KYLIN_SCALE_NODES.value]:
            return literal_eval(self.config[Config.KYLIN_SCALE_NODES.value])
        return ()

    @property
    def scaled_kylin_stacks(self) -> List:
        return [Params.KYLIN_SCALE_STACK_NAME.value.format(num=i)
                for i in generate_nodes(self.scaled_kylin_nodes)]

    @property
    def region(self) -> str:
        return self.config[Config.AWS_REGION.value]

    @property
    def cf_client(self):
        return boto3.client(Client.CLOUD_FORMATION.value, region_name=self.region)

    @property
    def rds_client(self):
        return boto3.client(Client.RDS.value, region_name=self.region)

    @property
    def ec2_client(self):
        return boto3.client(Client.EC2.value, region_name=self.region)

    @property
    def ssm_client(self):
        return boto3.client('ssm', region_name=self.region)

    @property
    def create_complete_waiter(self):
        return self.cf_client.get_waiter('stack_create_complete')

    @property
    def delete_complete_waiter(self):
        return self.cf_client.get_waiter('stack_delete_complete')

    @property
    def exists_waiter(self):
        return self.cf_client.get_waiter('stack_exists')

    @property
    def db_port(self) -> str:
        return self.config[Config.DB_PORT.value]

    @property
    def db_identifier(self) -> str:
        return self.config[Config.DB_IDENTIFIER.value]

    @property
    def vpc_stack_name(self) -> str:
        return self.config[Config.VPC_STACK.value]

    @property
    def rds_stack_name(self) -> str:
        return self.config[Config.RDS_STACK.value]

    @property
    def static_service_stack_name(self) -> str:
        return self.config[Config.STATIC_SERVICE_STACK.value]

    @property
    def zk_stack_name(self) -> str:
        return self.config[Config.ZOOKEEPERS_STACK.value]

    @property
    def kylin_stack_name(self) -> str:
        return self.config[Config.KYLIN_STACK.value]

    @property
    def spark_master_stack_name(self) -> str:
        return self.config[Config.SPARK_MASTER_STACK.value]

    @property
    def spark_slave_stack_name(self) -> str:
        return self.config[Config.SPARK_WORKER_STACK.value]

    @property
    def path_of_vpc_stack(self) -> str:
        return full_path_of_yaml(File.VPC_YAML.value)

    @property
    def path_of_rds_stack(self) -> str:
        return full_path_of_yaml(File.RDS_YAML.value)

    @property
    def path_of_static_service_stack(self) -> str:
        return full_path_of_yaml(File.STATIC_SERVICE_YAML.value)

    @property
    def path_of_zk_stack(self) -> str:
        return full_path_of_yaml(File.ZOOKEEPERS_SERVICE_YAML.value)

    @property
    def path_of_kylin_stack(self) -> str:
        return full_path_of_yaml(File.KYLIN4_YAML.value)

    @property
    def path_of_kylin_scale_stack(self) -> str:
        return full_path_of_yaml(File.KYLIN_SCALE_YAML.value)

    @property
    def path_of_spark_master_stack(self) -> str:
        return full_path_of_yaml(File.SPARK_MASTER_YAML.value)

    @property
    def path_of_spark_slave_stack(self) -> str:
        return full_path_of_yaml(File.SPARK_WORKER_YAML.value)

    @property
    def path_of_spark_slave_scaled_stack(self) -> str:
        return full_path_of_yaml(File.SPARK_WORKER_SCALE_YAML.value)

    @property
    def bucket_full_path(self) -> str:
        full_path: str = self.config[Params.S3_FULL_BUCKET_PATH.value]
        assert full_path.startswith('s3:/'), f'bucket full path must start with s3:/'
        if full_path.endswith('/'):
            full_path = full_path.rstrip('/')
        return full_path

    @property
    def bucket_path(self) -> str:
        # remove thre prefix of 's3:/'
        path = self.bucket_full_path[len('s3:/'):]
        return path

    def get_vpc_id(self) -> str:
        if not self.vpc_id:
            self.vpc_id = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.VPC_ID.value
            )
            assert self.vpc_id, f'vpc id must not be empty or None.'
        return self.vpc_id

    def get_instance_profile(self) -> str:
        if not self.instance_profile:
            self.instance_profile = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.INSTANCE_PROFILE.value
            )
        return self.instance_profile

    def get_subnet_id(self) -> str:
        if not self.subnet_id:
            self.subnet_id = self.get_specify_resource_from_output(self.vpc_stack_name, Params.SUBNET_ID.value)
            assert self.subnet_id, 'subnet id must not be empty or None.'
        return self.subnet_id

    def get_subnet_group(self) -> str:
        if not self.subnet_group:
            self.subnet_group = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.SUBNET_GROUP_NAME.value
            )
            assert self.subnet_group, 'subnet group must not be empty or None.'
        return self.subnet_group

    def get_db_host(self) -> str:
        if not self.db_host:
            self.db_host = self.get_rds_describe()['Endpoint']['Address']
            assert self.db_host, 'db_host must not be empty or None.'
        return self.db_host

    def get_security_group_id(self) -> str:
        if not self.security_group:
            self.security_group = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.SECURITY_GROUP.value
            )
            assert self.security_group, f'security_group_id must not be empty or None.'
        return self.security_group

    def get_instance_id(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ""
        return self.get_specify_resource_from_output(stack_name, Params.INSTANCE_ID.value)

    def get_instance_ids_of_slaves_stack(self) -> List:
        return [
            self.get_specify_resource_from_output(
                self.spark_slave_stack_name, f'IdOfInstanceOfSlave0{i}'
            )
            for i in range(1, 4)
        ]

    # ============ VPC Services Start ============
    def create_vpc_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.vpc_stack_name):
            return
        params: Dict = self.config[Config.EC2_VPC_PARAMS.value]
        resp = self.create_stack(
            stack_name=self.vpc_stack_name,
            file_path=self.path_of_vpc_stack,
            params=params,
            is_capability=True
        )
        return resp

    def terminate_vpc_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.vpc_stack_name)
        # Make sure that vpc stack deleted successfully.
        assert self.is_stack_deleted_complete(self.vpc_stack_name),\
            f'{self.vpc_stack_name} deleted failed, please check.'
        return resp

    # ============ VPC Services End ============

    # ============ RDS Services Start ============
    def get_rds_describe(self) -> Optional[Dict]:
        if not self.db_identifier:
            raise Exception(f'{Config.DB_IDENTIFIER.value} must not be empty or None.')
        if not self.is_rds_exists():
            return
        describe_rds: Dict = self.rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
        db_instances: List = describe_rds['DBInstances']
        assert len(db_instances) == 1, f'the identifier of RDS must exist only one.'
        return db_instances[0]

    def is_rds_exists(self) -> bool:
        try:
            self.rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
        except self.rds_client.exceptions.DBInstanceNotFoundFault as ex:
            logger.warning(ex.response['Error']['Message'])
            return False
        return True

    def create_rds_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.rds_stack_name):
            return
        if self.is_rds_exists():
            logger.warning(f'db {self.db_identifier} already exists.')
            return
        params: Dict = self.config[Config.EC2_RDS_PARAMS.value]
        # update needed params
        params[Params.SUBNET_GROUP_NAME.value] = self.get_subnet_group()
        params[Params.SECURITY_GROUP.value] = self.get_security_group_id()
        resp = self.create_stack(
            stack_name=self.rds_stack_name,
            file_path=self.path_of_rds_stack,
            params=params,
        )
        # Make sure that rds create successfully.
        assert self.is_stack_complete(self.rds_stack_name), f'Rds {self.db_identifier} create failed, please check.'
        return resp

    def terminate_rds_stack(self) -> Optional[Dict]:
        # Note: terminated rds will not delete it, user can delete db manually.
        resp = self.terminate_stack_by_name(self.rds_stack_name)
        return resp

    # ============ RDS Services End ============

    # ============ Static Services Start ============
    def create_static_service_stack(self) -> Optional[Dict]:
        if not self.is_rds_ready():
            msg = f'rds {self.db_identifier} is not ready, please check.'
            logger.warning(msg)
            raise Exception(msg)

        if self.is_stack_complete(self.static_service_stack_name):
            return
        params: Dict = self.config[Config.EC2_STATIC_SERVICES_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)
        params[Params.DB_HOST.value] = self.get_db_host()

        resp = self.create_stack(
            stack_name=self.static_service_stack_name,
            file_path=self.path_of_static_service_stack,
            params=params
        )
        return resp

    def terminate_static_service_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.static_service_stack_name)
        return resp

    def get_static_services_instance_id(self) -> Optional[str]:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        return self.get_specify_resource_from_output(self.static_service_stack_name, Params.INSTANCE_ID.value)

    def get_static_services_private_ip(self) -> Optional[str]:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        return self.get_specify_resource_from_output(
            self.static_service_stack_name, Params.STATIC_SERVICES_PRIVATE_IP.value)

    def get_static_services_public_ip(self) -> Optional[str]:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        if not self.is_associated_public_ip:
            logger.warning('Current static services was associated to a public ip.')
            return
        return self.get_specify_resource_from_output(
            self.static_service_stack_name, Params.STATIC_SERVICES_PUBLIC_IP.value)

    def get_static_services_basic_msg(self) -> Optional[str]:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        res = Params.STATIC_SERVICES_NAME.value + '\t' \
          + self.get_static_services_instance_id() + '\t' \
          + self.get_static_services_private_ip() + '\t' \
          + self.get_static_services_public_ip()
        return res

    # ============ Static Services End ============

    # ============ Zookeeper Services Start ============
    def get_instance_ids_of_zks_stack(self) -> List:
        return [
            self.get_specify_resource_from_output(
                self.zk_stack_name, Params.ZOOKEEPER_INSTANCE_ID.value.format(num=i)
            )
            for i in range(1, 4)
        ]

    def get_instance_ips_of_zks_stack(self) -> List:
        return [self.get_specify_resource_from_output(
            self.zk_stack_name, Params.ZOOKEEPER_IP.value.format(num=i))
            for i in range(1, 4)
        ]

    def create_zk_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.zk_stack_name):
            return
        params: Dict = self.config[Config.EC2_ZOOKEEPERS_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)

        resp = self.create_stack(
            stack_name=self.zk_stack_name,
            file_path=self.path_of_zk_stack,
            params=params
        )
        assert self.is_stack_complete(self.zk_stack_name), f'{self.zk_stack_name} create failed, please check.'
        return resp

    def terminate_zk_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.zk_stack_name)
        return resp

    def after_create_zk_cluster(self) -> None:
        zk_ips = self.get_instance_ips_of_zks_stack()
        zk_ids = self.get_instance_ids_of_zks_stack()
        # Check related instances status before refresh zks and start them
        for zk_id in zk_ids:
            assert self.is_ec2_instance_running(zk_id), f'Instance {zk_id} is not running, please start it first.'

        # refresh zk cluster cfg, because the zk cfg was not included
        # FIXME: it's hard code to make sure that zks were already initialized.
        time.sleep(10)

        self.refresh_zks_cfg(zk_ips=zk_ips, zk_ids=zk_ids)
        self.start_zks(zk_ids=zk_ids, zk_ips=zk_ips)

    def refresh_zks_cfg(self, zk_ips: List, zk_ids: List) -> None:
        assert len(zk_ips) == 3, f'Initialized zookeeper ips num is 3, not {len(zk_ips)}.'
        assert len(zk_ids) == 3, f'Initialized zookeeper ids num is 3, not {len(zk_ids)}.'
        configured_zks = self.configured_zks_cfg(zk_ids=zk_ids, zk_ips=zk_ips)
        if self.is_configured_zks(configured_zks):
            logger.warning('Zookeepers already configured, skip configure.')
            return
        need_to_configure_zks = [zk_id for zk_id in zk_ids if zk_id not in configured_zks]
        refresh_command = Commands.ZKS_CFG_COMMAND.value.format(host1=zk_ips[0], host2=zk_ips[1], host3=zk_ips[2])
        for zk_id in need_to_configure_zks:
            self.exec_script_instance_and_return(name_or_id=zk_id, script=refresh_command)

    @staticmethod
    def is_configured_zks(configured_zks: List) -> bool:
        return len(configured_zks) == 3

    def configured_zks_cfg(self, zk_ids: List, zk_ips: List) -> List:
        check_command = Commands.ZKS_CHECK_CONFIGURED_COMMAND.value
        configured_instances = []
        for zk_id, zk_ip in zip(zk_ids, zk_ips):
            resp = self.exec_script_instance_and_return(zk_id, check_command.format(host=zk_ip))
            if resp['StandardOutputContent'] == '0\n':
                logger.warning(f'Instance: {zk_id} which ip is {zk_ip} already configured zoo.cfg.')
                configured_instances.append(zk_id)
        return configured_instances

    def start_zks(self, zk_ids: List, zk_ips: List) -> None:
        assert len(zk_ids) == 3, f'Expected to start 3 zookeepers, not {len(zk_ids)}.'
        started_zks = self.started_zks(zk_ids=zk_ids, zk_ips=zk_ips)
        if self.is_started_zks(started_zks=started_zks):
            logger.warning('Zookeepers already started.')
            return

        need_to_start_zks = [zk_id for zk_id in zk_ids if zk_id not in started_zks]
        start_zk_command = Commands.ZKS_START_COMMAND.value
        for zk_id in need_to_start_zks:
            self.exec_script_instance_and_return(name_or_id=zk_id, script=start_zk_command)
        logger.info('Start zookeepers successfully.')

    @staticmethod
    def is_started_zks(started_zks: List) -> bool:
        return len(started_zks) == 3

    def started_zks(self, zk_ids: List, zk_ips: List) -> List:
        check_command = Commands.ZKS_CHECK_STARTED_COMMAND.value
        started_instances = []
        for zk_id, zk_ip in zip(zk_ids, zk_ips):
            resp = self.exec_script_instance_and_return(zk_id, check_command)
            if resp['StandardOutputContent'] == '0\n':
                logger.warning(f'Instance: {zk_id} which ip is {zk_ip} already started.')
                started_instances.append(zk_id)
        return started_instances

    def get_zookeepers_host(self) -> str:
        zk_ips = self.get_instance_ips_of_zks_stack()
        res = ','.join([zk_ip + ':2181' for zk_ip in zk_ips])
        return res

    def get_zks_basic_msg(self) -> List:
        if not self.is_stack_complete(self.zk_stack_name):
            return []

        res = [
            Params.ZOOKEEPER_NAME.value.format(num=i) + '\t'
            + self.get_specify_resource_from_output(
                self.zk_stack_name, Params.ZOOKEEPER_INSTANCE_ID.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(self.zk_stack_name, Params.ZOOKEEPER_IP.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(self.zk_stack_name, Params.ZOOKEEPER_PUB_IP.value.format(num=i))
            for i in range(1, 4)
        ]
        return res

    # ============ Zookeeper Services End ============

    # ============ kylin Services Start ============
    def create_kylin_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.kylin_stack_name):
            return

        params: Dict = self.config[Config.EC2_KYLIN4_PARAMS.value]
        params = self.update_basic_params(params)
        # update extra params
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host()
        params[Params.ZOOKEEPER_HOSTS.value] = self.get_zookeepers_host()
        params[Params.DB_HOST.value] = self.get_db_host()

        resp = self.create_stack(
            stack_name=self.kylin_stack_name,
            file_path=self.path_of_kylin_stack,
            params=params
        )
        return resp

    def terminate_kylin_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.kylin_stack_name)
        return resp

    def get_kylin_private_ip(self) -> Optional[str]:
        return self._get_kylin_private_ip(self.kylin_stack_name)

    def get_scaled_kylin_private_ip(self, stack_name: str) -> Optional[str]:
        return self._get_kylin_private_ip(stack_name)

    def _get_kylin_private_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ""
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PRIVATE_IP.value)

    def get_kylin_public_ip(self) -> Optional[str]:
        if not self.is_stack_complete(self.kylin_stack_name):
            return
        if not self.is_associated_public_ip:
            logger.warning('Current kylin was associated to a public ip.')
            return
        return self.get_specify_resource_from_output(self.kylin_stack_name, Params.KYLIN4_PUBLIC_IP.value)

    def get_kylin_instance_id(self) -> Optional[str]:
        if not self.is_stack_complete(self.kylin_stack_name):
            return
        return self.get_specify_resource_from_output(self.kylin_stack_name, Params.INSTANCE_ID.value)

    def get_kylin_basic_msg(self) -> Optional[str]:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        res = Params.KYLIN_NAME.value + '\t' \
              + self.get_kylin_instance_id() + '\t' \
              + self.get_kylin_private_ip() + '\t' \
              + self.get_kylin_public_ip()
        return res

    def get_scaled_kylin_private_ip(self, stack_name: str) -> Optional[str]:
        if not self.is_stack_complete(stack_name):
            return
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PRIVATE_IP.value)

    def get_scaled_kylin_public_ip(self, stack_name: str) -> Optional[str]:
        if not self.is_stack_complete(stack_name):
            return ""
        if not self.is_associated_public_ip:
            logger.warning('Current scaled kylin node was associated to a public ip.')
            return
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PUBLIC_IP.value)

    def get_scaled_kylin_basic_msg(self) -> List:
        msgs = []
        for stack in self.scaled_kylin_stacks:
            if not self.is_stack_complete(stack):
                continue

            instance_id = self.get_instance_id(stack)
            private_ip = self.get_scaled_kylin_private_ip(stack)
            public_ip = self.get_scaled_kylin_public_ip(stack)
            msg = stack + '\t' + instance_id + '\t' + private_ip + '\t' + public_ip + '\t'
            msgs.append(msg)
        return msgs

    def scale_up_kylin(self, kylin_num: int) -> Optional[Dict]:
        """
        add kylin node
        """
        stack_name = Params.KYLIN_SCALE_STACK_NAME.value.format(num=kylin_num)
        self._validate_kylin_scale(stack_name)

        if self.is_stack_complete(stack_name):
            return

        params: Dict = self.config[Config.EC2_KYLIN4_SCALE_PARAMS.value]
        # update extra params
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host()
        params[Params.ZOOKEEPER_HOSTS.value] = self.get_zookeepers_host()
        params[Params.DB_HOST.value] = self.get_db_host()

        resp = self.create_stack(
            stack_name=stack_name,
            file_path=self.path_of_kylin_scale_stack,
            params=params
        )

        return resp

    def scale_down_kylin(self, kylin_num: int) -> Optional[Dict]:
        stack_name = Params.KYLIN_SCALE_STACK_NAME.value.format(num=kylin_num)
        self._validate_kylin_scale(stack_name)
        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.terminate_stack_by_name(stack_name)
        return resp

    # ============ kylin Services End ============

    # ============ Spark Master Services Start ============
    def get_spark_master_host(self) -> Optional[str]:
        if not self.is_stack_complete(self.spark_master_stack_name):
            return

        return self.get_specify_resource_from_output(self.spark_master_stack_name, Params.SPARK_MASTER_HOST.value)

    def get_spark_master_public_ip(self) -> Optional[str]:
        if not self.is_stack_complete(self.spark_master_stack_name):
            return

        return self.get_specify_resource_from_output(self.spark_master_stack_name, Params.SPARK_PUB_IP.value)

    def create_spark_master_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.spark_master_stack_name):
            return

        params: Dict = self.config[Config.EC2_SPARK_MASTER_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)
        params[Params.DB_HOST.value] = self.get_db_host()
        resp = self.create_stack(
            stack_name=self.spark_master_stack_name,
            file_path=self.path_of_spark_master_stack,
            params=params
        )
        return resp

    def terminate_spark_master_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.spark_master_stack_name)
        return resp

    def start_spark_master(self) -> None:
        spark_master_id = self.get_instance_id(self.spark_master_stack_name)
        if not self.is_ec2_instance_running(spark_master_id):
            msg = f'Instance of spark master{spark_master_id} was not running, please start it first.'
            logger.error(msg)
            raise Exception(msg)

        start_command = Commands.START_SPARK_MASTER_COMMAND.value
        self.exec_script_instance_and_return(name_or_id=spark_master_id, script=start_command)

    def get_spark_master_instance_id(self) -> Optional[str]:
        if not self.is_stack_complete(self.spark_master_stack_name):
            return

        return self.get_specify_resource_from_output(self.spark_master_stack_name, Params.INSTANCE_ID.value)

    def get_spark_master_msg(self) -> Optional[str]:
        if not self.is_stack_complete(self.spark_master_stack_name):
            return
        res = Params.SPARK_MASTER_NAME.value + '\t' \
              + self.get_spark_master_instance_id() + '\t' \
              + self.get_spark_master_host() + '\t' \
              + self.get_spark_master_public_ip()
        return res
    # ============ Spark Master Services End ============

    # ============ Spark Slave Services Start ============
    def create_spark_slave_stack(self) -> Optional:
        if not self.is_stack_complete(self.spark_master_stack_name):
            msg = f'Spark master {self.spark_master_stack_name} must be created before create spark slaves.'
            logger.error(msg)
            raise Exception(msg)

        if self.is_stack_complete(self.spark_slave_stack_name):
            return

        params: Dict = self.config[Config.EC2_SPARK_WORKER_PARAMS.value]
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host()

        resp = self.create_stack(
            stack_name=self.spark_slave_stack_name,
            file_path=self.path_of_spark_slave_stack,
            params=params
        )
        return resp

    def terminate_spark_slave_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.spark_slave_stack_name)
        return resp

    def get_instance_ids_of_spark_slave_stack(self) -> Optional[List]:
        if not self.is_stack_complete(self.spark_slave_stack_name):
            return
        return [
            self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_ID.value.format(num=i))
            for i in range(1, 4)
        ]

    def get_instance_ips_of_spark_slaves_stack(self) -> Optional[List]:
        if not self.is_stack_complete(self.spark_slave_stack_name):
            return
        return [
            self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_PRIVATE_IP.value.format(num=i))
            for i in range(1, 4)
        ]

    def get_scaled_spark_worker_private_ip(self, stack_name: str) -> Optional[str]:
        if not self.is_stack_complete(stack_name):
            return
        return self.get_specify_resource_from_output(stack_name, Params.SPARK_SCALED_WORKER_PRIVATE_IP.value)

    def get_scaled_spark_worker_public_ip(self, stack_name: str) -> Optional[str]:
        if not self.is_stack_complete(stack_name):
            return
        if not self.is_associated_public_ip:
            logger.warning('Current spark worker was associated to a public ip.')
            return
        return self.get_specify_resource_from_output(stack_name, Params.SPARK_SCALED_WORKER_PUBLIC_IP.value)

    def get_spark_slaves_basic_msg(self) -> List:
        if not self.is_stack_complete(self.spark_slave_stack_name):
            return []

        res = [
            Params.SPARK_WORKER_NAME.value.format(num=i) + '\t'
            + self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_ID.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_PRIVATE_IP.value.format(num=i)) + '\t'
            + (self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_PUBLIC_IP.value.format(num=i))
               if self.is_associated_public_ip else '')
            for i in range(1, 4)
        ]
        return res

    def get_scaled_spark_workers_basic_msg(self) -> List:
        msgs = []
        for stack in self.scaled_spark_workers_stacks:
            if not self.is_stack_complete(stack):
                continue

            instance_id = self.get_instance_id(stack)
            private_ip = self.get_scaled_spark_worker_private_ip(stack)
            public_ip = self.get_scaled_spark_worker_public_ip(stack)
            msg = stack + '\t' + instance_id + '\t' + private_ip + '\t' + public_ip
            msgs.append(msg)
        return msgs

    def scale_up_worker(self, worker_num: int) -> Optional[Dict]:
        """
        add spark workers for kylin
        """
        stack_name = Params.SPARK_WORKER_SCALE_STACK_NAME.value.format(num=worker_num)
        self._validate_spark_worker_scale(stack_name)

        if self.is_stack_complete(stack_name):
            return

        params: Dict = self.config[Config.EC2_SPARK_SCALE_SLAVE_PARAMS.value]
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host()
        params[Params.SPARK_WORKER_NUM.value] = str(worker_num)

        resp = self.create_stack(
            stack_name=stack_name,
            file_path=self.path_of_spark_slave_scaled_stack,
            params=params
        )

        return resp

    def scale_down_worker(self, worker_num: int) -> Optional[Dict]:
        stack_name = Params.SPARK_WORKER_SCALE_STACK_NAME.value.format(num=worker_num)
        self._validate_spark_worker_scale(stack_name)
        if self.is_stack_deleted_complete(stack_name):
            return

        instance_id = self.get_instance_id(stack_name)
        # spark decommission feature start to be supported in spark 3.1.x.
        # refer: https://issues.apache.org/jira/browse/SPARK-20624.
        try:
            self.exec_script_instance_and_return(
                name_or_id=instance_id, script=Commands.SPARK_DECOMMISION_WORKER_COMMAND.value)
            # FIXME: hard code for sleep spark worker to execute remaining jobs
            # sleep 5 min to ensure all jobs in decommissioned workers are done
            time.sleep(60 * 3)
        except AssertionError as ex:
            logger.error(ex)

        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.delete_stack(stack_name)
        return resp
    # ============ Spark Slave Services End ============

    # ============ Prometheus Services Start ============
    def start_prometheus(self) -> None:
        self.refresh_prometheus_param_map()
        self.start_prometheus_server()

    def start_prometheus_server(self) -> None:
        start_command = Commands.START_PROMETHEUS_COMMAND.value
        instance_id = self.get_static_services_instance_id()
        self.exec_script_instance_and_return(name_or_id=instance_id, script=start_command)

    def stop_prometheus_server(self) -> None:
        stop_command = Commands.STOP_PROMETHEUS_COMMAND.value
        instance_id = self.get_static_services_instance_id()
        self.exec_script_instance_and_return(name_or_id=instance_id, script=stop_command)

    def restart_prometheus_server(self) -> None:
        self.stop_prometheus_server()
        self.start_prometheus_server()

    def refresh_prometheus_param_map(self) -> None:
        static_services_id = self.get_static_services_instance_id()

        refresh_config_commands = self.refresh_prometheus_commands()
        for command in refresh_config_commands:
            self.exec_script_instance_and_return(name_or_id=static_services_id, script=command)

        # Special support spark metrics into prometheus
        spark_config_commands = self.refresh_spark_metrics_commands()
        for command in spark_config_commands:
            self.exec_script_instance_and_return(name_or_id=static_services_id, script=command)

    def refresh_prometheus_commands(self) -> List:
        params = self.prometheus_param_map()
        # NOTE: the spaces in template is for prometheus config's indent
        commands = [Commands.PROMETHEUS_CFG_COMMAND.value.format(node=node, host=host) for node, host in params.items()]
        return commands

    def after_scale_prometheus_params_map_of_kylin(self) -> Dict:
        kylin_ips, _ = self.get_scaled_node_private_ips()

        kylin_node_keys = [Params.KYLIN_SCALE_NODE_NAME.value.format(num=i)
                           for i in generate_nodes(self.scaled_kylin_nodes)]
        kylin_params_map = dict(zip(kylin_node_keys, kylin_ips))
        return kylin_params_map

    def after_scale_prometheus_params_map_of_spark_worker(self) -> Dict:
        _, spark_workers_ips = self.get_scaled_node_private_ips()

        spark_workers_keys = [Params.SPARK_SCALE_WORKER_NAME.value.format(num=i)
                              for i in generate_nodes(self.scaled_spark_workers)]
        spark_workers_params_map = dict(zip(spark_workers_keys, spark_workers_ips))

        return spark_workers_params_map

    def is_prometheus_configured(self, host: str) -> bool:
        static_services_instance_id = self.get_static_services_instance_id()
        check_command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=host)
        output = self.exec_script_instance_and_return(name_or_id=static_services_instance_id, script=check_command)
        return output['StandardOutputContent'] == '0\n'

    def get_prometheus_configured_hosts(self, hosts: List) -> List:
        static_services_instance_id = self.get_static_services_instance_id()
        configured_hosts = []
        for host in hosts:
            check_command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=host)
            output = self.exec_script_instance_and_return(name_or_id=static_services_instance_id, script=check_command)
            if output['StandardOutputContent'] == '0\n':
                configured_hosts.append(host)
        return configured_hosts

    def check_prometheus_config_after_scale(self, node_type: str) -> Dict:
        if node_type == 'kylin':
            workers_param_map = self.after_scale_prometheus_params_map_of_kylin()
        else:
            workers_param_map = self.after_scale_prometheus_params_map_of_spark_worker()

        instance_id = self.get_instance_id(self.static_service_stack_name)
        not_exists_nodes: Dict = {}
        for k, v in workers_param_map.items():
            command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=k)
            output = self.exec_script_instance_and_return(name_or_id=instance_id, script=command)
            # output['StandardOutputContent'] = '1\n' means the node not exists in the prometheus config
            if output['StandardOutputContent'] == '1\n':
                not_exists_nodes.update({k: v})
        return not_exists_nodes

    def check_prometheus_config_after_scale_down(self, node_type: str) -> Dict:
        if node_type == 'kylin':
            workers_param_map = self.after_scale_prometheus_params_map_of_kylin()
        else:
            workers_param_map = self.after_scale_prometheus_params_map_of_spark_worker()

        instance_id = self.get_instance_id(self.static_service_stack_name)
        exists_nodes: Dict = {}
        for k, v in workers_param_map.items():
            command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=k)
            output = self.exec_script_instance_and_return(name_or_id=instance_id, script=command)
            # output['StandardOutputContent'] = '0\n' means the node exists in the prometheus config
            if output['StandardOutputContent'] == '0\n':
                exists_nodes.update({k: v})
        return exists_nodes

    def refresh_prometheus_config_after_scale_up(self, expected_nodes: Dict) -> None:
        commands = self.refresh_prometheus_commands_after_scale(expected_nodes)
        instance_id = self.get_instance_id(self.static_service_stack_name)
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    def refresh_prometheus_config_after_scale_down(self, exists_nodes: Dict) -> None:
        instance_id = self.get_instance_id(self.static_service_stack_name)
        commands = [Commands.PROMETHEUS_DELETE_CFG_COMMAND.value.format(node=worker) for worker in exists_nodes.keys()]
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=instance_id, script=command)

    # ============ Prometheus Services End ============

    # ============ Utils Services Start ============
    def update_basic_params(self, params: Dict) -> Dict:
        params[Params.SUBNET_ID.value] = self.get_subnet_id()
        params[Params.SECURITY_GROUP.value] = self.get_security_group_id()
        params[Params.INSTANCE_PROFILE.value] = self.get_instance_profile()
        # update bucket path
        params[Params.BUCKET_FULL_PATH.value] = self.bucket_full_path
        params[Params.BUCKET_PATH.value] = self.bucket_path
        return params

    def terminate_stack_by_name(self, stack_name: str) -> Optional[Dict]:
        if self.is_stack_deleted_complete(stack_name):
            return

        resp = self.delete_stack(stack_name)
        return resp

    def create_stack(self, stack_name: str, file_path: str, params: Dict, is_capability: bool = False) -> Dict:
        try:
            if is_capability:
                resp = self.cf_client.create_stack(
                    StackName=stack_name,
                    TemplateBody=read_template(file_path),
                    Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
                    Capabilities=['CAPABILITY_IAM']
                )
            else:
                resp = self.cf_client.create_stack(
                    StackName=stack_name,
                    TemplateBody=read_template(file_path),
                    Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
                )
            return resp
        except ParamValidationError as ex:
            logger.error(ex)
        assert self.is_stack_complete(stack_name=stack_name), \
            f"Stack {stack_name} not create complete, please check."

    def delete_stack(self, stack_name: str) -> Dict:
        logger.info(f'Current terminating stack: {stack_name}.')
        resp = self.cf_client.delete_stack(StackName=stack_name)
        return resp

    def refresh_spark_metrics_commands(self) -> List:
        spark_master_host = self.get_spark_master_host()
        commands = [
            Commands.SPARK_DRIVER_METRIC_COMMAND.value.format(node='spark_driver', host=spark_master_host),
            Commands.SPARK_WORKER_METRIC_COMMAND.value.format(node='spark_worker', host=spark_master_host),
            Commands.SPARK_APPLICATIONS_METRIC_COMMAND.value.format(node='spark_applications', host=spark_master_host),
            Commands.SPARK_MASTER_METRIC_COMMAND.value.format(node='spark_master', host=spark_master_host),
            Commands.SPARK_EXECUTORS_METRIC_COMMAND.value.format(node='spark_executors', host=spark_master_host),
        ]
        return commands

    @staticmethod
    def refresh_prometheus_commands_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.PROMETHEUS_CFG_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    def prometheus_param_map(self) -> Dict:
        params_map: Dict = {}
        static_ip = self.get_static_services_private_ip()
        static_map = {Params.STATIC_SERVICES_NAME.value: static_ip}

        kylin_ip = self.get_kylin_private_ip()
        kylin_map = {Params.KYLIN_NAME.value: kylin_ip}

        spark_master_ip = self.get_spark_master_host()
        spark_master_map = {Params.SPARK_MASTER_NAME.value: spark_master_ip}

        zk_ips = self.get_instance_ips_of_zks_stack()
        zks_map = {
            Params.ZOOKEEPER_NAME.value.format(num=i): zk_ips[i - 1] for i in range(1, 4)
        }
        spark_slaves_ips = self.get_instance_ips_of_spark_slaves_stack()
        spark_slaves_map = {
            Params.SPARK_WORKER_NAME.value.format(num=i): spark_slaves_ips[i - 1] for i in range(1, 4)
        }
        params_map.update(static_map)
        params_map.update(kylin_map)
        params_map.update(spark_master_map)
        params_map.update(zks_map)
        params_map.update(spark_slaves_map)

        return params_map

    def zk_param_map(self) -> Dict:
        return dict(zip(self.get_instance_ids_of_zks_stack(), self.get_instance_ips_of_zks_stack()))

    def static_services_param_map(self) -> Dict:
        return dict(zip(self.get_static_services_instance_id(), self.get_static_services_private_ip()))

    def spark_master_param_map(self) -> Dict:
        return dict(zip(self.get_spark_master_instance_id(), self.get_spark_master_host()))

    def spark_slaves_param_map(self) -> Dict:
        return dict(zip(self.get_instance_ids_of_spark_slave_stack(), self.get_instance_ips_of_spark_slaves_stack()))

    def kylin_param_map(self) -> Dict:
        return dict(zip(self.get_kylin_instance_id(), self.get_kylin_private_ip()))

    def get_scaled_node_private_ips(self) -> [List, List]:
        scaled_kylin_stacks = self.scaled_kylin_stacks
        scaled_kylin_ips = []
        for stack in scaled_kylin_stacks:
            ip = self.get_scaled_kylin_private_ip(stack)
            if not ip:
                continue
            scaled_kylin_ips.append(ip)

        scaled_spark_worker_stacks = self.scaled_spark_workers_stacks
        scaled_workers_ips = []
        for stack in scaled_spark_worker_stacks:
            ip = self.get_scaled_spark_worker_private_ip(stack)
            if not ip:
                continue
            scaled_workers_ips.append(ip)
        return scaled_kylin_ips, scaled_workers_ips

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
        static_msg = self.get_static_services_basic_msg()
        kylin_msg = self.get_kylin_basic_msg()
        spark_master_msg = self.get_spark_master_msg()

        msgs = [m for m in [static_msg, kylin_msg, spark_master_msg] if m]

        spark_slaves_msg = self.get_spark_slaves_basic_msg()
        zks_msg = self.get_zks_basic_msg()

        scaled_kylins_msg = self.get_scaled_kylin_basic_msg()
        scaled_spark_workers_msg = self.get_scaled_spark_workers_basic_msg()

        msgs.extend(zks_msg)
        msgs.extend(spark_slaves_msg)
        msgs.extend(scaled_kylins_msg)
        msgs.extend(scaled_spark_workers_msg)
        header_msg = '\n=================== List Alive Nodes ===========================\n'
        result = header_msg + f"Stack Name\t\tInstance ID\t\tPrivate Ip\t\tPublic Ip\t\t\n"
        for msg in msgs:
            result += msg + '\n'
        result += header_msg
        logger.info(result)

    def is_ec2_stacks_ready(self) -> bool:
        if not (
                self.is_stack_complete(self.vpc_stack_name)
                and self.is_stack_complete(self.static_service_stack_name)
                and self.is_stack_complete(self.zk_stack_name)
                and self.is_stack_complete(self.spark_master_stack_name)
                and self.is_stack_complete(self.spark_slave_stack_name)
                and self.is_stack_complete(self.kylin_stack_name)
        ):
            return False
        return True

    def is_ec2_stacks_terminated(self) -> bool:
        deleted_cost_stacks: bool = (
                self.is_stack_deleted_complete(self.static_service_stack_name)
                and self.is_stack_deleted_complete(self.zk_stack_name)
                and self.is_stack_deleted_complete(self.spark_master_stack_name)
                and self.is_stack_deleted_complete(self.spark_slave_stack_name)
                and self.is_stack_deleted_complete(self.kylin_stack_name)
        )
        if not deleted_cost_stacks:
            return False
        if not self.config['ALWAYS_DESTROY_ALL'] \
                or self.is_stack_deleted_complete(self.vpc_stack_name):
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

    def stop_ec2_instances(self, instance_ids: List):
        self.ec2_client.stop_instances(
            InstanceIds=instance_ids,
            Force=True
        )

    def start_ec2_instance(self, instance_id: str) -> Dict:
        resp = self.ec2_client.start_instances(
            InstanceIds=[instance_id]
        )
        return resp

    def start_ec2_instances(self, instance_ids: List) -> Dict:
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

    def is_rds_ready(self) -> bool:
        describe_rds: Dict = self.get_rds_describe()
        if not describe_rds:
            return False
        rds_endpoints: Dict = describe_rds['Endpoint']
        # TODO: check rds with password and user is accessible.

        is_rds_available: bool = describe_rds['DBInstanceStatus'] == 'available'
        is_rds_matched_port = str(rds_endpoints['Port']) == self.db_port
        return is_rds_available and is_rds_matched_port

    def _validate_spark_worker_scale(self, stack_name: str) -> None:
        if stack_name not in self.scaled_spark_workers_stacks:
            msg = f'{stack_name} not in scaled list, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _validate_kylin_scale(self, stack_name: str) -> None:
        if stack_name not in self.scaled_kylin_stacks:
            msg = f'{stack_name} not in scaled list, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _stack_status(self, stack_name: str, required_status: str = 'CREATE_COMPLETE') -> bool:
        return self._stack_status_check(name_or_id=stack_name, status=required_status)

    def _stack_deleted(self, stack_name: str, required_status: str = 'DELETE_COMPLETE') -> bool:
        return self._stack_status_check(name_or_id=stack_name, status=required_status)

    def _stack_status_check(self, name_or_id: str, status: str) -> bool:
        try:
            resp: Dict = self.cf_client.describe_stacks(StackName=name_or_id)
        except ClientError:
            return False
        return resp['Stacks'][0]['StackStatus'] == status

    def is_stack_complete(self, stack_name: str) -> bool:
        if self._stack_complete(stack_name):
            logger.warning(f"{stack_name} already complete, skip create it again.")
            return True
        return False

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
            # logger.error(wx)
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

    def is_stack_deleted_complete(self, stack_name: str) -> bool:
        if self._stack_delete_complete(stack_name):
            logger.warning(f"{stack_name} already deleted, skip delete it.")
            return True
        return False

    def _stack_delete_complete(self, stack_name: str) -> bool:
        try:
            self.delete_complete_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 60,
                    'MaxAttempts': 120
                }
            )
        except WaiterError as wx:
            # logger.error(wx)
            return False
        return True
    # ============ Utils Services End ============


class AWS:

    def __init__(self, config) -> None:
        self.cloud_instance = AWSInstance(config)
        self.config = config

    @property
    def is_cluster_ready(self) -> bool:
        if self.is_instances_ready:
            return True
        msg = f'Current cluster is not ready, please deploy cluster first.'
        logger.warning(msg)
        return False

    @property
    def is_instances_ready(self) -> bool:
        return self.cloud_instance.is_ec2_stacks_ready()

    @property
    def is_cluster_terminated(self) -> bool:
        if self.is_instances_terminated:
            return True
        msg = 'Current cluster is alive, please destroy cluster first.'
        logger.warning(msg)
        return False

    @property
    def is_instances_terminated(self) -> bool:
        return self.cloud_instance.is_ec2_stacks_terminated()

    @property
    def kylin_stack_name(self) -> str:
        return self.cloud_instance.kylin_stack_name

    @property
    def is_associated_public_ip(self) -> bool:
        return self.config[Params.ASSOSICATED_PUBLIC_IP.value] == 'true'

    @property
    def is_destroy_all(self) -> bool:
        return self.config[Params.ALWAYS_DESTROY_ALL.value] is True

    def get_resources(self, stack_name: str) -> Dict:
        return self.cloud_instance.get_stack_output(stack_name)

    def init_cluster(self) -> None:
        if not self.is_instances_ready:
            self.cloud_instance.create_vpc_stack()
            self.cloud_instance.create_rds_stack()
            self.cloud_instance.create_static_service_stack()
            self.cloud_instance.create_zk_stack()
            # Need to refresh its config and start them after created zks
            self.cloud_instance.after_create_zk_cluster()
            self.cloud_instance.create_spark_master_stack()
            self.cloud_instance.create_spark_slave_stack()
            self.cloud_instance.create_kylin_stack()
        self.cloud_instance.start_prometheus()
        logger.info('Cluster start successfully.')

    def get_kylin_address(self):
        kylin_resources = self.get_kylin_resources()
        kylin4_address = kylin_resources.get(Params.KYLIN4_PRIVATE_IP.value)
        if self.is_associated_public_ip:
            kylin4_address = kylin_resources.get(Params.KYLIN4_PUBLIC_IP.value)
        return kylin4_address

    def get_kylin_resources(self):
        if not self.is_cluster_ready:
            self.init_cluster()
        kylin_resources = self.get_resources(self.kylin_stack_name)
        return kylin_resources

    def destroy_aws_cloud(self):
        self.terminate_ec2_cluster()

    def terminate_ec2_cluster(self) -> Optional[Dict]:
        if self.is_cluster_terminated:
            return
        self.cloud_instance.terminate_spark_slave_stack()
        self.cloud_instance.terminate_spark_master_stack()
        self.cloud_instance.terminate_kylin_stack()
        self.cloud_instance.terminate_zk_stack()
        self.cloud_instance.terminate_static_service_stack()

        if self.is_destroy_all:
            # RDS will be removed by user manually.
            self.cloud_instance.terminate_rds_stack()
            self.cloud_instance.terminate_vpc_stack()
        # Don't need to terminate vpc stack, because it's free resource on your aws if don't use it.
        # Check again after terminated all node.
        assert self.is_cluster_terminated, f'Cluster was not terminated clearly, please check.'

    def alive_workers(self):
        self.cloud_instance.alive_workers()

    def scale_up_down(self, scale_type: str, node_type: str) -> None:
        # validate scale_type & node_type
        self.validate_scale_type(scale_type)
        self.validate_node_type(node_type)

        worker_nums = self.generate_scaled_list(node_type)

        exec_pool = ThreadPoolExecutor(max_workers=10)
        with exec_pool as pool:
            if scale_type == 'up':
                if node_type == 'kylin':
                    pool.map(self.cloud_instance.scale_up_kylin, worker_nums)
                else:
                    pool.map(self.cloud_instance.scale_up_worker, worker_nums)
            else:
                if node_type == 'kylin':
                    pool.map(self.cloud_instance.scale_down_kylin, worker_nums)
                else:
                    pool.map(self.cloud_instance.scale_down_worker, worker_nums)

    def after_scale(self, scale_type: str, node_type: str) -> None:
        if scale_type == 'up':
            logger.info(f"Checking exists prometheus config after scale up.")
            not_exists_nodes = self.cloud_instance.check_prometheus_config_after_scale(node_type)
            logger.info("Refresh prometheus config after scale up.")
            if not_exists_nodes:
                self.cloud_instance.refresh_prometheus_config_after_scale_up(not_exists_nodes)
        else:
            logger.info(f"Checking exists prometheus config after scale down.")
            exists_nodes = self.cloud_instance.check_prometheus_config_after_scale_down(node_type)
            logger.info("Refresh prometheus config after scale down.")
            if exists_nodes:
                self.cloud_instance.refresh_prometheus_config_after_scale_down(exists_nodes)
        self.cloud_instance.restart_prometheus_server()

    @staticmethod
    def validate_scale_type(scale_type: str) -> None:
        if scale_type is None or scale_type not in ['up', 'down']:
            msg = f'Not supported scale type: {scale_type}'
            logger.error(msg)
            raise Exception(msg)

    @staticmethod
    def validate_node_type(node_type: str) -> None:
        if node_type is None or node_type not in ['kylin', 'spark_worker']:
            msg = f'Not supported node type: {node_type}'
            logger.error(msg)
            raise Exception(msg)

    def generate_scaled_list(self, node_type: str) -> List:
        if node_type == 'kylin':
            kylin_nodes = literal_eval(self.config[Config.KYLIN_SCALE_NODES.value])
            return generate_nodes(kylin_nodes)
        else:
            worker_nodes = literal_eval(self.config[Config.SPARK_WORKER_SCALE_NODES.value])
            return generate_nodes(worker_nodes)
