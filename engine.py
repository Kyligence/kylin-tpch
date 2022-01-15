import logging
import os

import yaml

from constant.client import Client
from constant.config import Config
from constant.deployment import ScaleType, NodeType
from constant.yaml_files import File
from engine_utils import EngineUtils

logger = logging.getLogger(__name__)


class Engine:

    def __init__(self) -> None:
        d = os.path.dirname(__file__)
        with open(os.path.join(d, File.CONFIG_YAML.value)) as stream:
            config = yaml.safe_load(stream)
        self.config = config
        self.is_ec2_cluster = self.config[Config.DEPLOY_PLATFORM.value] == Client.EC2.value
        self.server_mode = None
        self.engine_utils = EngineUtils(self.config)

    def launch_cluster(self):
        logger.info('Ec2: first launch Instances And Kylin nodes.')
        self.engine_utils.launch_aws_kylin()
        logger.info('Kylin Cluster already start successfully.')

    def destroy_cluster(self):
        logger.info('Ec2: destroy useless nodes.')
        self.engine_utils.destroy_aws_kylin()
        logger.info('Ec2: destroy useless nodes successfully.')

    def list_alive_nodes(self) -> None:
        logger.info('Ec2: list alive nodes.')
        self.engine_utils.alive_nodes()
        logger.info('Ec2: list alive nodes successfully.')

    def scale_nodes(self, scale_type: str, node_type: str, cluster_num: int = None) -> None:
        if cluster_num:
            self.engine_utils.scale_nodes_in_cluster(
                scale_type=scale_type,
                node_type=node_type,
                cluster_num=cluster_num
            )
        else:
            self.engine_utils.scale_nodes(scale_type=scale_type, node_type=node_type)

        if node_type == NodeType.KYLIN.value and scale_type == ScaleType.UP.value:
            logger.info(f'Current Kylin Node already scaled, please wait a moment to access it.')
        logger.info(f"Current scaling {scale_type} {node_type} nodes "
                    f"in {cluster_num if cluster_num else 'default'} cluster successfully.")

    def scale_cluster(self, scale_type: str) -> None:
        logger.info(f'Current scaling {scale_type} cluster nodes.')
        self.engine_utils.scale_cluster(scale_type=scale_type)
        logger.info(f'Scaled {scale_type} cluster nodes successfully.')

    def is_inited_env(self) -> bool:
        try:
            self.engine_utils.check_needed_files()
            return True
        except AssertionError:
            return False

    def init_env(self) -> None:
        # validate s3 bucket path
        self.engine_utils.validate_s3_bucket()
        if self.is_inited_env():
            logger.info('Env already inited, skip init again.')
            return
        self.engine_utils.download_tars()
        self.engine_utils.download_jars()
        self.engine_utils.upload_needed_files()
        # check again
        assert self.is_inited_env()
