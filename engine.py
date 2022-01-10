import logging
import os

import yaml

from aws_utils import EngineUtils
from constant.client import Client
from constant.config import Config
from constant.yaml_files import File

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

    def list_alive_workers(self) -> None:
        logger.info('Ec2: list alive nodes.')
        self.engine_utils.alive_workers()
        logger.info('Ec2: list alive nodes successfully.')

    def scale_nodes(self, scale_type: str, node_type: str) -> None:
        self.engine_utils.scale_nodes(scale_type=scale_type, node_type=node_type)
        logger.info(f'Current scaling {scale_type} {node_type} nodes successfully.')

    def is_inited_env(self) -> bool:
        try:
            self.engine_utils.check_needed_files()
            return True
        except AssertionError:
            return False

    def init_env(self) -> None:
        if self.is_inited_env():
            logger.info('Env already inited, skip init again.')
            return
        self.engine_utils.download_tars()
        self.engine_utils.download_jars()
        self.engine_utils.upload_needed_files()
        # check again
        assert self.is_inited_env()
