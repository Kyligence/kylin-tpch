import ast
import logging
import os
from typing import Tuple, List

import yaml

from aws_utils import EngineUtils

logger = logging.getLogger(__name__)


class Engine:

    def __init__(self) -> None:
        d = os.path.dirname(__file__)
        with open(os.path.join(d, 'kylin_configs.yaml')) as stream:
            config = yaml.safe_load(stream)
        self.config = config
        self.is_ec2_cluster = self.config['DEPLOY_PLATFORM'] == 'ec2'
        self.server_mode = None
        # default alive workers num is 3, so scaling workers index must be bigger than 3
        self.default_num = 3
        self.scale_up_nodes: Tuple = ast.literal_eval(self.config['SCALE_NODES'])
        self.engine_utils = EngineUtils(self.config)

    def launch_cluster(self):
        logger.info('Ec2: first launch Instances And Kylin nodes')
        self.engine_utils.launch_aws_kylin()
        logger.info('Kylin Cluster already start successfully.')

    def destroy_cluster(self):
        logger.info('Ec2: destroy useless nodes')
        self.engine_utils.destroy_aws_kylin()
        logger.info('Ec2: destroy useless nodes successfully.')

    def scale_workers(self, scale_type: str):
        self._validate_scale()
        workers = self._generate_nodes()
        self.engine_utils.scale_aws_worker(worker_nums=workers, scale_type=scale_type)
        logger.info(f'Current scaling {scale_type} total {len(workers)} nodes successfully.')

    def _validate_scale(self):
        if not self.scale_up_nodes:
            msg = f'Scale nodes is none, please check.'
            logger.error(msg)
            raise Exception(msg)

        if not isinstance(self.scale_up_nodes, tuple):
            msg = f'Scale nodes type is invalid, please check.'
            logger.error(msg)
            raise Exception(msg)

        if len(self.scale_up_nodes) != 2 or self.scale_up_nodes[0] > self.scale_up_nodes[1]:
            msg = f'Invalid `SCALE_NODES`, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _generate_nodes(self) -> List:
        if self.scale_up_nodes[0] == self.scale_up_nodes[1]:
            return [self.scale_up_nodes[0]]
        return list(range(self.scale_up_nodes[0], self.scale_up_nodes[1] + 1))
