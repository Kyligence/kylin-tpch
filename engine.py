import logging
import os

import yaml
from collections import deque

from kylin import (
    launch_aws_kylin,
    destroy_aws_kylin,
    scale_aws_worker,
    scale_down_aws_worker
)

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
        self.standby_nodes = deque([4, 5])
        self.scaled_nodes = deque()

    def launch_cluster(self):
        logger.info('Ec2: first launch Instances And Kylin nodes')
        launch_aws_kylin(self.config)

    def destroy_cluster(self):
        logger.info('Ec2: destroy useless nodes')
        destroy_aws_kylin(self.config)

    def scale_up_workers(self):
        try:
            scaled_worker_index = self.standby_nodes.popleft()
        except IndexError:
            logger.error(f'Current do not has any node to scale up.')
            return
        self.scaled_nodes.append(scaled_worker_index)
        scale_aws_worker(scaled_worker_index, self.config)

    def scale_down_workers(self):
        try:
            expected_down_node = self.scaled_nodes.popleft()
        except IndexError:
            logger.error(f'Current do not has any node to scale down.')
            return
        self.standby_nodes.append(expected_down_node)
        scale_down_aws_worker(expected_down_node)
