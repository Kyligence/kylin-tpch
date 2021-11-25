import logging
import os

import yaml
from collections import deque

from kylin import (
    launch_aws_kylin,
    destroy_aws_kylin
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

    def launch_cluster(self):
        logger.info('Ec2: first launch Instances And Kylin nodes')
        launch_aws_kylin(self.config)

    def destroy_cluster(self):
        logger.info('Ec2: destroy useless nodes')
        destroy_aws_kylin(self.config)
