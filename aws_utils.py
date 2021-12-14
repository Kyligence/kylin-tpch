import logging
from typing import List

from aws import AWS
from constant.config import Config
from instance import KylinInstance

logger = logging.getLogger(__file__)


class EngineUtils:

    def __init__(self, config):
        self.aws_instance = AWS(config)
        self.config = config

    def launch_aws_kylin(self):
        cloud_addr = self.get_cloud_addr()
        kylin_mode = self.config[Config.EC2_MASTER_PARAMS.value]['Ec2KylinMode']
        # launch kylin
        kylin_instance = KylinInstance(host=cloud_addr, port='7070', home=None, mode=kylin_mode)
        assert kylin_instance.client.await_kylin_start(
            check_action=kylin_instance.client.check_login_state,
            timeout=1800,
            check_times=10
        )

    def destroy_aws_kylin(self):
        self.aws_instance.destroy_aws_cloud()

    def scale_aws_worker(self, worker_nums: List, scale_type: str):
        logger.info(f'Current scaling {scale_type} node: {worker_nums}.')
        assert self.get_cloud_addr(), 'Master node must be ready.'
        self.aws_instance.scale_up_down(self.config, worker_nums, scale_type)
        self.aws_instance.after_scale(worker_nums, scale_type)

    def get_cloud_addr(self) -> str:
        """
        retrieve the kylin and spark master node ip
        :return: cloud_addr which from the master public ip or private ip
        """
        # launch aws cluster
        if self.config[Config.CLOUD_ADDR.value] is None:
            cloud_addr = self.aws_instance.aws_cloud()
        else:
            cloud_addr = self.config[Config.CLOUD_ADDR.value]
        # make sure that cloud addr always exists
        assert cloud_addr is not None, 'cloud address is None, please check.'
        return cloud_addr
