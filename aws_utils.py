import logging

from aws import AWS
from constant.config import Config
from instance import KylinInstance

logger = logging.getLogger(__file__)


class EngineUtils:

    def __init__(self, config):
        self.aws_instance = AWS(config)
        self.config = config
        
    @property
    def cloud_address(self) -> str:
        return self.config[Config.CLOUD_ADDR.value]

    def launch_aws_kylin(self):
        cloud_addr = self.get_kylin_address()
        # launch kylin
        kylin_instance = KylinInstance(host=cloud_addr, port='7070')
        assert kylin_instance.client.await_kylin_start(
            check_action=kylin_instance.client.check_login_state,
            timeout=1800,
            check_times=10
        )

    def destroy_aws_kylin(self):
        self.aws_instance.destroy_aws_cloud()

    def alive_workers(self):
        self.aws_instance.alive_workers()

    def scale_nodes(self, scale_type: str, node_type: str) -> None:
        logger.info(f'Current scaling {scale_type} {node_type} nodes.')
        assert self.is_cluster_ready() is True, 'Cluster nodes must be ready.'
        self.aws_instance.scale_up_down(self.config, scale_type, node_type)
        self.aws_instance.after_scale(scale_type, node_type)

    def is_cluster_ready(self) -> bool:
        if self.cloud_address:
            return True
        return self.aws_instance.is_cluster_ready

    def get_kylin_address(self) -> str:
        kylin_address = self.cloud_address
        if not kylin_address:
            kylin_address = self.aws_instance.get_kylin_address()
        assert kylin_address, f'kylin address is None, please check.'
        return kylin_address
