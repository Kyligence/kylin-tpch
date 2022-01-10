import logging
from typing import List

from aws import AWS
from constant.config import Config
from constant.yaml_files import Tar
from instance import KylinInstance
from utils import download_tar, files_in_tar, download_jar, files_in_jars

logger = logging.getLogger(__file__)


class EngineUtils:

    def __init__(self, config):
        self.aws_instance = AWS(config)
        self.config = config
        
    @property
    def cloud_address(self) -> str:
        return self.config[Config.CLOUD_ADDR.value]

    def needed_tars(self) -> List:
        jdk_package = Tar.JDK.value
        kylin_package = Tar.KYLIN.value.format(KYLIN_VERSION=self.config['KYLIN_VERSION'])
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            kylin_package = Tar.KYLIN_WITH_SOFT.value.format(KYLIN_VERSION=self.config['KYLIN_VERSION'])
        hive_package = Tar.HIVE.value.format(HIVE_VERSION=self.config['HIVE_VERSION'])
        hadoop_package = Tar.HADOOP.value.format(HADOOP_VERSION=self.config['HADOOP_VERSION'])
        node_exporter_package = Tar.NODE.value.format(NODE_EXPORTER_VERSION=self.config['NODE_EXPORTER_VERSION'])
        prometheus_package = Tar.PROMETHEUS.value.format(PROMETHEUS_VERSION=self.config['PROMETHEUS_VERSION'])
        spark_package = Tar.SPARK.value.format(SPARK_VERSION=self.config['SPARK_VERSION'],
                                               HADOOP_VERSION=self.config['HADOOP_VERSION'])
        zookeeper_package = Tar.ZOOKEEPER.value.format(ZOOKEEPER_VERSION=self.config['ZOOKEEPER_VERSION'])
        packages = [jdk_package, kylin_package, hive_package, hadoop_package, node_exporter_package,
                    prometheus_package, spark_package, zookeeper_package]
        return packages

    def needed_jars(self) -> List:
        # FIXME: hard version of jars
        jars = []
        commons_configuration = 'commons-configuration-1.3.jar'
        mysql_connector = 'mysql-connector-java-5.1.40.jar'
        jars.append(commons_configuration)
        jars.append(mysql_connector)
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            kylin_soft_affinity_cache = 'kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar'
            alluxio_client = 'alluxio-2.6.1-client.jar'
            jars.append(kylin_soft_affinity_cache)
            jars.append(alluxio_client)
        return jars

    @staticmethod
    def needed_scripts() -> List:
        kylin = 'prepare-ec2-env-for-kylin4.sh'
        spark_master = 'prepare-ec2-env-for-spark-master.sh'
        spark_slave = 'prepare-ec2-env-for-spark-slave.sh'
        static_services = 'prepare-ec2-env-for-static-services.sh'
        zookeeper = 'prepare-ec2-env-for-zk.sh'
        return [kylin, spark_master, spark_slave, static_services, zookeeper]

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
        self.aws_instance.scale_up_down(scale_type, node_type)
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

    def download_tars(self) -> None:
        logger.info("Downloading packages.")
        packages = self.needed_tars()
        for package in packages:
            download_tar(filename=package)
        assert files_in_tar() == 8, \
            'Expected downloaded tars must be `7` ' \
            'which contains [jdk, kylin, hive, hadoop, zookeeper, node_exporter' \
            ', prometheus, spark].'

    def download_jars(self) -> None:
        logger.info("Downloading jars.")
        jars = self.needed_jars()
        for jar in jars:
            download_jar(jar)
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            assert files_in_jars() == 4, f"Needed jars must be 4, not {files_in_jars()}, " \
                                         f"which contains {jars}."
        else:
            assert files_in_jars() == 2, f"Needed jars must be 2, not {files_in_jars()}, " \
                                         f"which contains {jars}."

    def upload_needed_files(self) -> None:
        logger.info("Start to uploading tars.")
        self.aws_instance.upload_needed_files(self.needed_tars(), self.needed_jars(), self.needed_scripts())
        logger.info("Uploaded tars successfully.")

    def check_needed_files(self) -> None:
        self.aws_instance.check_needed_files(self.needed_tars(), self.needed_jars(), self.needed_scripts())

