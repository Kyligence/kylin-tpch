import logging
from logging import config

import argparse
from engine import Engine

logger = logging.getLogger(__name__)


def deploy_on_aws(deploy_type: str) -> None:
    aws_engine = Engine()
    if deploy_type == 'deploy':
        aws_engine.launch_cluster()
    elif deploy_type == 'destroy':
        aws_engine.destroy_cluster()


if __name__ == '__main__':
    config.fileConfig('logging.ini')

    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False, action='store_true', default='destroy', dest='type')
    args = parser.parse_args()
    deploy_on_aws(args.type)
