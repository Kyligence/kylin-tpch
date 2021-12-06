import logging.config

import argparse


def deploy_on_aws(deploy_type: str) -> None:
    from engine import Engine
    aws_engine = Engine()
    if deploy_type == 'deploy':
        aws_engine.launch_cluster()
    elif deploy_type == 'destroy':
        aws_engine.destroy_cluster()


if __name__ == '__main__':
    logging.config.fileConfig('logging.ini')

    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False, default='deploy', dest='type',
                        help="Use 'deploy' to create a cluster or 'destroy' to delete cluster")
    args = parser.parse_args()
    deploy_on_aws(args.type)
