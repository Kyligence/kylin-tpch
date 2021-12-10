import logging.config

import argparse


def deploy_on_aws(deploy_type: str) -> None:
    from engine import Engine
    aws_engine = Engine()
    if not aws_engine.is_ec2_cluster:
        msg = f'Only supported platform: EC2, please check `DEPLOY_PLATFORM`'
        raise Exception(msg)
    if deploy_type == 'deploy':
        aws_engine.launch_cluster()
    elif deploy_type == 'destroy':
        aws_engine.destroy_cluster()
    elif deploy_type == 'scale_up':
        aws_engine.scale_workers('up')
    elif deploy_type == 'scale_down':
        aws_engine.scale_workers('down')


if __name__ == '__main__':
    logging.config.fileConfig('logging.ini')

    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False, default='deploy', dest='type',
                        choices=['deploy', 'destroy', 'scale_up', 'scale_down'],
                        help="Use 'deploy' to create a cluster or 'destroy' to delete cluster")
    args = parser.parse_args()
    deploy_on_aws(args.type)
