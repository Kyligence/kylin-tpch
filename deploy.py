import logging.config

import argparse


def deploy_on_aws(deploy_type: str, scale_type: str, node_type: str) -> None:
    from engine import Engine
    aws_engine = Engine()
    if not aws_engine.is_ec2_cluster:
        msg = f'Now only supported platform: EC2, please check `DEPLOY_PLATFORM`.'
        raise Exception(msg)
    if deploy_type == 'deploy':
        aws_engine.launch_cluster()
    elif deploy_type == 'destroy':
        aws_engine.destroy_cluster()
    elif deploy_type == 'list':
        aws_engine.list_alive_workers()
    elif deploy_type == 'scale':
        if not scale_type or not node_type:
            msg = 'Invalid scale params, `scale-type` and `node-type` must be not None.'
            raise Exception(msg)
        aws_engine.scale_nodes(scale_type, node_type)


if __name__ == '__main__':
    logging.config.fileConfig('logging.ini')

    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False, default='list', dest='type',
                        choices=['deploy', 'destroy', 'list', 'scale'],
                        help="Use 'deploy' to create a cluster or 'destroy' to delete cluster "
                             "or 'list' to list alive nodes.")
    parser.add_argument("--scale-type", required=False, dest='scale_type',
                        choices=['up', 'down'],
                        help="This param must be used with '--type' and '--node-type' "
                             "Use 'up' to scale up nodes or 'down' to scale down nodes. "
                             "Node type will be in ['kylin', 'spark-worker'].")
    parser.add_argument("--node-type", required=False, dest='node_type',
                        choices=['kylin', 'spark_worker'],
                        help="This param must be used with '--type' and '--scale-type' "
                             "Use 'kylin' to scale up/down kylin nodes "
                             "or 'spark-worker' to scale up/down spark_worker nodes. ")
    args = parser.parse_args()
    deploy_on_aws(args.type, args.scale_type, args.node_type)
