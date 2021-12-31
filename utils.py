import os
from typing import List, Tuple


def generate_nodes(scale_nodes: Tuple) -> List:
    if not scale_nodes:
        return []
    _from, _to = scale_nodes
    if _from == _to:
        return [_from]
    return list(range(_from, _to + 1))


def read_template(file_path: str):
    with open(file=file_path, mode='r') as template:
        res_template = template.read()
    return res_template


cur_dir = os.path.dirname(__file__)
yaml_path = os.path.join(cur_dir, 'cloudformation_templates')


def full_path_of_yaml(yaml_name: str) -> str:
    return os.path.join(yaml_path, yaml_name)
