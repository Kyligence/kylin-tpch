import os
import logging
from typing import List, Tuple, Generator

import requests

from constant.path import TARS_PATH, JARS_PATH, TEMPLATES_PATH

logger = logging.getLogger(__file__)


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


download_base_url = 'https://s3.cn-north-1.amazonaws.com.cn/public.kyligence.io/kylin'


def full_path_of_yaml(yaml_name: str) -> str:
    return os.path.join(TEMPLATES_PATH, yaml_name)


def download_tar(filename: str) -> None:
    base_url = download_base_url + '/tar/'
    url = base_url + filename
    download(url=url, dest_folder=TARS_PATH, filename=filename)


def download_jar(filename: str) -> None:
    base_url = download_base_url + '/jars/'
    url = base_url + filename
    download(url=url, dest_folder=JARS_PATH, filename=filename)


def download(url: str, dest_folder: str, filename: str) -> None:
    if not os.path.exists(dest_folder):
        # create folder if it does not exist
        os.makedirs(dest_folder)

    file_path = os.path.join(dest_folder, filename)
    if os.path.exists(file_path):
        logger.info(f'{filename} already exists, skip download it.')
        return
    r = requests.get(url, stream=True)
    if r.ok:
        logger.info(f"saving to {os.path.abspath(file_path)}.")
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        logger.error("Download failed: status code {}\n{}".format(r.status_code, r.text))


def files_in_tar() -> int:
    if not os.path.exists(TARS_PATH):
        logger.error(f'{TARS_PATH} does exists, please check.')
        return 0
    return sum(1 for _ in listdir_nohidden(TARS_PATH))


def files_in_jars() -> int:
    if not os.path.exists(JARS_PATH):
        logger.error(f'{JARS_PATH} does exists, please check.')
        return 0
    return sum(1 for _ in listdir_nohidden(JARS_PATH))


def list_dir(dest_folder: str) -> List:
    return os.listdir(dest_folder)


def listdir_nohidden(dest_folder: str) -> Generator:
    for f in os.listdir(dest_folder):
        if not f.startswith('.'):
            yield f
