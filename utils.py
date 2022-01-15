import logging
import os
from typing import List, Tuple, Generator, Dict

import requests
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from constant.path import (
    TARS_PATH,
    JARS_PATH,
    TEMPLATES_PATH,
    KYLIN_PROPERTIES_TEMPLATE_DIR,
)

logger = logging.getLogger(__name__)


class Utils:
    DOWNLOAD_BASE_URL = 'https://s3.cn-north-1.amazonaws.com.cn/public.kyligence.io/kylin'

    @staticmethod
    def generate_nodes(scale_nodes: Tuple) -> List:
        if not scale_nodes:
            return []
        _from, _to = scale_nodes
        if _from == _to:
            return [_from]
        return list(range(_from, _to + 1))

    @staticmethod
    def read_template(file_path: str):
        with open(file=file_path, mode='r') as template:
            res_template = template.read()
        return res_template

    @staticmethod
    def full_path_of_yaml(yaml_name: str) -> str:
        return os.path.join(TEMPLATES_PATH, yaml_name)

    @staticmethod
    def download_tar(filename: str) -> None:
        base_url = Utils.DOWNLOAD_BASE_URL + '/tar/'
        url = base_url + filename
        Utils.download(url=url, dest_folder=TARS_PATH, filename=filename)

    @staticmethod
    def download_jar(filename: str) -> None:
        base_url = Utils.DOWNLOAD_BASE_URL + '/jars/'
        url = base_url + filename
        Utils.download(url=url, dest_folder=JARS_PATH, filename=filename)

    @staticmethod
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

    @staticmethod
    def files_in_tar() -> int:
        if not os.path.exists(TARS_PATH):
            logger.error(f'{TARS_PATH} does exists, please check.')
            return 0
        return sum(1 for _ in Utils.listdir_nohidden(TARS_PATH))

    @staticmethod
    def files_in_jars() -> int:
        if not os.path.exists(JARS_PATH):
            logger.error(f'{JARS_PATH} does exists, please check.')
            return 0
        return sum(1 for _ in Utils.listdir_nohidden(JARS_PATH))

    @staticmethod
    def list_dir(dest_folder: str) -> List:
        return os.listdir(dest_folder)

    @staticmethod
    def listdir_nohidden(dest_folder: str) -> Generator:
        for f in os.listdir(dest_folder):
            if not f.startswith('.'):
                yield f

    @staticmethod
    def render_properties(params: Dict, cluster_num: int = None, properties_template: str = 'kylin.properties') -> None:
        search_path = KYLIN_PROPERTIES_TEMPLATE_DIR.format(cluster_num=cluster_num if cluster_num else 'default')

        dest_path = os.path.join(search_path, 'kylin.properties')
        rendered_file = os.path.join(search_path, '.rendered')
        if os.path.exists(rendered_file):
            logger.info(f'{dest_path} already rendered. Skip render it again.')
            return

        env = Environment(loader=FileSystemLoader(searchpath=search_path))
        try:
            template = env.get_template(properties_template)
        except TemplateNotFound:
            raise Exception(f'Properties template: {properties_template} not in the path: {search_path}.\n '
                            f'Please copy the needed kylin.properties template in `backup/properties/templates` '
                            f'to `backup/properties/{cluster_num}`\n. If `backup/properties/{cluster_num}` not exists, '
                            f'please make it and rename the template file to `kylin.properties` in this dir.')
        output_from_parsed_template = template.render(params)

        with open(dest_path, 'w') as f:
            f.write(output_from_parsed_template)
        # touch a file with current time
        with open(rendered_file, 'a'):
            os.utime(rendered_file, None)
        logger.info(f'Current {dest_path} rendered.')
