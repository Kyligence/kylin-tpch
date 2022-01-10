import os

CUR_DIR = os.path.dirname(__file__)
BACKUP_PATH = os.path.join(CUR_DIR, '..', 'backup')
JARS_PATH = os.path.join(BACKUP_PATH, 'jars')
SCRIPTS_PATH = os.path.join(BACKUP_PATH, 'scripts')
TARS_PATH = os.path.join(BACKUP_PATH, 'tars')

ROOT_DIR = os.path.join(CUR_DIR, '..')
TEMPLATES_PATH = os.path.join(ROOT_DIR, 'cloudformation_templates')