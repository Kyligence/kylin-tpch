#! /usr/bin/python
import optparse
import os
import requests
import uuid
import time
import sys

parser = optparse.OptionParser(description='Kybot upload client')

parser.add_option('-s', action='store',
    dest='server', default='http://127.0.0.1:7070/kylin',
    help='Kylin server url')
parser.add_option('-u', action='store',
    dest='username', default='ADMIN',
    help='Kylin username')
parser.add_option('-p', action='store',
    dest='password', default='KYLIN',
    help='Kylin password')
parser.add_option('-d', action='store',
    dest='directory', help='Query directory')
parser.add_option('-o', action='store',
    dest='project', default='tpch',
    help='Project')
parser.add_option('-r', action='store',
    dest='rounds', default='1',
    help='how many rounds to run')
parser.add_option('-t', action='store',
    dest='type', default='kylin',
    help='which one to query? kylin or hive?')
parser.add_option('-c', action='store',
    dest='scale', default='kylin',
    help='scale factor')


def initHttpSession():
    global s
    s = requests.Session()

def login(config):
    print("Login to %s" % config.server)
    s.auth = (config.username, config.password)
    r = s.post("%s%s" % (config.server, '/api/user/authentication'))

    if r.status_code != requests.codes.ok:
        print("Login Failed!")
        sys.exit(1)

def do_query_kylin(sql):
    payload = {
        "acceptPartial": True,
        "limit": 50000,
        "offset": 0,
        "project": "tpch",
        "sql": sql
    }
    headers = {"Content-Type": "application/json"}
    r = s.post("%s%s" % (config.server, '/api/query'), json=payload, headers=headers)

    if r.status_code != requests.codes.ok:
        return -1
    else:
        result = r.json()
        return int(result['duration'])


def query_kylin(config):
    try:
        dirpath = config.directory + "/"
        query_files = os.listdir(config.directory)
        rounds = int(config.rounds)
        for query_file in query_files:
            if query_file.endswith(".sql"):
                with open(dirpath + query_file) as f:
                    sql = f.read()
                    duration = 0
                    for i in range(0, rounds):
                        d = do_query_kylin(sql)
                        if d == -1:
                            break
                        duration += d
                    print("%s     %d" % (query_file, duration / rounds))
    except OSError,e:
        print("Failed to open files.")

def do_query_hive(sql, name, scale):
    hive_cmd = "hive -e \" use tpch_flat_orc_%s; %s \" > %s.out" % (sql, scale, name)
    os.system(hive_cmd)

def query_hive(config):
    try:
        dirpath = config.directory + "/original-queries/"
        query_files = os.listdir(config.directory)
        rounds = int(config.rounds)
        for query_file in query_files:
            if query_file.endswith(".sql"):
                with open(dirpath + query_file) as f:
                    sql = f.read()
                    do_query_hive(sql, query_file, config.scale)
    except OSError,e:
        print("Failed to open files.")

def query(config):
    if config.type == "kylin":
        initHttpSession()
        login(config)
        query_kylin(config)       
    else:
        query_hive(config)


if __name__ == "__main__":
    (config, args) = parser.parse_args()   
    query(config)