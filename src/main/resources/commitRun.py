#!/usr/bin/python3
import base64
import boto3
import json
import os
import pymysql
import sys


def read_secret(secret_id):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    response = client.get_secret_value(SecretId=secret_id)
    secret = response.get('SecretString')

    # check for encoded secret
    if not secret:
        secret = base64.b64decode(response['SecretBinary'])

    # parse it as json
    return json.loads(secret)


def main():
    method = os.getenv('JOB_METHOD')
    stage = os.getenv('JOB_STAGE')

    # parse command line
    event = json.loads(sys.argv[1])

    # extract the output, inputs, and rds instance to use
    output = event['output']
    inputs = event['inputs']
    secret = read_secret(event['rds_secret'])

    # connect to the aggregator database
    conn = pymysql.connect(
        host=secret['host'],
        user=secret['username'],
        passwd=secret['password'],
        db=secret['dbname'],
    )

    # insert query
    sql = (
        'INSERT INTO `runs` '
        '  ( `method` '
        '  , `stage` '
        '  , `output` '
        '  , `input` '
        '  , `version` '
        '  ) '
        'VALUES (%s, %s, %s, %s, %s) '
        'ON DUPLICATE KEY UPDATE '
        '  version = VALUES(version), '
        '  timestamp = NOW() '
    )

    # insert all rows
    with conn.cursor() as c:
        for key, version in inputs:
            c.execute(sql, [
                method,
                stage,
                output,
                key,
                version,
            ])

    # done
    conn.commit()


if __name__ == '__main__':
    if not os.getenv('JOB_DRYRUN'):
        main()
