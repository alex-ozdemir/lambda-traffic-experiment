#!/usr/bin/env python3

'''
Adapted from Sadjad's python script in pbrt-cloud
'''

import os
import zipfile
import argparse
import boto3
import botocore.exceptions

ANSI_RED = '\033[91m'
ANSI_RESET = '\033[0m'


def create_function_package(output, binary_path):
    PACKAGE_FILES = {
            "bootstrap": binary_path,
            }

    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as funczip:
        for fn, fp in PACKAGE_FILES.items():
            funczip.write(fp, fn)


def install_lambda_package(package_file, function_name, region):
    with open(package_file, 'rb') as pfin:
        package_data = pfin.read()

    client = boto3.client('lambda', region_name=region)

    try:
        client.delete_function(FunctionName=function_name)
        print("Deleted function '{}'.".format(function_name))
    except:
        pass

    try:
        response = client.create_function(
                FunctionName=function_name,
                Runtime='provided',
                Role='arn:aws:iam::387291866455:role/gg-lambda-role',
                Handler='main.handler',
                Code={
                    'ZipFile': package_data
                    },
                Timeout=900,
                MemorySize=3008,
                Environment={
                    'Variables': {
                        'RUST_BACKTRACE': '1'
                        }
                    },
                )
        print("Updated function '{}' ({}).".format(
            function_name, response['FunctionArn']))
    except botocore.exceptions.ClientError as e:
        print("{}Failed to update function because:{}\n  {}".format(
            ANSI_RED, ANSI_RESET, e.response['Error']['Message']))


def update_binary(cargo_binary_name):
    binary_path = './target/x86_64-unknown-linux-musl/release/{}'.format(
            cargo_binary_name)
    function_name = 'rust-test-{}'.format(cargo_binary_name)
    function_package = "{}.zip".format(function_name)

    if not os.path.exists(binary_path):
        raise Exception("Cannot find {} binary at {}".format(
            cargo_binary_name, binary_path))
    try:
        create_function_package(function_package, binary_path)
        print("Installing lambda function {}... ".format(function_name), end='')
        install_lambda_package(function_package, function_name, os.environ.get('AWS_REGION'))
    finally:
        os.remove(function_package)


def main():
    parser = argparse.ArgumentParser(
            description="Update the code for Lambda functions.")
    args = parser.parse_args()
    #update_binary('sender')
    #update_binary('receiver')
    update_binary('remote')


if __name__ == '__main__':
    main()
