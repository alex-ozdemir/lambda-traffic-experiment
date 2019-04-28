#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

import argparse
import bisect
import boto3
import botocore
from datetime import datetime, timezone
import functools as ft
import glob
import os
import os.path
import pprint
import random
import re
import string
import sys
import time

s3 = boto3.client('s3')

def sync_experiment(exp_name):
    more = True
    page_token = None

    while more:
        if page_token is not None:
            resp=s3.list_objects_v2(Bucket='aozdemir-network-test', Prefix=exp_name, ContinuationToken=page_token)
        else:
            resp=s3.list_objects_v2(Bucket='aozdemir-network-test', Prefix=exp_name)

        more = resp['IsTruncated']
        if more:
            page_token = resp['ContinuationToken']

        directory = f'data/{exp_name}'
        if not os.path.exists(directory):
            os.makedirs(directory)

        if resp['KeyCount'] > 0:
            for entry in resp['Contents']:
                key = entry['Key']
                fname = f'data/{key}'
                do_copy = False
                if os.path.exists(fname):
                    local_mtime = os.path.getmtime(fname)
                    remote_mtime = entry['LastModified'].timestamp()
                    do_copy = local_mtime < remote_mtime
                else:
                    do_copy = True
                if do_copy:
                    print(f'Downloading {key}')
                    s3.download_file('aozdemir-network-test', key, fname)

if len(sys.argv) != 2:
    print('Provide an experiment name as the first argument')
    sys.exit(2)
exp_name = sys.argv[1]

sync_experiment(exp_name)

def agg(df, key_fields, const_fields, sum_fields):
    M = {}
    for (i, r) in df.iterrows():
        key = tuple(r[f] for f in key_fields)
        const = tuple(r[f] for f in const_fields)
        sum_ = tuple(r[f] for f in sum_fields)
        if key in M:
            c, s, j = M[key]
            assert c == const, f'Row {j} had const values {c} but row {i} had {const}'
            ss = tuple(a + b for a, b in zip(s, sum_))
            M[key] = (const, ss, j)
        else:
            M[key] = (const, sum_, i)
    data = [ key + v[0] + v[1] for key, v in M.items() ]
    return pd.DataFrame(data=data,columns=key_fields+const_fields+sum_fields)

split_link = pd.concat([pd.read_csv(f'data/{exp_name}/{f}') for f in os.listdir(f'data/{exp_name}') if re.match('^\\d*.csv', f)], ignore_index=True)

links = agg(split_link, ['from','to','round'], ['duration','packets_per_ms'], ['bytes_s','bytes_r','packets_s','packets_r'])
links['senders'] = [int(a.bytes_s > 0) for _, a in links.iterrows()]
links['receivers'] = [int(a.bytes_r > 0) for _, a in links.iterrows()]

linkfile = f'data/{exp_name}/links.csv'
links.to_csv(linkfile)
print(f'Link data written to {linkfile}')

net = agg(links, ['round', 'to'], ['duration','packets_per_ms','senders'], ['receivers', 'bytes_s','bytes_r','packets_s','packets_r'])
print(net)
net = agg(net, ['round'], ['duration','packets_per_ms','receivers'], ['senders', 'bytes_s','bytes_r','packets_s','packets_r'])

net['rate_r'] = net.bytes_r / net.duration * 8 / 10 ** 6 / net.senders
net['rate_s'] = net.bytes_s / net.duration * 8 / 10 ** 6 / net.senders
net['% sent'] = net.packets_s / (net.packets_per_ms * net.duration * 1000 * net.senders) * 100
net['% loss'] = (net.packets_s - net.packets_r) / net.packets_s * 100
del net['bytes_r']
del net['bytes_s']
del net['packets_r']
del net['packets_s']

netfile = f'data/{exp_name}/net.csv'
net.to_csv(netfile)
print(f'Net data written to {netfile}')
print('Net data:')
print(net)
