#!/usr/bin/python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

import argparse
import bisect
import boto3
import botocore
import functools as ft
import glob
import os
import os.path
import random
import re
import string
import sys

fname = sys.argv[1]
df = pd.read_csv(fname)
gpd = df.groupby('round').agg(np.sum)
n_senders = df.max().to + 1
f = 1 / (n_senders * (n_senders - 1))
gpd.secs *= f
gpd.rate *= f
del gpd['from']
del gpd['to']
gpd['rate_r'] = gpd.bytes_r / gpd.secs * 8 / 10 ** 6 / n_senders
gpd['rate_s'] = gpd.bytes_s / gpd.secs * 8 / 10 ** 6 / n_senders
gpd['% sent'] = gpd.packets_s / (gpd.rate * gpd.secs * 1000 * n_senders) * 100
gpd['% loss'] = (gpd.packets_s - gpd.packets_r) / gpd.packets_s * 100
del gpd['bytes_r']
del gpd['bytes_s']
del gpd['packets_r']
del gpd['packets_s']
print(gpd)
