#!/usr/bin/python

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
#print(df)
gpd = df.groupby('round').agg(np.sum)
n = df.max().to + 1
f = 1 / (n * (n-1))
gpd.secs *= f
gpd.rate *= f
del gpd['from']
del gpd['to']
gpd['loss'] = (gpd.packets_s - gpd.packets_r) / gpd.packets_s
gpd['Mb/s'] = gpd.bytes_r / gpd.secs * 8 / 10 ** 6 / n
gpd['% sent'] = gpd.packets_s / (gpd.rate * gpd.secs * 1000 * n)
print(gpd)
