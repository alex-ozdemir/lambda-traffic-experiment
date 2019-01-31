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
BUCKET_NAME = 'rust-test-2'

def merge_experiments(e1, e2):
    s1_path = './data/sender-results-{}.csv'.format(e1)
    r1_path = './data/receiver-results-{}.csv'.format(e1)
    s2_path = './data/sender-results-{}.csv'.format(e2)
    r2_path = './data/receiver-results-{}.csv'.format(e2)
    s1_df = pd.read_csv(s1_path)
    r1_df = pd.read_csv(r1_path)
    s2_df = pd.read_csv(s2_path)
    r2_df = pd.read_csv(r2_path)
    smallest_unused_packet_e1 = max(row.first_packet_id + row.packets_sent for (i, row) in s1_df.iterrows())
    smallest_unused_packet_e2 = max(row.first_packet_id + row.packets_sent for (i, row) in s2_df.iterrows())
    for (i, row) in s2_df:
        row.first_packet_id += smallest_unused_packet_e1
    for (i, row) in r2_df:
        row.packet_id += smallest_unused_packet_e1
    combined_s_df = pd.concat([s1_df, s2_df], ignoreIndex = True)
    combined_r_df = pd.concat([r1_df, r2_df], ignoreIndex = True)
    new_e_id = random.randrange(0, 2 ** 32 - 1)
    combined_s_path = './data/sender-results-{}.csv'.format(new_e_id)
    combined_r_path = './data/receiver-results-{}.csv'.format(new_e_id)
    combined_r_df.to_csv(combined_r_path)
    combined_s_df.to_csv(combined_s_path)
    print('New experiment {} of {} sent packets created!'.format(new_e_id, smallest_unused_packet_e1 + smallest_unused_packet_e2))

def partition_packets(packets, bucket_ranges):
    '''
    Given a list of value and a list of N half-open intervals,
    returns N lists containing the (sorted) values in each interval.
    '''
    packets.sort()
    buckets = []
    for start, end in bucket_ranges:
        left_i = bisect.bisect_left(packets, start)
        right_i = bisect.bisect_left(packets, end)
        buckets.append(packets[left_i:right_i])
    packets.clear()
    return buckets

def open_sender_results(experiment):
    data_glob = './data/sender-results-{}*.csv'.format(experiment)
    files = glob.glob(data_glob)
    if len(files) == 0:
        print("Missing sender data: {}".format(data_glob))
        sys.exit(2)
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

def open_receiver_results(experiment):
    data_glob = './data/receiver-results-{}*.csv'.format(experiment)
    files = glob.glob(data_glob)
    if len(files) == 0:
        print("Missing reciever data: {}".format(data_glob))
        sys.exit(2)
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

def analyze(experiment, emit_csv=False):
    send_res = open_sender_results(experiment)
    rec_res = open_receiver_results(experiment)

    buckets = partition_packets(rec_res['packet_id'].tolist(), [
                                (row.first_packet_id,
                                 row.first_packet_id + row.packets_sent)
                                    for (i, row) in send_res.iterrows()])

    send_res['packets_recv'] = [len(packets) for packets in buckets]

    send_res['real_duration_ns'] = send_res['write_time'] + send_res['sleep_time']
    send_res['real_dur_ms'] = (send_res['real_duration_ns'] / 10 ** 6)

    send_res['pause_ms'] = send_res['sleep_period'] / 10 ** 6
    send_res['rate_sent'] = send_res['bytes_sent'] * 8 / send_res['real_duration_ns'] * 1000
    send_res['rate_recv'] = send_res['packets_recv'] * 1400 * 8/ send_res['real_duration_ns'] * 1000
    send_res['ex_rate_sent'] = send_res['packets_per_ms'] * 1400 * 8 / 10 ** 6 * 1000
    send_res['sent %'] = send_res['rate_sent'] / send_res['ex_rate_sent'] * 100
    send_res['write %'] = (send_res['write_time']) / send_res['real_duration_ns'] * 100
    send_res['sleep %'] = (send_res['sleep_time']) / send_res['real_duration_ns'] * 100

    #send_res['label'] = ['{}p/ms {}ms'.format(row.packets_per_ms, int(row.sleep_period/10**6)) for i, row in send_res.iterrows()]
    #send_res['label'] = ['{}ms'.format(int(row.sleep_period/10**6)) for i, row in send_res.iterrows()]
    #send_res['label'] = ['{}p/ms'.format(int(row.packets_per_ms)) for i, row in send_res.iterrows()]
    #send_res['label'] = ['{}'.format(int(row.sender_id)) for i, row in send_res.iterrows()]


    df = send_res
    if emit_csv:
        df2 = df.rename(columns={
            'rate_sent':'rate_sent_Mb_s',
            'rate_recv':'rate_recv_Mb_s',
            'ex_rate_sent':'expected_rate_sent',
            'sent %':'percent_expected_data_sent',
            'write %': 'percent_time_spent_writing',
            'sleep %': 'percent_time_spent_pausing',
            'duration': 'planned_duration_ns',
            'real_dur_ms': 'real_duration_ms',
            })
        if not os.path.exists('tables'):
            os.makedirs('tables')
        df2.to_csv('tables/{}.csv'.format(experiment), index_label='round_i')
    else:
        print(df)
        df_with_plot_data = df[['packets_per_ms','sender_id','rate_recv','rate_sent']]
        if len(set(df_with_plot_data.sender_id)) > 1:
            grouped = df_with_plot_data.groupby(['packets_per_ms']).sum().reset_index()
            grouped['sender_id'] = -1
            df_with_plot_data = pd.concat([df_with_plot_data, grouped], ignore_index=True)
        df_with_plot_data['label'] = ['Σ' if row.sender_id < 0 else str(int(row.sender_id)) for i, row in df_with_plot_data.iterrows()]
        print(df_with_plot_data)
        fig, ax = plt.subplots()
        cmap = cm.get_cmap('Spectral')
        df_with_plot_data.plot(x='rate_sent',
                y='rate_recv',
                c='packets_per_ms',
                colormap='gnuplot',
                marker='x',
                kind='scatter',
                ax=ax,
                s=40,
                linewidth=2)

        for k, v in df_with_plot_data.iterrows():
            ax.annotate(v.label, (v.rate_sent, v.rate_recv),
                        xytext=(2, -10), textcoords='offset points',
                        family='monospace', fontsize=10, color='darkslategrey')
        plt.xlabel('Sent (Mb/s)')
        plt.ylabel('Received (Mb/s)')
        plt.xlim(left=-0.15)
        plt.ylim(bottom=-0.15)
        plt.title('Traffic')

        lims = [
            0,
            np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
        ]

        ax.plot(lims, lims, 'k-', linewidth=0.25, alpha=0.75, zorder=0)
        # plt.savefig('out.png', dpi=600)
        plt.show()

def ensure_sender_data_present(experiment):
    data_path_glob = './data/sender-results-{}-*.csv'.format(experiment)
    data_path_regex = './data/sender-results-{}-(\d+)-of-(\d+).csv'.format(experiment)
    data_path_prefix = 'sender-results-{}-'.format(experiment)
    files_that_match_glob = glob.glob(data_path_glob)
    download = False
    if len(files_that_match_glob) == 0:
        download = True
    else:
        results = re.search(data_path_regex, files_that_match_glob[0])
        assert results is not None
        n_senders = int(results.group(2))
        if len(files_that_match_glob) < n_senders:
            download = True
    if download:
        s3 = boto3.client('s3')
        resource = boto3.resource('s3')
        try:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=data_path_prefix)
            if 'Contents' not in response:
                print("There is no send data for experiment {}.".format(experiment))
                sys.exit(2)
            prefixes = [res['Key'] for res in response['Contents']]
            for (i, key) in enumerate(prefixes):
                print('Downloading the sender data {}/{}'.format(i + 1, len(prefixes)))
                try:
                    target_path = './data/{}'.format(key)
                    resource.Bucket(BUCKET_NAME).download_file(key, target_path)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("There is no send data for sender {}.".format(i+1))
                        sys.exit(2)
                    else:
                        raise
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("There is no send data for experiment {}.".format(experiment))
                sys.exit(2)
            else:
                raise

def ensure_receiver_data_present(experiment):
    data_path_glob = './data/receiver-results-{}-*.csv'.format(experiment)
    data_path_regex = './data/receiver-results-{}-(\d+)-of-(\d+).csv'.format(experiment)
    data_path_prefix = 'receiver-results-{}-'.format(experiment)
    files_that_match_glob = glob.glob(data_path_glob)
    download = False
    if len(files_that_match_glob) == 0:
        download = True
    else:
        results = re.search(data_path_regex, files_that_match_glob[0])
        assert results is not None
        n_receivers = int(results.group(2))
        if len(files_that_match_glob) < n_receivers:
            download = True
    if download:
        s3 = boto3.client('s3')
        resource = boto3.resource('s3')
        try:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=data_path_prefix)
            if 'Contents' not in response:
                print("There is no receipt data for experiment {}.".format(experiment))
                sys.exit(2)
            prefixes = [res['Key'] for res in response['Contents']]
            for (i, key) in enumerate(prefixes):
                print('Downloading the receiver data {}/{}. This may be slow'.format(i + 1, len(prefixes)))
                try:
                    target_path = './data/{}'.format(key)
                    resource.Bucket(BUCKET_NAME).download_file(key, target_path)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("There is no receipt data for receiver {}.".format(i+1))
                        sys.exit(2)
                    else:
                        raise
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("There is no receipt data for experiment {}.".format(experiment))
                sys.exit(2)
            else:
                raise



def ensure_experiment_data_present(experiment):
    if os.path.exists('data') and not os.path.isdir('data'):
        raise Exception('Error! `data` exists bust is not a directory')
    if not os.path.isdir('data'):
        os.makedirs('data')
    ensure_sender_data_present(experiment)
    ensure_receiver_data_present(experiment)

def main():
    parser = argparse.ArgumentParser(description="Analyze experiments")
    parser.add_argument(
            '-t',
            '--table',
            action='store_true',
            default=False,
            help='emit a csv file instead of drawing a graph'
            )
    parser.add_argument(
            '-d',
            '--download',
            action='store_true',
            default=False,
            help='download the experimental data if it is absent'
            )
    parser.add_argument(
            'experiment',
            help='The numerical ID of the experiment to analyze'
            )

    args = parser.parse_args()

    if args.download:
        ensure_experiment_data_present(args.experiment)
    print('Analyzing')
    analyze(args.experiment, emit_csv=args.table)

main()

