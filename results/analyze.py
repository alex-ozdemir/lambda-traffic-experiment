#!/usr/bin/python

import argparse
import boto3
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import bisect
import os
import os.path
import string
import random
from matplotlib import cm

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

def analyze(experiment, emit_csv=False):
    send_res_path = './data/sender-results-{}.csv'.format(experiment)
    rec_res_path = './data/receiver-results-{}.csv'.format(experiment)
    send_res = pd.read_csv(send_res_path)
    rec_res = pd.read_csv(rec_res_path)
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
    send_res['label'] = ['{}p/ms'.format(int(row.packets_per_ms)) for i, row in send_res.iterrows()]

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
        print(df[[
            'packets_per_ms',
            'pause_ms',
            'write %',
            'real_dur_ms',
            'rate_sent',
            'rate_recv',
            ]])
        fig, ax = plt.subplots()
        cmap = cm.get_cmap('Spectral')
        df.plot(x='rate_sent',
                y='rate_recv',
                c='packets_per_ms',
                colormap='gnuplot',
                marker='x',
                kind='scatter',
                ax=ax,
                s=40,
                linewidth=2)

        # for k, v in df.iterrows():
        #     ax.annotate(v.label, (v.rate_sent, v.rate_recv),
        #                 xytext=(2, -10), textcoords='offset points',
        #                 family='monospace', fontsize=10, color='darkslategrey')
        plt.xlabel('Sent (Mb/s)')
        plt.ylabel('Received (Mb/s)')
        plt.xlim(left=-0.15)
        plt.ylim(bottom=-0.15)
        plt.title('Traffic Between Two λ Instances')

        lims = [
            0,
            np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
        ]

        ax.plot(lims, lims, 'k-', linewidth=0.25, alpha=0.75, zorder=0)
        plt.savefig('1by1-uncongested.pdf')
        # plt.savefig('out.png', dpi=600)
        plt.show()

def ensure_experiment_data_present(experiment):
    send_res_path = './data/sender-results-{}.csv'.format(experiment)
    rec_res_path = './data/receiver-results-{}.csv'.format(experiment)
    send_s3_name = 'sender-results-{}.csv'.format(experiment)
    rec_s3_name = 'receiver-results-{}.csv'.format(experiment)
    BUCKET_NAME = 'test-rust-2'
    if os.path.exists('data') and not os.path.isdir('data'):
        raise Exception('Error! `data` exists bust is not a directory')
    if not os.path.isdir('data'):
        os.makedirs('data')
    if not os.path.exists(send_res_path):
        s3 = boto3.resource('s3')
        try:
            s3.Bucket(BUCKET_NAME).download_file(send_s3_name, send_res_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("There is no send data for that experiment")
            else:
                raise
    if not os.path.exists(rec_res_path):
        s3 = boto3.resource('s3')
        try:
            print('Downloading the receipt data... This may take a while')
            s3.Bucket(BUCKET_NAME).download_file(rec_s3_name, rec_res_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("There is no receipt data for that experiment.")
            else:
                raise

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

