#!/usr/bin/env python3
import pandas as pd
import json
import glob
import os
import argparse
import sys
from datetime import datetime
from urllib.error import HTTPError
from slack import WebClient
from slack.errors import SlackApiError

def read_sa(file):
    table = pd.read_table(
        file,
        names = ['id', 'name', 'state', 'code'],
        skiprows = 2,
        delim_whitespace = True
    )

    # convert table to list of tuples
    return list(table.itertuples(index = False, name = None))

class SlurmJob:
    def __init__(self, sacct_row) -> None:
        self.id, self.name, self.state, self.code = sacct_row

    def __repr__(self) -> str:
        return f'Job {self.id} named {self.name}: {self.state}'

    def write_json(self) -> None:
        with open(f'job_{self.id}.json', 'w') as f:
            json.dump({
                'id': self.id,
                'name': self.name,
                'state': self.state,
                'code': self.code
            }, f)

    def announce(self) -> None:
        print(self.id)

def slurm_from_json(file):
    with open(file, 'r') as f:
        sacct_row = json.load(f)
        return SlurmJob((tuple(sacct_row.values())))

def slurms_from_sacct(file):
    slurms = []
    for row in read_sa(file):
        slurms.append(SlurmJob(row))

    return slurms

def compare_sa(old, new):
    for new_slurm in new:
        if new_slurm.state == 'PENDING':
            continue
        else:
            try:
                old_slurm = next(x for x in old if x.id == new_slurm.id)
                if old_slurm.state != new_slurm.state:
                    new_slurm.announce()
            except StopIteration:
                new_slurm.announce()

        new_slurm.write_json()

if __name__ == '__main__':

    olds = []
    for file in glob.glob('*.json'):
        olds.append(slurm_from_json(file))

    news = slurms_from_sacct('example_future_sa.txt')

    compare_sa(olds, news)