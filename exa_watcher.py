#!/usr/bin/env python3
import pandas as pd
import logging
import json
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

def slurm_from_json(file):
    with open(file, 'r') as f:
        sacct_row = json.load(f)
        return SlurmJob((tuple(sacct_row.values())))

if __name__ == '__main__':
    rows = read_sa('example_sa.txt')

    print(slurm_from_json('16740130_job.json'))