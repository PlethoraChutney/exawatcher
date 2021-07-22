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

    def announce(self, slack_client, slack_dm, old_state = None) -> None:
        if old_state:
            slack_client.chat_postMessage(
                channel = slack_dm,
                text = f'Hi! Job {self.name} ({self.id}) has changed from {old_state} to {self.state}.'
            )
        else:
            slack_client.chat_postMessage(
                channel = slack_dm,
                text = f'Hi! Job {self.name} ({self.id}) has changed to {self.state}.'
            )

def slurm_from_json(file):
    with open(file, 'r') as f:
        sacct_row = json.load(f)
        return SlurmJob((tuple(sacct_row.values())))

def slurms_from_sacct(file):
    slurms = []
    for row in read_sa(file):
        slurms.append(SlurmJob(row))

    return slurms

def compare_sa(old, new, client, dm):
    # remove old JSONs that aren't required anymore
    new_ids = [x.id for x in new]
    for slurm in [x.id for x in old if x.id not in new_ids]:
        os.remove(f'job_{slurm}.json')

    # announce jobs that have changed state
    for new_slurm in new:
        if new_slurm.state == 'PENDING':
            continue
        else:
            try:
                old_slurm = next(x for x in old if x.id == new_slurm.id)
                if old_slurm.state != new_slurm.state:
                    new_slurm.announce(client, dm, old_slurm.state)
            except StopIteration:
                new_slurm.announce(client, dm)

        new_slurm.write_json()

def make_slack_client(args):
    error_status = False

    if args.dm:
        slack_dm = args.dm
    else:
        try:
            slack_dm = os.environ['SLACK_DM']
        except KeyError:
            print('Please put your slack DM ID in env variable SLACK_MICROSCOPY_CHANNEL')
            error_status = True

    if args.token:
        slack_bot_token = args.token
    else:
        try:
            slack_bot_token = os.environ['SLACK_BOT_TOKEN']
        except KeyError:
            print('Please put your slack bot token in env variable SLACK_BOT_TOKEN')
            error_status = True
            slack_bot_token = False

    # if the user provided a bot token we can test it even without a channel
    if error_status and not slack_bot_token:
        sys.exit(1)
    
    slack_web_client = WebClient(token=slack_bot_token)
    try:
        slack_web_client.auth_test()
    except SlackApiError:
        print('Slack authentication failed. Please check your bot token.')
        error_status = True

    # we need to check this again in case setting the channel failed
    if error_status:
        sys.exit(1)
    
    return (slack_web_client, slack_dm)

def main(args) :
    slack_client, slack_dm = make_slack_client(args)


    olds = []
    for file in glob.glob(args.olds):
        olds.append(slurm_from_json(file))

    news = slurms_from_sacct(args.sacct)

    compare_sa(olds, news, slack_client, slack_dm)

parser = argparse.ArgumentParser(
    description='Check for changes in slurm jobs. Requires custom sacct output (see README).'
)
parser.add_argument(
    'olds',
    type = str,
    help = 'Glob for old slurm job JSONs'
)
parser.add_argument(
    'sacct',
    type = str,
    help = 'Output of `sacct` command'
)
parser.add_argument(
    '--token',
    help = 'Slack bot token. If not provided, will use SLACK_BOT_TOKEN env variable',
    type = str
)
parser.add_argument(
    '--dm',
    help = 'Slack DM ID to post to. If not provided, will use SLACK_DM env variable',
    type = str
)

args = parser.parse_args()

if __name__ == '__main__':
    main(args)

    