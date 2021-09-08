#!/usr/bin/env python3
import pandas as pd
import json
import glob
import os
import argparse
import sys
import re
import shutil
import subprocess
from datetime import datetime
from urllib.error import HTTPError
from slack import WebClient
from slack.errors import SlackApiError

job_regex = 'P(.*)J([0-9]{3})'
data_location = '/home/exacloud/gscratch/BaconguisLab/posert'

def read_sa(file):
    table = pd.read_table(
        file,
        names = ['id', 'name', 'state', 'code'],
        dtype = {'id': str, 'name': str, 'state': str, 'code': str},
        skiprows = 2,
        sep = '\s{2,}',
        engine = 'python'
    )

    # sub-jobs get launched as part of RELION's processing.
    # 
    # filtering only to ids with length 8 gets us only the
    # primary, named job. If your queue is at a different
    # order of magnitude of jobs, you may need to modify this
    table = table.loc[table['id'].str.len() == 8]

    # convert table to list of tuples
    return list(table.itertuples(index = False, name = None))

def make_projection(map_location):
    # need relion_project and mrc2tif to make pngs from maps
    if not shutil.which('relion_project') or not shutil.which('mrc2tif'):
        raise EnvironmentError

    loc_base = map_location[:-4]
    if 'proj' in map_location:
        return

    # project map to single image (only mrc out available)
    subprocess.run(['relion_project', '--i', map_location, '--o', loc_base+'proj.mrc'])
    # convert mrc to png
    subprocess.run(['mrc2tif', '-p', loc_base+'proj.mrc', loc_base+'.png'])

    return loc_base+'.png'

class RunInfo:
    def __init__(self, location) -> None:
        try:
            self.location = glob.glob(location)[0]
            self.dir = os.path.split(self.location)[0]
            self.job_type = self.location.split('/')[-3]
            self.addendum = f'\nJob type: {self.job_type}'
            self.get_info()
        # If there's no run.out, the location glob line raises an IndexError
        except IndexError:
            self.addendum = f"\nI couldn't find a `run.out` file for this job. Did you set the name correctly?"

    def get_info(self):
        self.files = []
        if self.job_type == 'PostProcess':
            self.table = pd.read_table(
                self.location,
                names = ['stat', 'value'],
                dtype = {'stat': str, 'value': str},
                sep = '\s{2,}',
                engine = 'python'
            )
            # convert last four lines of table to numpy array, take second value of each entry
            results = self.table[-4:].to_numpy()[:,1]
            resolution = results[3]
            map_loc = os.path.split(results[0])[1]
            self.addendum += f'\nFinal resolution: *{resolution}*\nMap at: `{self.dir}/{map_loc}`'

            self.files.append(make_projection(f'{self.dir}/{map_loc}'))
        elif self.job_type == 'Refine3D':
            relevant_lines = []
            with open(self.location, 'r') as f:
                for line in f:
                    if 'Auto-refine: + Final' in line:
                        relevant_lines.append(line.rsplit())
            map_loc = relevant_lines[0].split(' ')[-1]
            resolution = relevant_lines[-1].split(' ')[-1]
            self.addendum += f'\nFinal resolution: *{resolution}*\nMap at: `{self.dir}/{map_loc}`'

            self.files.append(make_projection(f'{self.dir}/{map_loc}'))
        elif self.job_type == 'Extract':
            with open(self.location, 'r') as f:
                for line in f:
                    if "Written out STAR file with" in line:
                        match = re.search('([0-9]{1,}) particles', line)
                        
            self.addendum += f'\nExtracted {match.group(1)} particles.'
        elif self.job_type == 'Class3D':
            maps_to_project = glob.glob(f'{self.dir}/run_it025_class*.mrc')
            for vol in maps_to_project:
                if 'proj' not in vol:
                    self.files.append(make_projection(vol))

    
    def __repr__(self) -> str:
        return f'run.out file at {self.location}'

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
            self.message = f'Hi! Job {self.name} ({self.id}) has changed from {old_state} to {self.state}.'
        else:
            self.message = f'Hi! Job {self.name} ({self.id}) has changed to {self.state}.'
        match = re.search(job_regex, self.name)
        if match and self.state == 'COMPLETED':
            self.info = RunInfo(f'{data_location}/{match.group(1)}/*/job{match.group(2)}/run.out')
            self.message += self.info.addendum

        result = slack_client.chat_postMessage(
            channel = slack_dm,
            text = self.message
        )
        try:
            if self.info.files:
                for filename in self.info.files:
                    slack_client.files_upload(
                        channels = slack_dm,
                        file = filename,
                        thread_ts = result['ts'],
                        filetype = 'png'
                    )
        except AttributeError:
            pass

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
        if new_slurm.state != "PENDING":
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

    
