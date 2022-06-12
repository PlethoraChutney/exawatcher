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
import logging
from datetime import datetime
from urllib.error import HTTPError
from slack import WebClient
from slack.errors import SlackApiError

####### Hard-coded values #######
#
# This will get you anything after P and three numbers after J in a string matching P[whatever]J###
job_regex = 'P(.*)J([0-9]{3})'
#
# Change this for each user. Should probably be an argument.
# This is where you put your project directories, i.e., what comes after P in the above
data_location = '/home/exacloud/gscratch/BaconguisLab/posert'

projection_error_message = "\nI couldn't make a projection image. Make sure `relion_project` and `mrc2tif` are in your environment."

class Database(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_dir = os.path.split(db_path)[0]

        self.lock_file = os.path.join(self.db_dir, '.dblock')
        try:
            with open(self.db_path, 'r') as f:
                self.db = json.load(f)
        except FileNotFoundError:
            self.db = {}

    def check_lock(self):
        if os.path.exists(self.lock_file):
            logging.info('Lock file exists. Exiting.')
            sys.exit(0)
        else:
            open(self.lock_file, 'a').close()

    def commit_change(self):
        with open(self.db_path, 'w') as f:
            json.dump(self.db, f)

    def close_db(self):
        self.commit_change()
        os.remove(self.lock_file)

    def update_entry(self, entry, state):
        entry = os.path.expanduser(entry)
        old_state = self.db.get(entry)
        self.db[entry] = state
        self.commit_change()
        return [old_state == state, old_state]

class SlurmJob(object):
    def __init__(self, path, slack_info):
        self.path = path
        self.slack_client = slack_info['client']
        self.slack_dm = slack_info['dm']
        self.message = ''
        self.files = []

    def announce(self):
        result = self.slack_client.chat_postMessage(
            channel = self.slack_dm,
            text = self.message
        )
        for filename in self.info.files:
            self.slack_client.files_upload(
                channels = self.slack_dm,
                file = filename,
                thread_ts = result['ts'],
                filetype = 'png'
            )
    




####### Read slurm and RELION info #######

def read_sa(file):
    # Note that this is hard-coded. You need to follow the sacct command rules
    # from the README
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
    subprocess.run(['relion_project', '--i', map_location, '--o', loc_base+'proj.mrc'],
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL
    )
    # convert mrc to png
    subprocess.run(['mrc2tif', '-p', loc_base+'proj.mrc', loc_base+'.png'],
      stdout=subprocess.DEVNULL
    )

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

            try:
                self.files.append(make_projection(f'{self.dir}/{map_loc}'))
            except EnvironmentError:
                self.addendum += projection_error_message
        
        elif self.job_type == 'Refine3D':
            relevant_lines = []
            with open(self.location, 'r') as f:
                for line in f:
                    if 'Auto-refine: + Final' in line:
                        relevant_lines.append(line.rsplit("\n")[0])
            map_loc = relevant_lines[0].split(' ')[-1].split('/')[-1]
            resolution = relevant_lines[-1].split(' ')[-1]
            self.addendum += f'\nFinal resolution: *{resolution}*\nMap at: `{self.dir}/{map_loc}`'
            try:
                self.files.append(make_projection(f'{self.dir}/{map_loc}'))
            except EnvironmentError:
                self.addendum += projection_error_message
        
        elif self.job_type == 'Extract':
            with open(self.location, 'r') as f:
                for line in f:
                    if "Written out STAR file with" in line:
                        match = re.search('([0-9]{1,}) particles', line)
                        
            self.addendum += f'\nExtracted {match.group(1)} particles.'
        
        elif self.job_type == 'Class3D':
            mrcs = glob.glob(f'{self.dir}/run_it*_class*.mrc')
            iterations = [re.search('it([0-9]{3})', x).group(1) for x in mrcs]
            iterations.sort()
            max_it = iterations[-1]
            maps_to_project = glob.glob(f'{self.dir}/run_it{max_it}_class*.mrc')

            import starfile
            import matplotlib.pyplot as plt
            classes_over_time = None

            for iteration in list(set(iterations)):
                pd.options.mode.chained_assignment = None
                star_files = starfile.read(f'{self.dir}/run_it{iteration}_model.star')
                cm = star_files['model_classes']
                cm = cm[['rlnReferenceImage', 'rlnClassDistribution']]
                cm['rlnReferenceImage'] = cm.rlnReferenceImage.apply(lambda x: re.search('class[0-9]{3}', x).group(0))
                cm.rename(columns = {'rlnReferenceImage': 'Class','rlnClassDistribution': iteration}, inplace = True)
                cm = cm.set_index('Class')

                if classes_over_time is None:
                    classes_over_time = cm
                else:
                    classes_over_time = classes_over_time.join(cm)

            self.addendum += f'\nMap location: `{self.dir}/run_it025_class*.mrc`'

            class_memb_table = classes_over_time[iterations[-1]]
            self.addendum += f'\nClass Membership (fraction of particles)\n```{str(class_memb_table)}```'

            # sort columns then transpose so that each column is a class
            classes_over_time = classes_over_time.reindex(sorted(classes_over_time.columns), axis = 1)
            classes_over_time = classes_over_time.transpose()
            iteration_nums = [int(x) for x in list(classes_over_time.index)]


            fig = plt.figure()
            for rln_class in classes_over_time.columns:
                plt.plot(iteration_nums, classes_over_time[rln_class], '-o')

            plt.xlabel('Iteration number')
            plt.ylabel('Percent particle membership')

            fig.savefig('classes_over_time.png')
            self.files.append('classes_over_time.png')

            for vol in maps_to_project:
                if 'proj' not in vol:
                    try:
                        self.files.append(make_projection(vol))
                    except EnvironmentError:
                        self.addendum += projection_error_message
                        break
        
        elif self.job_type == 'InitialModel':
            maps_to_project = glob.glob(f'{self.dir}/run_it300_class*.mrc')
            for vol in maps_to_project:
                if 'proj' not in vol:
                    self.addendum += f"\nMap location: `{self.dir}/run_it300_class*.mrc`"
                    try:
                        self.files.append(make_projection(vol))
                    except EnvironmentError:
                        self.addendum += projection_error_message
                        break
        
        elif self.job_type == 'CtfRefine':
            self.files.append(f'{self.dir}/logfile.pdf')

    
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

def manual_process(job_name, client, dm):
    job = SlurmJob(['Manual Process', job_name, 'COMPLETED', '0:0'])
    job.announce(client, dm, 'MANUAL')

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
    db = Database(args.db)
    if args.new_job:
        db.update_entry(args.new_job, 'Pending')

    if not (args.process_all or args.process_job):
        sys.exit(0)

    # check lock after adding new jobs so that we can add jobs
    # during processing. Since the DB is read only at startup, this
    # shouldn't present any issues, and lets us include jobs in RELION
    # launch scripts.
    db.check_lock()

    if args.process_all:
        print('Processing all')
        jobs_to_process = list(db.db.keys())
    else:
        jobs_to_process = args.process_job

    print(jobs_to_process)

    db.close_db()

parser = argparse.ArgumentParser(
    description='Check for changes in slurm jobs. Requires custom sacct output (see README).'
)
parser.add_argument(
    '--new-job',
    help = "Add a new directory to exawatcher's database. If this dir already exists its state will be reset.",
    type = str
)
parser.add_argument(
    '--process-all',
    help = 'Run exawatchers processor on all jobs in database',
    action = 'store_true'
)
parser.add_argument(
    '--process-job',
    help = 'Process job at the specified directory. Can be given multiple times.',
    nargs = 1,
    action = 'append',
    type = str
)
parser.add_argument(
    '--db',
    help = 'Alternate database location. Default is ~/exawatcher.db',
    default = os.path.join(os.path.expanduser('~'), 'exawatcher.db')
)

args = parser.parse_args()

if __name__ == '__main__':
    main(args)

    
