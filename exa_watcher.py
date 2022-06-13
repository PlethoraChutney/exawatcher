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


# remove annoying pandas error message
pd.options.mode.chained_assignment = None

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

    @property
    def slack_key(self):
        return self.db.get('slack_key')
    
    @slack_key.setter
    def slack_key(self, new_key):
        self.db['slack_key'] = new_key
        self.commit_change()

    @property
    def slack_dm(self):
        return self.db.get('slack_dm')
    
    @slack_dm.setter
    def slack_dm(self, new_dm_id):
        self.db['slack_dm'] = new_dm_id
        self.commit_change()

    @property
    def current_projects(self):
        return [x for x in self.db.keys() if x not in ['slack_key', 'slack_dm']]

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

    def new_project(self, project_dir):
        project_dir = os.path.expanduser(project_dir)
        project_dir = os.path.abspath(os.path.normpath(project_dir))
        if not os.path.exists(project_dir):
            logging.error('Give a path to a RELION project.')
            sys.exit(1)
        project_name = os.path.split(project_dir)[1]
        self.db[project_name] = project_dir
        self.commit_change()

    def remove_project(self, project_name):
        try:
            del self.db[project_name]
        except KeyError:
            logging.error(f'Could not find {project_name} in database.')


class Project(object):
    def __init__(self, project_name, project_dir, slack_info):
        self.project_name = project_name
        self.project_dir = project_dir
        self.slack_info = slack_info

        self.available_job_types = {
            'Class3D': JobClass3D,
            'CtfRefine': JobCtfRefine,
            'Extract': JobExtract,
            'InitialModel': JobInitialModel,
            'PostProcess': JobPostProcess,
            'Refine3D': JobRefine3D
        }

    def __repr__(self):
        return f'Project {self.project_name}'

    def scan_for_jobs(self):
        self.usable_jobs = {}
        all_jobs = glob.glob(os.path.join(self.project_dir, '*', 'job*'))
        for job in all_jobs:
            job_num = re.search('job([0-9]{3})', job).group(1)

            job_type = [x for x in self.available_job_types.keys() if x in job]
            try:
                job_type = job_type[0]
            except IndexError:
                # not a job type we can process yet
                job_type = False

            if job_type:
                self.usable_jobs[job_num] = self.available_job_types[job_type](
                    job,
                    self.project_name,
                    job_num,
                    self.slack_info
                )

    def process_jobs(self):
        for job in self.usable_jobs.values():
            if job.status != job.old_status:
                if job.status == 'Finished':
                    job.finished_process()
                
                job.announce()


class RelionJob(object):
    def __init__(self, path, project, number, slack_info):
        self.path = path
        self.project = project
        self.number = number
        # exapath is where we'll store all the crap for exawatcher
        # like current job status and any files/images we make
        self.exapath = os.path.join(path, '.exawatcher')
        self.status_path = os.path.join(self.exapath, 'last_status.txt')
        self.slack_client = slack_info['client']
        self.slack_dm = slack_info['dm']
        self.files = []

        if not os.path.exists(self.exapath):
            os.makedirs(self.exapath)
            with open(self.status_path, 'a') as f:
                f.write('Pending')
            self.old_status = 'Pending'

        else:
            try:
                with open(self.status_path, 'r') as f:
                    self.old_status = f.readline().strip()
            except FileNotFoundError:
                with open(self.status_path, 'a') as f:
                    f.write('Pending')
                self.old_status = 'Pending'

        self.check_status()
        self.write_status(self.status)

        self.message = f'Job {self.number} in project {self.project} has '
        if self.status == 'Finished':
            self.message += 'finished.'
        else:
            self.message += f'changed from {self.old_status} to {self.status}.'

    def check_status(self):
        status = 'Pending'
        # RELION writes a series of files during a job's lifetime. I've decided
        # their heirarchy somewhat manually here.
        if os.path.exists(os.path.join(self.path, 'run.out')):
            status = 'Running'
        if os.path.exists(os.path.join(self.path, 'RELION_JOB_EXIT_FAILURE')):
            status = 'Failed'
        if os.path.exists(os.path.join(self.path, 'RELION_JOB_EXIT_ABORTED')):
            status = 'User Abort'
        if os.path.exists(os.path.join(self.path, 'RELION_JOB_EXIT_SUCCESS')):
            status = 'Finished'

        self.status = status

    def write_status(self, new_status):
        with open(self.status_path, 'w') as f:
            f.write(new_status)

    def announce(self):
        result = self.slack_client.chat_postMessage(
            channel = self.slack_dm,
            text = self.message
        )
        for filename in self.files:
            self.slack_client.files_upload(
                channels = self.slack_dm,
                file = filename,
                thread_ts = result['ts'],
                filetype = 'png'
            )
    
    def make_projection(self, map_filename):
        # need relion_project (pro-JECT, not PRO-ject haha) and mrc2tif to make pngs from maps
        if not shutil.which('relion_project') or not shutil.which('mrc2tif'):
            self.message += "\nI couldn't make a projection image. Make sure `relion_project` and `mrc2tif` are in your environment."
            return

        map_base = map_filename[:-4]

        # project map to single image (only mrc out available)
        subprocess.run(
            [
                'relion_project',
                '--i',
                os.path.join(self.path, map_filename),
                '--o',
                os.path.join(self.exapath, map_base+'proj.mrc')
            ],
            stdout = subprocess.DEVNULL,
            stderr = subprocess.DEVNULL
        )
        # convert mrc to png
        subprocess.run(
            [
                'mrc2tif',
                '-p',
                os.path.join(self.exapath, map_base+'proj.mrc'),
                os.path.join(self.exapath, map_base+'.png')
            ],
            stdout=subprocess.DEVNULL
        )

        self.files.append(os.path.join(self.exapath, map_base+'.png'))

    def finished_process(self):
        pass


class JobRefine3D(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        relevant_lines = []
        with open(os.path.join(self.path, 'run.out'), 'r') as f:
            for line in f:
                final_res = re.match('Auto-refine: + Final resolution (without masking) is: ([0-9.]+)', line)

                if final_res:
                    final_res = final_res.group(1)
                    self.message += f'\nFinal resolution: *{final_res}*\nMap at: `{self.path}/run_class001.mrc`'
                    break

        self.make_projection(f'{self.path}/run_class001.mrc')

class JobClass3D(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        mrcs = glob.glob(f'{self.path}/run_it*_class*.mrc')
        iterations = [re.search('it([0-9]{3})', x).group(1) for x in mrcs]
        iterations = list(set(iterations))
        iterations.sort()
        max_it = iterations[-1]
        maps_to_project = glob.glob(f'{self.path}/run_it{max_it}_class*.mrc')

        import starfile
        import matplotlib.pyplot as plt
        classes_over_time = None

        for iteration in iterations:
            star_files = starfile.read(f'{self.path}/run_it{iteration}_model.star')
            cm = star_files['model_classes']
            cm = cm[['rlnReferenceImage', 'rlnClassDistribution']]

            # get the class number and fraction of particles for this iteration
            cm['rlnReferenceImage'] = cm.rlnReferenceImage.apply(lambda x: re.search('class[0-9]{3}', x).group(0))
            cm.rename(columns = {'rlnReferenceImage': 'Class','rlnClassDistribution': iteration}, inplace = True)
            cm = cm.set_index('Class')

            if classes_over_time is None:
                classes_over_time = cm
            else:
                classes_over_time = classes_over_time.join(cm)

        self.message += f'\nMap location: `{self.path}/run_it025_class*.mrc`'

        class_memb_table = classes_over_time[iterations[-1]]
        self.message += f'\nClass Membership (fraction of particles)\n```{str(class_memb_table)}```'

        # sort columns then transpose so that each column is a class
        classes_over_time = classes_over_time.reindex(sorted(classes_over_time.columns), axis = 1)
        classes_over_time = classes_over_time.transpose()
        iteration_nums = [int(x) for x in list(classes_over_time.index)]


        fig = plt.figure()
        for rln_class in classes_over_time.columns:
            plt.plot(iteration_nums, classes_over_time[rln_class], '-o', label = f'Class {rln_class}')

        plt.xlabel('Iteration number')
        plt.ylabel('Percent particle membership')

        outpath = os.path.join(self.exapath, 'classes_over_time.png')
        fig.savefig(outpath)
        self.files.append(outpath)

        for vol in maps_to_project:
            self.make_projection(vol)

class JobPostProcess(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        with open(os.path.join(self.path, 'run.out'), 'r') as f:
            for line in f:
                if 'FINAL RESOLUTION' in line:
                    final_res = re.search('[0-9.]+').group(0)

        self.message += f'\nFinal resolution: *{final_res}*\nMap at: `{self.path}/postprocess.mrc`'

        self.make_projection(os.path.join(self.path, 'postprocess.mrc'))

class JobExtract(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        with open(self.location, 'r') as f:
            for line in f:
                if "Written out STAR file with" in line:
                    match = re.search('[0-9]+ particles', line).group(0)
                    
        self.message += f'\nExtracted {match}.'

class JobInitialModel(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        mrcs = glob.glob(f'{self.path}/run_it*_class*.mrc')
        iterations = [re.search('it([0-9]{3})', x).group(1) for x in mrcs]
        iterations = list(set(iterations))
        iterations.sort()
        max_it = iterations[-1]
        maps_to_project = glob.glob(f'{self.path}/run_it{max_it}_class*.mrc')
        for vol in maps_to_project:
            self.message += f"\nMap location: `{self.path}/run_it300_class*.mrc`"
            self.make_projection(vol)

class JobCtfRefine(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        self.files.append(os.path.join(self.path, 'logfile.pdf'))

def create_slack_client(slack_key) -> WebClient:
    slack_web_client = WebClient(token=slack_key)

    try:
        slack_web_client.auth_test()
    except SlackApiError:
        logging.error('Slack client creation failed. Check your token')
        sys.exit(2)

    return slack_web_client

def main(args) :
    # database work can be done even while a lock file exists.
    db = Database(args.db)
    if args.new_project:
        db.new_project(args.new_project)

    if args.slack_key:
        db.slack_key = args.slack_key
    if args.slack_dm_id:
        db.slack_dm = args.slack_dm_id

    if args.remove_project:
        db.remove_project(args.remove_project)

    if args.list_projects:
        print('Current projects:', *db.current_projects, sep = '\n  ')

    if args.test_slack:
        slack_client = create_slack_client(db.slack_key)
        slack_client.chat_postMessage(channel = db.slack_dm, text = 'Slack client successful.')

    if not (args.process_all or args.process_project):
        sys.exit(0)

    # we should only process if another instance of exa_watcher is
    # not currently processing
    db.check_lock()

    if args.process_all:
        process_targets = db.current_projects
    else:
        process_targets = args.process_project

    slack_info = {
        'client': create_slack_client(db.slack_key),
        'dm': db.slack_dm
    }

    for project_name in process_targets:
        current_processor = Project(project_name, db.db.get(project_name), slack_info)
        current_processor.scan_for_jobs()
        current_processor.process_jobs()


    db.close_db()

parser = argparse.ArgumentParser(
    description='Check for changes in slurm jobs. Requires custom sacct output (see README).'
)
database = parser.add_argument_group('database')
database.add_argument(
    '--db',
    help = 'Alternate database location. Default is ~/exawatcher.db',
    default = os.path.join(os.path.expanduser('~'), 'exawatcher.db')
)
database.add_argument(
    '--new-project',
    help = "Add a new directory to exawatcher's database. If this dir already exists nothing will change.",
    type = str
)
database.add_argument(
    '--list-projects',
    help = 'List currently tracked projects',
    action = 'store_true'
)
database.add_argument(
    '--remove-project',
    help = "Stop tracking project. Use project name, not full path. Does not delete data."
)
database.add_argument(
    '--slack-key',
    help = 'Update or add Slack key to database. Must run at least once before first time processing.'
)
database.add_argument(
    '--slack-dm-id',
    help = 'Update or add Slack DM id. Must run at least once before first time processing.'
)

process = parser.add_argument_group('process')
process.add_argument(
    '--process-all',
    help = "Run exawatcher's processor on all jobs in database",
    action = 'store_true'
)
process.add_argument(
    '--process-project',
    help = 'Process specified project name (not path). Can be given multiple times.',
    nargs = 1,
    action = 'append',
    type = str
)

verbosity = parser.add_argument_group('verbosity')
vxg = verbosity.add_mutually_exclusive_group()
vxg.add_argument(
    '-q', '--quiet',
    help = 'Print Errors only',
    action = 'store_const',
    dest = 'verbosity',
    const = 'q'
)
vxg.add_argument(
    '-v', '--verbose',
    help = 'Print Info, Warnings, and Errors. Default state.',
    action = 'store_const',
    dest = 'verbosity',
    const = 'v'
)
vxg.add_argument(
    '--debug',
    help = 'Print debug output.',
    action = 'store_const',
    dest = 'verbosity',
    const = 'd'
)

debug = parser.add_argument_group('debug')
debug.add_argument(
    '--test-slack',
    help = 'Send a test slack message using DB info.',
    action = 'store_true'
)

if __name__ == '__main__':
    args = parser.parse_args()

    levels = {
        'q': logging.ERROR, 
        'v': logging.INFO,
        'd': logging.DEBUG
    }
    try:
        level = levels[args.verbosity]
    except KeyError:
        level = logging.INFO

    logging.basicConfig(
        level = level,
        format = '{levelname}: {message} ({filename})',
        style = '{'
    )

    main(args)

    
