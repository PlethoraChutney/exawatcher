#!/usr/bin/env python3
import pandas as pd
import json
import glob
import os
import argparse
import sys
import re
from shutil import which
from socket import gethostname
import subprocess
import logging
from urllib.error import HTTPError
from slack import WebClient
from slack.errors import SlackApiError
from random import choice
import numpy as np
import skimage
import mrcfile
import starfile
import matplotlib.pyplot as plt

# remove annoying pandas error message
pd.options.mode.chained_assignment = None

class Settings(object):
    def __init__(self, settings_dict:dict = {}):
        self.settings = settings_dict

    @property
    def map_process(self):
        if 'projection' not in self.settings:
            return 'projection'
        else:
            return self.settings['projection']

    @map_process.setter
    def map_process(self, projection_setting):
        assert projection_setting in ['projection', 'slice']
        self.settings['projection'] == projection_setting

class Database(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_dir = os.path.split(db_path)[0]

        self.lock_file = os.path.join(self.db_dir, '.dblock')
        try:
            with open(self.db_path, 'r') as f:
                db_json = json.load(f)
                if 'version' not in db_json:
                    projects = [x for x in db_json.keys() if x not in ['slack_key', 'slack_dm']]
                    db_json['projects'] = {x: db_json[x] for x in projects}
                    for name in projects:
                        del db_json[name]
                    db_json['version'] = 1
                self.db = db_json
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
        return self.db['projects'].keys()

    def check_lock(self):
        if os.path.exists(self.lock_file):
            logging.info('Lock file exists. Exiting.')
            sys.exit(1)
        else:
            open(self.lock_file, 'a').close()

    def clear_lock(self):
        if os.path.exists(self.lock_file):
            os.remove(self.lock_file)

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
        self.db['projects'][project_name] = project_dir
        self.commit_change()

    def remove_project(self, project_name):
        try:
            del self.db['projects'][project_name]
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
            'Refine3D': JobRefine3D,
            'MultiBody': JobMultiBody
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

    def process_jobs(self, force = False):
        for job in self.usable_jobs.values():
            if job.status != job.old_status or force:
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
        self.exapath = os.path.join(path, f'.exawatcher{os.path.sep}')
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
                    self.old_status = f.readline().rstrip()
            except FileNotFoundError:
                with open(self.status_path, 'a') as f:
                    f.write('Pending')
                self.old_status = 'Pending'

        self.check_status()
        self.write_status(self.status)

        emoji = {
            'Running': '🏃',
            'Failed': '☠️',
            'User Abort': '🔪',
            'Pending': '⌚'
        }

        greetings = [
            'How are you?',
            'Hope you\'re well.',
            "How's it going?",
            "How's research?",
            "When are you going to graduate? Haha, anyway.",
            "Lookin' good!",
            "Are you drinking water?",
            "Have a snack after this!"
        ]

        self.message = f'Hi! {choice(greetings)}\nJob {self.number} in project {self.project} on {gethostname()} has '
        if self.status == 'Finished':
            self.message += 'finished ✔️.'
        else:
            self.message += f'changed from {self.old_status} to {self.status} {emoji[self.status]}.'

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

    def make_fsc_curve(self):
        fsc_data = starfile.read(os.path.join(
            self.path,
            'run_model.star'
        ))['model_class_1']

        fig = plt.figure()
        plt.axhline(y = 0.143, color = '#AFAFAF', linestyle = '-', zorder = 1)
        plt.plot(fsc_data.rlnResolution, fsc_data.rlnGoldStandardFsc, '-', zorder = 200)
        plt.xlabel('Resolution (A)')
        plt.ylabel('GSFSC')
        positions = fsc_data.rlnResolution[3::10]
        labels = [round(float(x), 1) for x in fsc_data.rlnAngstromResolution[3::10]]
        plt.xticks(positions, labels)
        plt.grid(color = '#EEEEEE')

        outpath = os.path.join(self.exapath, 'fsc.png')
        fig.savefig(outpath)
        self.files.append(outpath)
    
    def make_projection(self, map_filename):
        with mrcfile.open(map_filename) as mrc:
            map = mrc.data

        x_dim = np.sum(map, axis = 0)
        y_dim = np.sum(map, axis = 1)
        z_dim = np.sum(map, axis = 2)

        concat = np.concatenate((x_dim, y_dim, z_dim), axis = 1)
        # set the concatenated projections to all have the same
        # scale, with the darkest pixel 0 and the brightest pixel 1
        concat = concat  - np.min(concat)
        concat = np.divide(concat, np.max(concat))
        concat = skimage.img_as_ubyte(concat)
        outfile = os.path.join(
            self.exapath,
           os.path.split( map_filename)[1][:-4] + '_projected.png'
        )
        skimage.io.imsave(outfile, concat)

        self.files.append(outfile)

    def finished_process(self):
        pass


class JobRefine3D(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        relevant_lines = []
        with open(os.path.join(self.path, 'run.out'), 'r') as f:
            for line in f:
                if 'Final resolution' in line:
                    final_res = re.search('[0-9.]+', line).group(0)
                    self.message += f'\nFinal resolution: *{final_res}*\nMap at: `{self.path}/run_class001.mrc`'
                    break

        self.make_projection(f'{self.path}/run_class001.mrc')
        self.make_fsc_curve()

class JobClass3D(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def make_class_membership_plot(self, iterations):
        classes_over_time = None

        max_it = iterations[-1]

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

        self.message += f'\nMap location: `{self.path}/run_it{max_it}_class*.mrc`'

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
        plt.ylim(0, 1)
        plt.legend(loc = 'upper left')

        outpath = os.path.join(self.exapath, 'classes_over_time.png')
        fig.savefig(outpath)
        self.files.append(outpath)

    def make_particle_stability_plot(self):
        model_stars = glob.glob(os.path.join(self.path, 'run_it*_data.star'))
        model_stars.sort()

        def read_star(new_starfile):
            current_classes = starfile.read(new_starfile)['particles']
            current_classes = current_classes[['rlnImageName', 'rlnClassNumber']]
            current_classes.rename(columns = {'rlnImageName': 'Particle', 'rlnClassNumber': 'new_class'}, inplace = True)
            current_classes.set_index('Particle', inplace = True)

            return current_classes

        current_classes = read_star(model_stars.pop(0))
        current_classes.rename(columns = {'new_class': 'old_class'}, inplace = True)
        current_iter = 0
        iter_movement = {}
        while model_stars:
            star = read_star(model_stars.pop(0))
            current_iter += 1
            current_classes = current_classes.join(star, how = 'outer')
            current_classes = current_classes.assign(
                changed_class = lambda x: x.old_class != x.new_class
            )
            particles_moved = sum(current_classes['changed_class'])
            proportion_moved = particles_moved/current_classes.shape[0]
            iter_movement[current_iter] = proportion_moved

            current_classes.drop(['changed_class', 'old_class'], axis = 1, inplace = True)
            current_classes.rename(columns = {'new_class': 'old_class'}, inplace = True)

        fig = plt.figure()
        plt.plot(list(iter_movement.keys()), list(iter_movement.values()), '-o')

        plt.xlabel('Iteration number')
        plt.ylabel('Proportion of particles changing class')
        plt.ylim(0, 1)

        outpath = os.path.join(self.exapath, 'particle_stability.png')
        fig.savefig(outpath)
        self.files.append(outpath)

    def finished_process(self):
        mrcs = glob.glob(f'{self.path}/run_it*_class*.mrc')
        iterations = [re.search('it([0-9]{3})', x).group(1) for x in mrcs]
        iterations = list(set(iterations))
        iterations.sort()
        max_it = iterations[-1]
        maps_to_project = glob.glob(f'{self.path}/run_it{max_it}_class*.mrc')

        self.make_class_membership_plot(iterations)
        self.make_particle_stability_plot()

        for vol in maps_to_project:
            self.make_projection(vol)

class JobPostProcess(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        with open(os.path.join(self.path, 'run.out'), 'r') as f:
            for line in f:
                if 'FINAL RESOLUTION' in line:
                    final_res = re.search('[0-9.]+', line).group(0)

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
            self.message += f"\nMap location: `{self.path}/run_it{max_it}_class*.mrc`"
            self.make_projection(vol)

class JobCtfRefine(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        self.files.append(os.path.join(self.path, 'logfile.pdf'))

class JobMultiBody(RelionJob):
    def __init__(self, path, project, number, slack_info):
        super().__init__(path, project, number, slack_info)

    def finished_process(self):
        with open(f'{self.path}/run.out', 'r') as f:
            lines = [x.rstrip() for x in f]

        for line in lines:
            if 'Final reconstructions of each body' in line:
                words = line.split(' ')
                map_path = [x for x in words if 'MultiBody' in x][0]
                map_loc = map_path.split('/')[-1].replace('NNN', '???').replace(',', '')

            elif 'Final resolution' in line:
                self.message += f'\nFinal resolution: {line.split(" ")[-1]}'

            elif 'The first' in line and 'explain' in line and 'variance' in line:
                self.message += f'\n{line}'

        mrcs = glob.glob(f'{self.path}/{map_loc}')
        self.message += f"\nMap location: `{self.path}/{map_loc}`"

        for vol in mrcs:
            self.make_projection(vol)

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
        print('Current projects:', *list(db.current_projects), sep = '\n  ')

    if args.test_slack:
        slack_client = create_slack_client(db.slack_key)
        slack_client.chat_postMessage(channel = db.slack_dm, text = 'Slack client successful.')

    if args.clear_lock:
        db.clear_lock()

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
        current_processor = Project(project_name, db.db['projects'].get(project_name), slack_info)
        current_processor.scan_for_jobs()
        if not args.no_process:
            current_processor.process_jobs(force = args.force_process)

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
database.add_argument(
    '--clear-lock',
    help = 'Delete lock file. Do this if you had an error and need to process again.',
    action = 'store_true'
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
    action = 'append',
    type = str
)
process.add_argument(
    '--force-process',
    help = 'Process the given projects even if they already have been. Note that right now this forces reprocessing of all jobs in a project. Might be easier to delete the .exawatcher/last_status.txt file.',
    action = 'store_true'
)
process.add_argument(
    '--no-process',
    help = 'Ignore other process arguments, do not process anything. Useful when adding a large project for the first time.',
    action = 'store_true'
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

    
