# ExaWatcher

Watch a SLURM queue and send slack messages when jobs start, finish, or fail.
If you're watching RELION jobs, you'll get some more info.

You'll need a [slack bot](https://slack.com/help/articles/115005265703-Create-a-bot-for-your-workspace) with `chat:write` and `files:write` permissions. Your user ID (for the `DM`
variable) can be found at the bottom of your user profile in the Slack client.

## Submission Scripts
To pass information about the job to ExaWatcher, be sure you're using sbatch.
Add

`#SBATCH --job-name=XXXqueueXXX`

to your submission script. When you're submitting a job, in the "Queue name" field,
add a string to identify the project and job number. By default, ExaWatcher looks
for cryoSPARC format, i.e., `P[project-name]J[job-number]`. You can change this in
`exa_watcher.py`. You'll definitely have to change where ExaWatcher looks for your
files from the default of my username.



## `sacct` format
You must feed a text file of a **custom `sacct` output** with the following format
(exact column widths can differ, obviously, but by default they're too short):

` sacct --format="JobID%25,JobName%27,State%40,ExitCode" > /path/to/wherever/sacct-out.txt`

ExaWatcher reads the `JobId` to determine which are the main jobs and which are
sub-jobs and the `JobName` to determine where to look for RELION files. The `State`
needs to be longer in case a job gets cancelled by someone.

## cron job
I have found that running a cron job every five minutes is more than sufficient.
Make sure that the cron job has a RELION `bin` directory and an imod `bin`
directory in the environment. RELION and imod are required to convert 3DClass
maps into `png` files. You'll also need to have your slack token in your
environment...or as an argument (don't!)

### Example cron job:
watch-queue:
```
#!/bin/bash

PATH=$PATH:/path/to/relion/bin
source /path/to/imod/IMOD/IMOD-linux.sh
sacct --format="JobID%25,JobName%27,State%40,ExitCode" > /path/to/sacct-out.txt
cd /path/to/exawatcher
source venv/bin/activate
python exa_watcher.py '*.json' ./sacct-out.txt --dm CHANNEL_ID --token it-is-not-a-good-idea-to-save-tokens-in-text-files
```

cron job:

`*/5 * * * * /path/to/watch-queue`