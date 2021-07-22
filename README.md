# ExaWatcher

Watch a SLURM queue and send slack messages when jobs start, finish, or fail.

You'll need a slack bot with `chat:write` permissions. Your user ID (for the `DM`
variable) can be found at the bottom of your user profile in the Slack client.

You must feed a text file of a **custom `sacct` output** with the following format
(exact column widths can differ, obviously, but by default they're too short):
> sacct --format="JobID%25,JobName%27,State,ExitCode"