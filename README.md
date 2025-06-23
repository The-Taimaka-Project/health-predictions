# ETL and Inference Code for Predictive Models of Health Outcomes

This repo is under construction.

Authors:
- Brianna Eales
- Brian Chaplin
- Hunter Merrill

## Description

This codebase contains functionality for extracting, transforming, and loading (ETL) patient data to prepare it as input for downstream predictive models of patient outcomes, as well as running inference on this data using those models.

## How to run

### First time setup

This code runs on a DigitalOcean server on a schedule via a cron job. This requires a one-time setup to access the server, and then you must manually update the code on the server each time changes are made to this source code. If you have never used SSH to access the server, first [generate SSH keys](https://www.digitalocean.com/community/tutorials/how-to-configure-ssh-key-based-authentication-on-a-linux-server#step-1-creating-ssh-keys) on your machine if you have not already, then run the following (replacing `username` with your user name) to copy your public SSH key to the DO Taimaka server:

```bash
ssh-copy-id username@taimaka-internal.org
```

You will be prompted for your password. Once complete, you can then enter the server using just your SSH credentials via 

```bash
ssh username@taimaka-internal.org
```

### Updating the source code on the server

This repo is cloned to the server in the directory `/srv/projects/health-predictions`. Any time this source code is updated, simply SSH into the server, navigate to that directory, pull updates and install the pipeline:

```bash
cd /srv/projects/health-predictions  # navigate to the directory
git checkout main                    # ensure you're on the main branch
git pull origin main                 # pull in latest updates

# create a virtual environment if it doesn't exist already.
[ -d ".venv" ] && echo "env exists." || python3.12 -m venv .venv

. .venv/bin/activate                 # activate the virtual environment
pip install .                        # install the package and CLI tool
```

Now a command line tool called `infer` is available, and running it will run the full ETL+inference pipeline.

### Scheduling the Cron job

[Cron](https://help.ubuntu.com/community/CronHowto) is a system used to run tasks on designated schedules. Each user on the server can schedule jobs. Currently, the inference jobs are scheduled under user `hmerrill`; if the schedule needs to be moved to another user, contact Justin Graham and/or Hunter Merrill.

Hunter set the schedule by running `crontab -e` to open the scheduling file, and then pasting the following in the editor:

```bash
0 0 * * * source /srv/projects/.do_space_creds && echo "testing"
```

When the inference pipeline is ready, `echo "testing"` will be replaced by `. .venv/bin/activate && infer`.

## Next Steps

- Build out pipeline
- Schedule the pipeline with a cron job.

## Files and Structure

```
├── README.md
├── requirements.txt                                    # Dependencies.
├── taimaka_health_predictions                          # Top-level module.
│   ├── __init__.py
│   ├── inference                                       # Inference module.
│   │   ├── __init__.py
│   │   ├── __main__.py                                 # Runs the ETL+inference pipeline.
│   │   ├── etl_deterioration.py                        # Make ETL data model-ready.
│   │   ├── etl.py                                      # Load cleaned data from Postgres.
│   │   ├── infer.py                                    # Conducts inference on model-ready data.
│   │   └── util.py                                     # Utilities specific to inference.
│   ├── train                                           # Train module.
│   │   ├── __init__.py
│   │   └── train_new_onset_medical_complication.ipynb  # Training notebook.
│   └── utils                                           # Utilities shared by training & inference.
│       ├── __init__.py
│       ├── digitalocean.py                             # IO methods for DO Spaces.
│       └── globals.py                                  # Global 
├── tests                                               # Unit tests.
│   └── test_util.py
├── to_archive                                          # Legacy code, for reference.
│   ├── data-update.R
│   ├── etl_deterioration.ipynb
│   ├── etl.ipynb
│   └── infer.ipynb
└── .github
    └── workflows
        └── main.yml                                    # Schedules unit tests in Github Actions.
```

## Notes

This codebase was originally designed to run in DigitalOcean Functions; however, Functions are limited to 1GB RAM, which is not enough to install the required dependencies (and therefore not enough to actually run any inference, either).