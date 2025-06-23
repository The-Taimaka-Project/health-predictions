# ETL and Inference Code for Predictive Models of Health Outcomes

This repo is under construction.

Authors:
- Brianna Eales
- Brian Chaplin
- Hunter Merrill

## Description

This codebase contains functionality for extracting, transforming, and loading (ETL) patient data to prepare it as input for downstream predictive models of patient outcomes, as well as running inference on this data using those models. 

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

## Next Steps

- Build out pipeline
- Connect it to the DO Droplet
- Schedule the pipeline with a cron job.

## Notes

This codebase was originally designed to run in DigitalOcean Functions; however, Functions are limited to 1GB RAM, which is not enough to install the required dependencies (and therefore not enough to actually run any inference, either).