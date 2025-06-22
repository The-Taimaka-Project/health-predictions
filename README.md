# ETL and Inference Code for Predictive Models of Health Outcomes

This repo is under construction.

Authors:
- Brianna Eales
- Brian Chaplin
- Hunter Merrill

## Description

This codebase contains functionality for extracting, transforming, and loading (ETL) patient data to prepare it as input for downstream predictive models of patient outcomes, as well as running inference on this data using those models. 

## Files and Structure
- `taimaka_health_predictions`: Top-level module.
  - `taimaka_health_predictions/inference`: ETL scripts for preparing inference data and scripts for running inference on the prepared data.
    - `taimaka_health_predictions/inference/run/run.py`: This is the entry point and contains the `main` function that DO will invoke.
    - `taimaka_health_predictions/inference/run/etl.py`: Contains functions that load cleaned data from Postgres and returns weekly dataframes.
    - `taimaka_health_predictions/inference/run/etl_deterioration.py`: Contains functions that take in weekly dataframes and return time series.
    - `taimaka_health_predictions/inference/run/util.py`: Contains utility functions used throughout.
- `.github/workflows/main.yml`: Configuration file that runs unit tests in Github Actions.

## Next Steps

- Build out pipeline
- Connect it to the DO Droplet
- Schedule the pipeline with a cron job.

## Notes

This codebase was originally designed to run in DigitalOcean Functions; however, Functions are limited to 1GB RAM, which is not enough to install the required dependencies (and therefore not enough to actually run any inference, either).