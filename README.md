# ETL Code for Predictive Models of Health Outcomes

Authors:
- Brianna Eales
- Brian Chaplin
- Hunter Merrill

## Description

This codebase contains functionality for extracting, transforming, and loading (ETL) patient data to prepare it as input for downstream predictive models of patient outcomes.

## Structure

Preliminary, but currently my mental model is that it will most likely look like this:
- `briannas_code.R`: Collects raw input data, stores cleaned data (I'll call it "model-raw").
- `brians_code.py`: Takes the model-raw data, processes it further into model-ready data.
- `run_etl.sh`: just a wrapper around the two above scripts, can take care of authentication, temporary data, etc.

The bash script will be the entry point and is what will run on the compute instance. Running this ETL process can store both model-raw and model-ready data in the Postgres database.