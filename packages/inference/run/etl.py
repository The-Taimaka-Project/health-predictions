"""
This script will contain functions that load cleaned data from the Postgres database
and process it into weekly data.
"""

from globals import logger, DO_DIRECTORY
from util import convert_to_bool, find_collinear_columns, infer_phq_score, make_categorical, regress
# set FAIL_MODE to True if exceptions should be raised
FAIL_MODE = True
# set TRAIN_MODE to True if rows should be dropped if weekly cadence skips more than 4 weeks (1% of weekly patients)
TRAIN_MODE = False

import os
import pickle
import re
from warnings import simplefilter

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from tqdm import tqdm

import logging
# Create a logger# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG) # Set the minimum logging level

# Create a handler to output logs to the console
console_handler = logging.StreamHandler()

file_handler = logging.FileHandler('my_log.log')
file_handler.setLevel(logging.INFO) # Set the logging level for the handler


# Create a formatter to specify the log message format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(funcName)s - %(message)s')

# Add the formatter to the handler

file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)


# TODO: replace Google Drive with Postgres database

from google.colab import drive

drive.mount("/content/drive")

dir = "/content/drive/My Drive/[PBA] Full datasets/"

current = pd.read_csv(dir + "FULL_pba_current_processed_2024-11-15.csv")
admit = pd.read_csv(dir + "FULL_pba_admit_processed_2024-11-15.csv")
weekly = pd.read_csv(dir + "FULL_pba_weekly_processed_2024-11-15.csv")
raw = pd.read_csv(dir + "FULL_pba_admit_raw_2024-11-15.csv")
weekly_raw = pd.read_csv(dir + "FULL_pba_weekly_raw_2024-11-15.csv")
itp = pd.read_csv(dir + "FULL_pba_itp_roster_2024-11-15.csv")
relapse = pd.read_csv(dir + "FULL_pba_relapse_raw2024-11-15.csv")
mh = pd.read_csv(dir + "FULL_pba_mh_raw2024-11-15.csv")



logger.debug(f"weekly_raw shape: {weekly_raw.shape}")
logger.debug(f"weekly_raw null sums for pid, todate, end_time:\n{weekly_raw[['pid', 'todate', 'end_time']].isnull().sum()}")
logger.debug(f"weekly_raw notnull sums for pid, todate, end_time:\n{weekly_raw[['pid', 'todate', 'end_time']].notnull().sum()}")
logger.debug(f"current shape: {current.shape}")
logger.debug(f"admit shape: {admit.shape}")
logger.debug(f"weekly shape: {weekly.shape}")
logger.debug(f"raw shape: {raw.shape}")
logger.debug(f"weekly shape: {weekly_raw.shape}")
logger.debug(f"itp shape: {itp.shape}")
logger.debug(f"relapse shape: {relapse.shape}")
logger.debug(f"mh shape: {mh.shape}")

logger.info(weekly_raw[['pid', 'todate', 'end_time']].isnull().sum())
if weekly_raw[["pid", "todate", "end_time"]].isnull().sum().sum() > 0 & FAIL_MODE:
    raise RuntimeError(
        "primary must be populated:  null values found in weekly_raw[['pid', 'todate', 'end_time']]."
    )

# Check for duplicate pids in the 'admit' DataFrame
duplicate_pids_admit = admit[admit.duplicated(subset=["pid"], keep=False)]
logger.info(f"Duplicate pids found in 'admit' DataFrame: {duplicate_pids_admit['pid'].unique()}")

# prompt: if not duplicate_pids_admit.empty throw a runtime exception
if (not duplicate_pids_admit.empty) & FAIL_MODE:
    raise RuntimeError("Duplicate pids found in 'admit' DataFrame.")


# Identify columns with unique values [True, nan, False] and print null count
def find_3val_bool(df):
    for col in df.columns:
        if len(df[col].unique()) == 3:
            unique_vals = df[col].unique()
            if all(val in [True, False] or pd.isna(val) for val in unique_vals):
                null_ct = df[col].isnull().sum()
                size = df[col].size
                sum = df[col].sum()
                if null_ct > 0:
                    logger.info(f"Found 3-val bool column '{col}' with null count: {null_ct} {null_ct/size*100:.1f}% sum:{sum}")
                else:
                    logger.info(f"Found 3-val bool column '{col}' with null count: {df[col].isnull().sum()} sum:{sum}")

# prompt: convert detn columns with unique values [True nan False] to boolean


# Identify columns with unique values [True, nan, False] and convert them to boolean
def convert_3val_bool(df, threshold):
    for col in df.columns:
        if len(df[col].unique()) == 3:
            unique_vals = df[col].unique()
            if all(val in [True, False] or pd.isna(val) for val in unique_vals):
                null_ct = df[col].isnull().sum()
                if null_ct < threshold:
                    logger.info(f"Converting 3-val bool column '{col}' with null count: {null_ct}")
                    df[col] = df[col].fillna(False).astype(bool)


def convert_to_datetime(df):
    df_copy = df.copy()
    date_time_columns = []
    for col in df_copy.columns:
        if re.search(r"(date|time)", col, re.IGNORECASE):
            date_time_columns.append(col)
    for col in date_time_columns:
        df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
        df_copy[col].fillna(pd.NaT, inplace=True)
        df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce").dt.tz_localize(None)
        df_copy[col].fillna(pd.NaT, inplace=True)
        logger.debug(f'{col} {df_copy[col].dtypes}')
    return df_copy


# prompt: rename admit finalhl column to hl

# Rename the 'admit_finalhl' column to 'hl' in the 'admit' DataFrame
admit.rename(columns={"finalhl": "hl"}, inplace=True)
admit.drop(columns={"roundedhl"}, inplace=True)

admit["wfh"] = admit["weight"] / admit["hl"]
admit["hfa"] = admit["hl"] / admit["age"]
admit["wfa"] = admit["weight"] / admit["age"]

weekly["wfh"] = weekly["weight"] / weekly["finalhl"]
weekly["hfa"] = weekly["finalhl"] / weekly["age"]
weekly["wfa"] = weekly["weight"] / weekly["age"]

current = convert_to_datetime(current)
admit = convert_to_datetime(admit)
weekly = convert_to_datetime(weekly)
raw = convert_to_datetime(raw)
weekly_raw = convert_to_datetime(weekly_raw)
itp = convert_to_datetime(itp)
relapse = convert_to_datetime(relapse)
mh = convert_to_datetime(mh)


# prompt: sort itp by muac,age,initial_dx and deduplicate on pid,admit_date,outcome_date

# Sort by muac, age, initial_dx and then deduplicate on pid, admit_date, outcome_date so the record selection is determinative
itp_sorted = itp.sort_values(["muac", "age", "initial_dx"])
itp_deduped = itp_sorted.drop_duplicates(subset=["pid", "admit_date", "outcome_date"], keep="first")

logger.info(itp_sorted.shape)
logger.info(itp_deduped.shape)


if (itp_sorted.shape[0] != itp_deduped.shape[0]) & FAIL_MODE:
    raise RuntimeError(
        f"itp has {itp_sorted.shape[0] - itp_deduped.shape[0]} duplicate rows for ['pid', 'admit_date', 'outcome_date']."
    )

itp_deduped["admit_date"] = pd.to_datetime(itp_deduped["admit_date"])
itp_deduped["outcome_date"] = pd.to_datetime(itp_deduped["outcome_date"], format="mixed")
# 434 itp.los_days rows are null so populate them and fix the 2 wrong ones
itp_deduped["los_days"] = (itp_deduped["outcome_date"] - itp_deduped["admit_date"]).dt.days

# sort the most recent itp_roster row first so when flattened, occurrence 1 will be row 1
itp_deduped = itp_deduped.sort_values(["pid", "admit_date", "outcome_date"], ascending=False)

# prompt: for itp_deduped['outcome'] == 'Death' are there duplicate pid?

# Filter for rows where outcome is 'Death'
death_cases = itp_deduped[itp_deduped["outcome"] == "Death"]

# Check for duplicate pids in the filtered dataset
duplicate_pids = death_cases[death_cases.duplicated(subset=["pid"], keep=False)]

# Print the duplicate pids if any are found
if not duplicate_pids.empty:
    logger.error("Duplicate pids found for outcome 'Death':")
    logger.error(duplicate_pids["pid"].unique())
    if FAIL_MODE:
        raise RuntimeError("Duplicate itp pids found for outcome 'Death'.")
else:
    logger.info("No duplicate pids found for outcome 'Death'.")


# prompt: deduplicate death_cases on pid using the highest admit_date, just to be safe

# Group by 'pid' and select the row with the highest 'admit_date' for each group
death_cases = death_cases.loc[death_cases.groupby("pid")["admit_date"].idxmax()]

# Now, 'death_cases' contains only unique 'pid's with the highest 'admit_date'

# prompt: group itp by pid and create a dataframe with row_count,min(weight),max(weight),average(weight),min(muac),max(muac),average(muac) per pid

itp_agg = itp_deduped.groupby("pid").agg(
    itp_row_count=("pid", "count"),
    itp_first_admit=("admit_date", "last"),
    itp_last_admit=("admit_date", "first"),
    itp_avg_los_days=("los_days", "mean"),
    itp_min_los_days=("los_days", "min"),
    itp_max_los_days=("los_days", "max"),
    itp_last_muac=("muac", "first"),
    itp_first_muac=("muac", "last"),
    itp_min_muac=("muac", "min"),
    itp_max_muac=("muac", "max"),
    itp_avg_muac=("muac", "mean"),
    itp_avg_age=("age", "mean"),
)

itp_agg.reset_index(inplace=True)

# prompt: select rows in each group itp_deduped.groupby('pid')['pid'] where cumcount() < 3


# Group by 'pid' and select the first 3 rows within each group
selected_rows = itp_deduped.groupby("pid").apply(lambda x: x.head(3))

selected_rows.drop("pid", inplace=True, axis=1)
selected_rows.reset_index(inplace=True)

itp_series = (
    selected_rows.assign(col=selected_rows.groupby("pid").cumcount() + 1)
    .set_index(["pid", "col"])
    .unstack("col")
    .sort_index(level=(1, 0), axis=1)
)
itp_series.columns = [f"itp{y}_{x}" for x, y in itp_series.columns]
# prompt: make itp_series.index a column named 'pid'


itp_series = itp_series.reset_index()

# prompt: drop status column from raw
# as it's all nulls and gets confused with current.status

logger.debug(f"raw shape: {raw.shape}")
raw = raw.drop("status", axis=1)
raw.drop("glbsite", axis=1, inplace=True)
logger.debug(f"raw shape: {raw.shape}")



for col in [col for col in raw.columns if "threshold" in col]:
    raw[col] = pd.to_numeric(raw[col], errors="coerce")

# prompt: find raw['id'] that contain 'uuid:'

uuids_in_raw = raw[raw["id"].str.contains("uuid:", na=False)]

# prompt: throw runtime if uuids_in_raw not empty
if (not uuids_in_raw.empty) & FAIL_MODE:
    raise RuntimeError("raw['id'] contains the string 'uuid'.")

# prompt: rename raw['id'] to raw['uuid']

raw = raw.rename(columns={"id": "uuid"})

# drop columns in raw that are the same name (and value) in admit
# their values are the same so no need for confusion and the need to disambiguate between admit and raw
# simplifies and clarifies

cols_to_drop = [
    "site",
    "site_type",
    "staffmember",
    "receiving_otp",
    "state",
    "other_state",
    "gombe_lga",
    "other_lga",
    "settlement",
    "b_prevenr",
    "prev_pid",
    "b_knowsbday",
    "birthdate",
    "weight",
    "muac",
    "c_oedema",
    "b_twinalive",
    "b_cgishoh",
    "cg_sex",
    "cg_age",
    "c_vcardloc",
    "manual_nvdate",
    "cleaning_note",
    "b_has_phone_number",
]

raw = raw.drop([col for col in cols_to_drop if col in raw.columns], axis=1)

logger.debug(f"raw shape: {raw.shape}")

# prompt: find duplicated pid in admit_raw
# Find duplicate pids in admit_raw
duplicated_pids_raw = raw[raw.duplicated(subset=["pid"], keep=False)]["pid"].unique()
logger.debug(f"duplicated_pids_raw: {duplicated_pids_raw.size} {duplicated_pids_raw}")
if (duplicated_pids_raw.size > 0) & FAIL_MODE:
    raise RuntimeError(f"{duplicated_pids_raw.size} duplicate pids found in admit_raw.")

# prompt: deduplicate raw on pid,todate using the first row
# first row of group is latest start_time, most recent for a todate
raw = raw.sort_values(["pid", "todate", "start_time"], ascending=[True, False, False])
# diarrhea null rate is 0.091659 for todate descending and ascending so use descending to get the most recent
logger.debug(f"raw shape: {raw.shape}")
# Group by pid and, then take the first row (latest todate, start_time) within each group
raw = raw.groupby(["pid"], as_index=False).first()
logger.debug(f"raw shape: {raw.shape}")

# prompt: raw['emergency_admission']=1 if pid in duplicated_pids_raw

raw["emergency_admission"] = raw["pid"].isin(duplicated_pids_raw).astype(int)

# prompt: find duplicate raw['id']

duplicate_raw_ids = raw[raw.duplicated(subset=["uuid"], keep=False)]

# prompt: if duplicate_raw_ids not empty raise runtime exception
logger.debug(f"duplicated_pids_raw: {duplicate_raw_ids.size} {duplicate_raw_ids}")
if (not duplicate_raw_ids.empty) & FAIL_MODE:
    raise RuntimeError(f"{duplicate_raw_ids.size} duplicate raw['id'] found for a pid.")


# prompt: are there any columns in raw that have only 1 nunique()?
# if so, drop them as they add no value, eliminates 80 columns

cols_with_one_nunique = [col for col in raw.columns if raw[col].nunique() == 1]
logger.debug(f"Number of columns with one unique value: {len(cols_with_one_nunique)}")
logger.debug(f"Columns with one unique value: {cols_with_one_nunique}")

# then drop them
logger.debug(f"raw shape: {raw.shape}")
raw.drop(cols_with_one_nunique, axis=1, inplace=True)
logger.debug(f"raw shape: {raw.shape}")

# Replace 'resp_rate' values greater than 300 with 'resp_rate_2' values
weekly_raw.loc[weekly_raw["resp_rate"] > 300, "resp_rate"] = weekly_raw["resp_rate_2"]
weekly_raw.drop(columns=["resp_rate_2"], inplace=True)
weekly_raw.drop(columns=["resp_rate_3"], inplace=True)

# drop columns in weekly_raw that are the same in weekly
weekly_raw = weekly_raw.drop(
    ["site_type", "manual_nvdate", "b_added_phone_number", "b_excluded"], axis=1
)

# only 3 muac rows values differ and only 2 weight row values differ between weekly_raw and weekly so they can be dropped too
weekly_raw = weekly_raw.drop(["muac", "weight"], axis=1)
logger.debug(f"weekly_raw shape: {weekly_raw.shape}")

# prompt: find the 304 columns in weekly_raw that are all nulls and drop them

null_cols = weekly_raw.columns[weekly_raw.isnull().all()].tolist()
logger.debug(f"Number of null columns in weekly_raw: {len(null_cols)}")
logger.debug(f"Null columns in weekly_raw: {null_cols}")
logger.debug(f"weekly_raw shape before dropping null columns: {weekly_raw.shape}")
weekly_raw.drop(null_cols, axis=1, inplace=True)
logger.debug(f"weekly_raw shape after dropping null columns: {weekly_raw.shape}")
# prompt: are there any columns in relapse that have only 1 nunique()?
# if so, drop them as they add no value, eliminates 45 columns

cols_with_one_nunique = [col for col in weekly_raw.columns if weekly_raw[col].nunique() == 1]
logger.debug(f"{len(cols_with_one_nunique)} {cols_with_one_nunique}")

# then drop them
logger.debug(f"weekly_raw shape: {weekly_raw.shape}")
weekly_raw.drop(cols_with_one_nunique, axis=1, inplace=True)
logger.debug(f"weekly_raw shape: {weekly_raw.shape}")


# prompt: find all cat2 and cat2 prefixed columns in raw (weekly_raw has the same, too.)
cat1_cols_raw = [col for col in raw.columns if col.startswith("cat1")]
cat2_cols_raw = [col for col in raw.columns if col.startswith("cat2")]
cat_1_2_cols = cat1_cols_raw + cat2_cols_raw


cat1_cols_raw_weekly = [col for col in weekly_raw.columns if col.startswith("cat1")]
cat2_cols_raw_weekly = [col for col in weekly_raw.columns if col.startswith("cat2")]
cat_1_2_cols_weekly = cat1_cols_raw_weekly + cat2_cols_raw_weekly

# prompt: find cat_1_2_cols_weekly not in cat_1_2_cols

# Find columns in cat_1_2_cols_weekly that are NOT in cat_1_2_cols
cols_not_in_admit = set(cat_1_2_cols_weekly) - set(cat_1_2_cols)

logger.debug(f"Columns in 'cat_1_2_cols_weekly' but not in 'cat_1_2_cols': {cols_not_in_admit}")

# prompt: find cat_1_2_cols not in cat_1_2_cols_weekly

# Find columns in cat_1_2_cols that are NOT in cat_1_2_cols_weekly
cols_not_in_weekly = set(cat_1_2_cols) - set(cat_1_2_cols_weekly)

logger.debug(f"Columns in 'cat_1_2_cols' but not in 'cat_1_2_cols_weekly': {cols_not_in_weekly}")

# prompt: get row with first todate by pid for weekly_raw

# Assuming weekly_raw is already sorted by pid and todate as in the provided code.

# Get the first todate for each pid
first_todate_by_pid = weekly_raw.groupby("pid")["todate"].first()

# To get the entire row corresponding to the first todate for each pid, you can use the following:
# prompt: do rows_with_first_todate = weekly_raw[weekly_raw.apply(lambda row: (row['pid'], row['todate']) in first_todate_by_pid.items(), axis=1)] faster
# make this run faster than 21 minutes

# To get the entire row corresponding to the first todate for each pid, you can use the following:
# This is significantly faster than using .apply with a lambda function iterating over rows.
# We can achieve the same result by merging the weekly_raw with the first_todate_by_pid series.

rows_with_first_todate = pd.merge(
    weekly_raw, first_todate_by_pid.reset_index(), on=["pid", "todate"]
)


# prompt: what columns are in both cat_1_2_cols and cat_1_2_cols_weekly

# Find the common columns
cat_1_2_cols = list(set(cat_1_2_cols) & set(cat_1_2_cols_weekly))


# prompt: join raw to first_todate_by_pid on pid,todate

# Merge raw with first_todate_by_pid on pid and todate
raw_first_weekly = pd.merge(
    raw,
    rows_with_first_todate[cat_1_2_cols + ["pid"]],
    on=["pid"],
    how="left",
    suffixes=("", "_first_todate"),
)

# prompt: append '_first_todate' to each element in diarrhea_cols

cat_1_2_cols_updated = [col + "_first_todate" for col in cat_1_2_cols]

# prompt: for each col in diarrhea_cols raw_first_weekly fillna with col+'_first_todate'

logger.debug(f'raw {raw.shape}')
for col in cat_1_2_cols:
    raw_first_weekly[col] = raw_first_weekly[col].fillna(raw_first_weekly[col + "_first_todate"])

raw = raw_first_weekly.drop(columns=cat_1_2_cols_updated)
logger.debug(f'raw {raw.shape}')

# prompt: find the 6 columns in admit that are all nulls and drop them

null_cols = admit.columns[admit.isnull().all()].tolist()
logger.debug(f"Number of null columns in admit: {len(null_cols)}")
logger.debug(f"Null columns in admit: {null_cols}")
logger.debug(f"admit shape before dropping null columns: {admit.shape}")
admit.drop(null_cols, axis=1, inplace=True)
logger.debug(f"admit shape after dropping null columns: {admit.shape}")
# prompt: find site where admit['site'] != admit['site'].str.rstrip()

# find site where admit['site'] != admit['site'].str.rstrip()
non_stripped_sites = admit[admit['site'] != admit['site'].str.rstrip()]
if not non_stripped_sites.empty:
    logger.error("Rows where 'site' column has trailing whitespace:")
    logger.error(non_stripped_sites[['pid', 'site']].head())
    if FAIL_MODE:
        raise RuntimeError(f"{non_stripped_sites.size} admit rows found where 'site' column has trailing whitespace.")
else:
    logger.info("No rows found where 'site' column has trailing whitespace after stripping.")

admit.drop("autosite", axis=1, inplace=True)

# prompt: are there any columns in admit that have only 1 nunique()?
# if so, drop them as they add no value, eliminates 10 columns

cols_with_one_nunique = [col for col in admit.columns if admit[col].nunique() == 1]
logger.debug(f"{len(cols_with_one_nunique)} {cols_with_one_nunique}")
logger.debug(f"admit {admit.shape}")
# then drop them
admit.drop(cols_with_one_nunique, axis=1, inplace=True)
logger.debug(f"admit {admit.shape}")


# these have little or no change to admit so drop them
current.drop(["b_phoneconsent", "cleaning_note", "langpref"], axis=1, inplace=True)
logger.debug(f"current {current.shape}")

# prompt: change active - awaiting PID to active for current.status

# Change 'active - awaiting PID' to 'active' in the 'status' column of the 'current' DataFrame.
# current['status'] = current['status'].replace('active - awaiting PID', 'active')

# prompt: create variable status_dead in current where current['status'] == 'dead'
# this becomes the y variable for mortality prediction
# no need to create dummy variables for the other statuses because we're not predicting them

current["status_dead"] = current["status"] == "dead"


# prompt: group weekly by pid and create a dataframe by pid with the first not null value of cols in it
# cols where value never changes in weekly for a PID
cols = [
    "pid",
    "ses_care_decisionmaker_specify",
    "md_reviewstate",
    "ses_hh_slept",
    "ses_b_foodsecurity",
    "ses_edtype_father",
    "ses_drinkingwater",
    "ses_toilet",
    "ses_care_decisionmaker",
    "ses_livingchildren",
    "ses_edtype_mother",
    "ses_hh_adults",
    "ses_walltype",
]

# Group by 'pid' and get the first non-null value for each column
weekly_first_values = weekly[cols].groupby("pid").first()

# Create a new DataFrame with the first non-null values for each 'pid'
df_pid_first_values = pd.DataFrame(weekly_first_values)

# Reset the index to make 'pid' a regular column
df_pid_first_values = df_pid_first_values.reset_index()

# prompt: join admit to df_pid_first_values on pid

# Merge admit and df_pid_first_values on 'pid'
admit = pd.merge(admit, df_pid_first_values, on="pid", how="left", suffixes=("", "_weekly_first"))


# Replace null values in 'ses_edtype_mother' with values from 'ses_edtype_mother_weekly_first'
admit["ses_walltype"] = admit["ses_walltype"].fillna(admit["ses_walltype_weekly_first"])
admit["ses_care_decisionmaker_specify"] = admit["ses_care_decisionmaker_specify"].fillna(
    admit["ses_care_decisionmaker_specify_weekly_first"]
)
admit["ses_hh_slept"] = admit["ses_hh_slept"].fillna(admit["ses_hh_slept_weekly_first"])
admit["ses_b_foodsecurity"] = admit["ses_b_foodsecurity"].fillna(
    admit["ses_b_foodsecurity_weekly_first"]
)
admit["ses_edtype_father"] = admit["ses_edtype_father"].fillna(
    admit["ses_edtype_father_weekly_first"]
)
admit["ses_drinkingwater"] = admit["ses_drinkingwater"].fillna(
    admit["ses_drinkingwater_weekly_first"]
)
admit["ses_toilet"] = admit["ses_toilet"].fillna(admit["ses_toilet_weekly_first"])
admit["ses_care_decisionmaker"] = admit["ses_care_decisionmaker"].fillna(
    admit["ses_care_decisionmaker_weekly_first"]
)
admit["ses_livingchildren"] = admit["ses_livingchildren"].fillna(
    admit["ses_livingchildren_weekly_first"]
)
admit["ses_care_decisionmaker"] = admit["ses_care_decisionmaker"].fillna(
    admit["ses_care_decisionmaker_weekly_first"]
)
admit["ses_edtype_mother"] = admit["ses_edtype_mother"].fillna(
    admit["ses_edtype_mother_weekly_first"]
)
admit["ses_hh_adults"] = admit["ses_hh_adults"].fillna(admit["ses_hh_adults_weekly_first"])

# drop the weekly_first columns and also from weekly
admit.drop(
    [
        "ses_edtype_mother_weekly_first",
        "ses_care_decisionmaker_specify_weekly_first",
        "ses_hh_slept_weekly_first",
        "ses_b_foodsecurity_weekly_first",
        "ses_edtype_father_weekly_first",
        "ses_drinkingwater_weekly_first",
        "ses_toilet_weekly_first",
        "ses_care_decisionmaker_weekly_first",
        "ses_livingchildren_weekly_first",
        "ses_hh_adults_weekly_first",
    ],
    axis=1,
    inplace=True,
)

cols.remove("pid")


#  and also from weekly as they're redundant there now
weekly.drop(cols, axis=1, inplace=True)


# TODO is this necessary?
# prompt: remove uuid: from the front of admit.uuid

admit["uuid"] = admit["uuid"].str.replace("uuid:", "", regex=False)
weekly["uuid"] = weekly["uuid"].str.replace("uuid:", "", regex=False)


logger.debug(f"admit['uuid'].isnull().sum() before check: {admit['uuid'].isnull().sum()}")
if (admit['uuid'].isnull().sum() > 0) & FAIL_MODE:
    raise RuntimeError(f"admit['uuid'] contains {admit['uuid'].isnull().sum()} nulls.")
logger.debug(f"weekly['uuid'].isnull().sum() before check: {weekly['uuid'].isnull().sum()}")
if (weekly['uuid'].isnull().sum() > 0) & FAIL_MODE:
    raise RuntimeError(f"weekly['uuid'] contains {weekly['uuid'].isnull().sum()} nulls.")
logger.debug(f"raw['uuid'].isnull().sum() before check: {raw['uuid'].isnull().sum()}")
if (raw['uuid'].isnull().sum() > 0) & FAIL_MODE:
    raise RuntimeError(f"raw['id'] contains {raw['uuid'].isnull().sum()} nulls.")

# prompt: if raw['uuid'] is null set it to raw['pid'], 3 rows in admit and raw have null uuid but their PIDs are the same 3

# raw.loc[raw['uuid'].isnull(), 'uuid'] = raw.loc[raw['uuid'].isnull(), 'pid']
# admit.loc[admit['uuid'].isnull(), 'uuid'] = admit.loc[admit['uuid'].isnull(), 'pid']

# we can drop raw.pid from raw as it matches admit.pid even though we join on uuid
# will eliminate confusion and not longer need to use admit_pid vs raw_pid column names post join to disambiguate
raw = raw.drop("pid", axis=1)

logger.debug(f"weekly {weekly.shape}")
weekly_row_ct = weekly.shape[0]

# prompt: deduplicate weekly on set=['pid','calcdate'] and use the greatest md_submission

# Deduplicate weekly data based on 'pid' and 'calcdate', keeping the row with the greatest 'md_submissiondate'
weekly = (
    weekly.sort_values(["pid", "calcdate", "md_submissiondate"], ascending=[True, True, False])
    .groupby(["pid", "calcdate"])
    .first()
    .reset_index()
)


logger.debug(f"weekly_row_ct {weekly_row_ct} weekly shape {weekly.shape}")
if (weekly_row_ct > weekly.shape[0]) & FAIL_MODE:
    raise RuntimeError(
        f"weekly processed has {weekly_row_ct - weekly.shape[0]} duplicated pid/calcdate rows."
    )

# prompt: deduplicate weekly raw on set=['pid','todate'] and use the greatest end_time
# (If you group weekly_raw by pid and count number of duplicated todate by pid, there are 38 of them.)

# Deduplicate weekly raw data based on 'pid' and 'todate', keeping the row with the greatest 'end_time'
logger.debug(f"weekly {weekly_raw.shape}")
weekly_row_ct = weekly_raw.shape[0]
weekly_raw = (
    weekly_raw.sort_values(["pid", "todate", "end_time"], ascending=[True, True, False])
    .groupby(["pid", "todate"])
    .first()
    .reset_index()
)

logger.debug(f"weekly_row_ct {weekly_row_ct} weekly_raw shape {weekly_raw.shape}")
if (weekly_row_ct > weekly_raw.shape[0]) & FAIL_MODE:
    raise RuntimeError(
        f"weekly raw has {weekly_row_ct - weekly_raw.shape[0]} duplicated pid/todate rows."
    )

# weekly['md_submissiondate'] = pd.to_datetime(weekly['md_submissiondate'],format='mixed') # so we can find duplicate submission days
weekly["calcdate"] = pd.to_datetime(
    weekly["calcdate"]
)  # so we can calculate date differences for lags

# sort descending so the most important one, the latest one, becomes column 1 if we denormalize into a sequence of columns later
# TODO sort ascending and flip the shift values and aggregate labels so code is clearer, sort later on if needed
weekly = weekly.sort_values(["pid", "calcdate"], ascending=[True, False])

# prompt: lag muac,weight,calcdate in weekly to get rate of muac and weight change

# Group by 'pid' and then calculate the lag of 'muac' and 'weight'
weekly["muac_lag"] = weekly.groupby(["pid"])["muac"].shift(-1)
weekly["weight_lag"] = weekly.groupby(["pid"])["weight"].shift(-1)
weekly["calcdate_lag"] = weekly.groupby(["pid"])["calcdate"].shift(-1)

weekly["wfh_lag"] = weekly.groupby(["pid"])["wfh"].shift(-1)
weekly["hfa_lag"] = weekly.groupby(["pid"])["hfa"].shift(-1)
weekly["wfa_lag"] = weekly.groupby(["pid"])["wfa"].shift(-1)
weekly["hl_lag"] = weekly.groupby(["pid"])["finalhl"].shift(-1)

# take the difference from the prior visit (lag)
weekly["muac_diff"] = weekly["muac"] - weekly["muac_lag"]
weekly["weight_diff"] = weekly["weight"] - weekly["weight_lag"]
weekly["calcdate_diff"] = weekly["calcdate"] - weekly["calcdate_lag"]

weekly["wfh_diff"] = weekly["wfh"] - weekly["wfh_lag"]
weekly["hfa_diff"] = weekly["hfa"] - weekly["hfa_lag"]
weekly["wfa_diff"] = weekly["wfa"] - weekly["wfa_lag"]
weekly["hl_diff"] = weekly["finalhl"] - weekly["hl_lag"]


# prompt: convert weekly['md_submissiondate_diff'] to number of fractional days
weekly["calcdate_diff"] = weekly["calcdate_diff"].dt.total_seconds() / (24 * 60 * 60)

weekly["muac_diff_rate"] = weekly["muac_diff"] / weekly["calcdate_diff"]
weekly["weight_diff_rate"] = weekly["weight_diff"] / weekly["calcdate_diff"]

weekly["wfh_diff_rate"] = weekly["wfh_diff"] / weekly["calcdate_diff"]
weekly["hfa_diff_rate"] = weekly["hfa_diff"] / weekly["calcdate_diff"]
weekly["wfa_diff_rate"] = weekly["wfa_diff"] / weekly["calcdate_diff"]
weekly["hl_diff_rate"] = weekly["hl_diff"] / weekly["calcdate_diff"]


# Print the updated DataFrame with lagged values

# prompt: group weekly by pid and create a dataframe with row_count,min(weight),max(weight),average(weight),min(muac),max(muac),average(muac) per pid

weekly_agg = weekly.groupby("pid").agg(
    weekly_row_count=("pid", "count"),
    weekly_first_calcdate=("calcdate", "last"),
    weekly_last_calcdate=("calcdate", "first"),
    weekly_last_muac=("muac", "first"),
    weekly_first_muac=("muac", "last"),
    weekly_min_muac=("muac", "min"),
    weekly_max_muac=("muac", "max"),
    weekly_avg_muac=("muac", "mean"),
    weekly_first_weight=("weight", "last"),
    weekly_last_weight=("weight", "first"),
    weekly_min_weight=("weight", "min"),
    weekly_max_weight=("weight", "max"),
    weekly_avg_weight=("weight", "mean"),
    weekly_first_hl=("finalhl", "last"),
    weekly_last_hl=("finalhl", "first"),
    weekly_min_hl=("finalhl", "min"),
    weekly_max_hl=("finalhl", "max"),
    weekly_avg_hl=("finalhl", "mean"),
    weekly_first_wfh=("wfh", "last"),
    weekly_last_wfh=("wfh", "first"),
    weekly_min_wfh=("wfh", "min"),
    weekly_max_wfh=("wfh", "max"),
    weekly_avg_wfh=("wfh", "mean"),
    weekly_first_hfa=("hfa", "last"),
    weekly_last_hfa=("hfa", "first"),
    weekly_min_hfa=("hfa", "min"),
    weekly_max_hfa=("hfa", "max"),
    weekly_avg_hfa=("hfa", "mean"),
    weekly_first_wfa=("wfa", "last"),
    weekly_last_wfa=("wfa", "first"),
    weekly_min_wfa=("wfa", "min"),
    weekly_max_wfa=("wfa", "max"),
    weekly_avg_wfa=(
        "wfa",
        "mean",
    ),
)


weekly_agg["muac_diff"] = weekly_agg["weekly_last_muac"] - weekly_agg["weekly_first_muac"]
weekly_agg["weight_diff"] = weekly_agg["weekly_last_weight"] - weekly_agg["weekly_first_weight"]
weekly_agg["hl_diff"] = weekly_agg["weekly_last_hl"] - weekly_agg["weekly_first_hl"]
weekly_agg["wfh_diff"] = weekly_agg["weekly_last_wfh"] - weekly_agg["weekly_first_wfh"]
weekly_agg["hfa_diff"] = weekly_agg["weekly_last_hfa"] - weekly_agg["weekly_first_hfa"]
weekly_agg["wfa_diff"] = weekly_agg["weekly_last_wfa"] - weekly_agg["weekly_first_wfa"]


weekly_agg["calcdate_diff"] = (
    weekly_agg["weekly_last_calcdate"] - weekly_agg["weekly_first_calcdate"]
)
weekly_agg["calcdate_diff"] = weekly_agg["calcdate_diff"].dt.total_seconds() / (24 * 60 * 60)
weekly_agg["weight_diff_ratio"] = weekly_agg["weight_diff"] / weekly_agg["weekly_first_weight"]
weekly_agg["weight_diff_ratio_rate"] = weekly_agg["weight_diff_ratio"] / weekly_agg["calcdate_diff"]
weekly_agg["muac_diff_ratio"] = weekly_agg["muac_diff"] / weekly_agg["weekly_first_weight"]
weekly_agg["muac_diff_ratio_rate"] = weekly_agg["muac_diff_ratio"] / weekly_agg["calcdate_diff"]

weekly_agg["hl_diff_ratio"] = weekly_agg["hl_diff"] / weekly_agg["weekly_first_hl"]
weekly_agg["hl_diff_ratio_rate"] = weekly_agg["hl_diff_ratio"] / weekly_agg["calcdate_diff"]
weekly_agg["wfh_diff_ratio"] = weekly_agg["wfh_diff"] / weekly_agg["weekly_first_wfh"]
weekly_agg["wfh_diff_ratio_rate"] = weekly_agg["wfh_diff_ratio"] / weekly_agg["calcdate_diff"]

weekly_agg["hfa_diff_ratio"] = weekly_agg["hfa_diff"] / weekly_agg["weekly_first_hfa"]
weekly_agg["hfa_diff_ratio_rate"] = weekly_agg["hfa_diff_ratio"] / weekly_agg["calcdate_diff"]
weekly_agg["wfa_diff_ratio"] = weekly_agg["wfa_diff"] / weekly_agg["weekly_first_wfa"]
weekly_agg["wfa_diff_ratio_rate"] = weekly_agg["wfa_diff_ratio"] / weekly_agg["calcdate_diff"]


# itp.describe()
# prompt: print itp_agg for pid == '23-0212'

# prompt: print row in itp_deduped for pid == '23-0212'

weekly_agg.reset_index(inplace=True)

# find_collinear_columns(weekly_agg,threshold=0.95,col_ct_threshold=10)

# drop max and min weight, muac as they're .99 correlated with first and last and probably only interested in first and last rather than min and max
weekly_agg.drop(
    columns=["weekly_max_weight", "weekly_min_weight", "weekly_max_muac", "weekly_min_muac"],
    inplace=True,
)

# set sort back to see if fixes pids_to_delete having too many
# TODO sort ascending and flip the shift values and aggregate labels so code is clearer, sort later on if needed
weekly = weekly.sort_values(["pid", "calcdate"])

relapse["todate"] = pd.to_datetime(relapse["todate"])
relapse["todate_month"] = relapse["todate"].dt.to_period("M")
logger.debug(relapse["todate_month"].value_counts())
logger.debug(relapse["todate"].min())
logger.debug(relapse["todate"].max())
relapse.drop("todate_month", axis=1, inplace=True)

# prompt: find the 6 columns in relapse that are all nulls and drop them
# eliminates 76 columns

null_cols = relapse.columns[relapse.isnull().all()].tolist()
logger.debug(null_cols)
relapse.drop(null_cols, axis=1, inplace=True)

# prompt: are there any columns in relapse that have only 1 nunique()? put those column names in a list
# eliminates 85 columns

cols_with_one_nunique = [col for col in relapse.columns if relapse[col].nunique() == 1]
logger.debug(cols_with_one_nunique)

# then drop them
relapse.drop(cols_with_one_nunique, axis=1, inplace=True)


# so we can group by submission day
relapse["todate"] = pd.to_datetime(relapse["todate"]).dt.date

# prompt: deduplicate relapse on pid, submission_date
# this doesn't do anything for the training data but ensures that only one row per submission day per pid

# Sort relapse by 'pid' and 'submission_date' to ensure consistent deduplication
relapse_sorted = relapse.sort_values(["pid", "todate"])
logger.debug(f'relapse_sorted {relapse_sorted.shape}')
row_ct = relapse_sorted.shape[0]
# Drop duplicates, keeping the first occurrence for each 'pid' and 'todate'
relapse_deduped = relapse_sorted.drop_duplicates(subset=["pid", "todate"], keep="first")
logger.debug(f'row_ct {row_ct} relapse_duduped {relapse_deduped.shape}')
if (row_ct > relapse_deduped.shape[0]) & FAIL_MODE:
    raise RuntimeError(f"relapse {row_ct - relapse_deduped.shape[0]} duplicated pid/todate rows.")

# Now 'relapse_deduped' contains only the first relapse record for each unique combination of 'pid' and 'submission_date'
relapse = relapse_deduped

# sort the most recent relapse first
relapse = relapse.sort_values(["pid", "todate"], ascending=False)

# prompt: find columns in relapse with only 2 unique values

# Find columns in relapse with only 2 unique values
cols_with_two_nunique = [col for col in relapse.columns if relapse[col].nunique() == 2]
relapse[cols_with_two_nunique].isnull().sum()
relapse["b_outreach"].value_counts(dropna=False)

# find_3val_bool(relapse)


convert_3val_bool(relapse, len(relapse))

# prompt: find columns in relapse with type bool


# Assuming 'relapse' DataFrame is available from the previous code
number_cols = relapse.select_dtypes(include=["number"]).columns
number_cols

# find_collinear_columns(relapse)

relapse_collinear_columns_to_drop = [
    "sub_age",
    "wkl_age",
    "set_age",
    "weight_rounded",
    "hl_rounded",
    "ptonly_weight",
    "pre_weight",
]
relapse.drop(relapse_collinear_columns_to_drop, axis=1, inplace=True)

# find_collinear_columns(relapse,threshold=.99,col_ct_threshold=50)

# prompt: group itp by pid and create a dataframe with row_count,min(weight),max(weight),average(weight),min(muac),max(muac),average(muac) per pid

relapse_agg = relapse.groupby("pid").agg(
    relapse_row_count=("pid", "count"),
    relapse_first_admit=("todate", "last"),
    relapse_last_admit=("todate", "first"),
    relapse_last_muac=("muac", "first"),
    relapse_first_muac=("muac", "last"),
    relapse_min_muac=("muac", "min"),
    relapse_max_muac=("muac", "max"),
    relapse_avg_muac=("muac", "mean"),
    relapse_first_weight=("weight", "last"),
    relapse_last_weight=("weight", "first"),
    relapse_min_weight=("weight", "min"),
    relapse_max_weight=("weight", "max"),
    relapse_avg_weight=("weight", "mean"),
    relapse_avg_age=("age", "mean"),
)


relapse_agg.reset_index(inplace=True)

# TODO, make this run faster by removing the slow apply lambda function, will save 1 minute runtime
selected_rows = relapse.groupby("pid").apply(lambda x: x.head(3))

selected_rows.drop("pid", inplace=True, axis=1)
selected_rows.reset_index(inplace=True)

relapse_series = (
    selected_rows.assign(col=selected_rows.groupby("pid").cumcount() + 1)
    .set_index(["pid", "col"])
    .unstack("col")
    .sort_index(level=(1, 0), axis=1)
)
relapse_series.columns = [f"relapse{y}_{x}" for x, y in relapse_series.columns]
# prompt: make itp_series.index a column named 'pid'


relapse_series = relapse_series.reset_index()
logger.debug(relapse_series.shape)

mh["todate"] = pd.to_datetime(mh["todate"])
mh["todate_month"] = mh["todate"].dt.to_period("M")
logger.debug(mh["todate_month"].value_counts())
logger.debug(mh["todate"].min())
logger.debug(mh["todate"].max())
mh.drop("todate_month", axis=1, inplace=True)

# prompt: does mh have duplicate pid

# Check for duplicate pids in the 'mh' DataFrame
duplicate_pids_mh = mh[mh.duplicated(subset=["pid"], keep=False)]

if not duplicate_pids_mh.empty:
    logger.error("Duplicate pids found in 'mh' DataFrame:")
    logger.error(duplicate_pids_mh["pid"].value_counts())
    if FAIL_MODE:
        raise ValueError("Duplicate pids found in mental health.")
else:
    logger.debug("No duplicate pids found in 'mh' DataFrame.")

# Drop duplicates, keeping the first occurrence for each 'pid'
# 4/25 is the todate for the duplicated pid so no need to sort mh on todate
# drop the duplicated pid
mh = mh.drop_duplicates(subset=["pid"], keep="first")

# Now mh contains only the first mh record for each duplicated 'pid'

# prompt: find the 129 columns in mh that are all nulls and drop them

null_cols = mh.columns[mh.isnull().all()].tolist()
logger.debug(null_cols)
mh.drop(null_cols, axis=1, inplace=True)

# prompt: are there any columns in mh that have only 1 nunique()? put those column names in a list and drop them

cols_with_one_nunique = [col for col in mh.columns if mh[col].nunique() == 1]
logger.debug(cols_with_one_nunique)

# then drop them, 24 columns# Merge mental health
mh.drop(cols_with_one_nunique, axis=1, inplace=True)

# prompt: find rows where df['site_admit1'] != df['site_mh']
df = pd.merge(admit, mh, on="pid", how="left", suffixes=("_admit1", "_mh"))

# Find rows where df['site_admit1'] != df['site_mh']
rows_with_difference = df[df["site_admit1"] != df["site_mh"]]

# Print or further process the result
rows_with_difference[["site_admit1", "site_mh"]].value_counts()

mh.drop("site", axis=1, inplace=True)

# prompt: find columns in relapse with only 2 unique values

# Find columns in relapse with only 2 unique values
cols_with_two_nunique = [col for col in mh.columns if mh[col].nunique() == 2]
mh[cols_with_two_nunique].isnull().sum().sort_values(ascending=False)


# Assuming 'relapse' DataFrame is available from the previous code
number_cols = mh.select_dtypes(include=["number"]).columns
logger.debug(number_cols)
logger.debug(mh.select_dtypes(include=["boolean"]).columns)

mh = convert_to_bool(mh)

find_3val_bool(mh)

# convert all 3 val boolean regardless of count
convert_3val_bool(mh, len(mh))

find_collinear_columns(mh, threshold=0.99, col_ct_threshold=20)

# drop calc_numaddtlchildren as it's just num_children - 1
mh.drop("calc_numaddtlchildren", axis=1, inplace=True)


# somehow raw still has duplicate uuid
# TODO figure out why admit raw section still has duplicate uuid
# prompt: sort raw by uuid,starttime and deduplicate raw on uuid, keeping the first row, most recent starttime

# Sort raw by uuid, starttime, and deduplicate on uuid, keeping the first row
raw = raw.sort_values(["uuid", "start_time"], ascending=[True, False])
logger.debug(raw.shape)
row_ct = raw.shape[0]
raw = raw.drop_duplicates(subset=["uuid"], keep="first")
logger.debug(f'row_ct {row_ct} raw.shape {raw.shape}')
if (row_ct > raw.shape[0]) & FAIL_MODE:
    raise RuntimeError(f"raw has {row_ct - raw.shape[0]} duplicated id rows.")

# prompt: join admit with raw then with current

# inner join keeps all the rows because uuid has been massaged in both admit and raw to match (uuid: stripped off in both)
admit_raw1 = pd.merge(admit, raw, on="uuid", how="inner", suffixes=("_admit", "_raw"))
logger.debug(admit_raw1.shape)

# Merge admit and current data on 'pid' with a inner join as current and admit have the same number of rows so left join unnecessary
admit_current = pd.merge(
    admit_raw1, current, on="pid", how="inner", suffixes=("_admit", "_current")
)
logger.debug(admit_current.shape)

# Merge weekly aggregate stats
admit_current = pd.merge(
    admit_current, weekly_agg, on="pid", how="left", suffixes=("_admit", "_weekly_agg")
)
logger.debug(admit_current.shape)

# Merge itp_roster aggregate stats
admit_current = pd.merge(
    admit_current, itp_agg, on="pid", how="left", suffixes=("_admit", "_itp_agg")
)
logger.debug(admit_current.shape)

# add the length of stay (in days) for the death cases as an attribute of admit since only one itp death row per patient
admit_current = pd.merge(
    admit_current,
    death_cases[["pid", "los_days"]],
    on="pid",
    how="left",
    suffixes=("_admit", "_itp"),
)
logger.debug(admit_current.shape)

# Merge up to 3 itp_roster flattened (rows turned into column groups) rows
admit_current = pd.merge(
    admit_current, itp_series, on="pid", how="left", suffixes=("_admit", "_itp_series")
)
logger.debug(admit_current.shape)


# Merge mental health
admit_current_mh = pd.merge(admit_current, mh, on="pid", how="inner", suffixes=("_admit", "_mh"))
logger.debug(admit_current_mh.shape)

# Merge relapse aggregate stats
admit_current_relapse = pd.merge(
    admit_current, relapse_agg, on="pid", how="inner", suffixes=("_admit", "_relapse_agg")
)
logger.debug(admit_current_relapse.shape)

# Merge up to 3 relapse flattened (rows turned into column groups) rows
admit_current_relapse = pd.merge(
    admit_current_relapse,
    relapse_series,
    on="pid",
    how="inner",
    suffixes=("_admit", "_relapse_series"),
)
logger.debug(admit_current_relapse.shape)

# Print some info
logger.debug(f'{admit.shape}, {current.shape}, {raw.shape}, {weekly_agg.shape}, {itp_agg.shape}, {itp_series.shape}, {relapse_series.shape}, {mh.shape}')

logger.debug(f'{admit_current.shape},{admit_current_mh.shape},{admit_current_relapse.shape}')

# prompt: column names that are in both weekly and weekly_raw

# Assuming weekly and weekly_raw are pandas DataFrames
common_cols = list(set(weekly.columns) & set(weekly_raw.columns))
common_cols.remove("pid")
# TODO put this in for full file if todate.x is used
# common_cols.remove('todate')


# drop common columns from weekly_raw as they're lilkely redundant
weekly_raw.drop(common_cols, axis=1, inplace=True)

# prompt: join weekly to weekly_raw on pid,calcdate
# and inner join as they all match so no need for left join as all data is preserved

weekly_raw.rename(columns={"todate": "calcdate"}, inplace=True)

weekly_joined = pd.merge(
    weekly, weekly_raw, on=["pid", "calcdate"], suffixes=["_processed", "_raw"], how="inner"
)
logger.debug(weekly_joined.shape)
logger.debug(weekly_raw.shape)
logger.debug(weekly.shape)

logger.debug(admit_current.shape)
collinear_columns_to_drop = [
    "age_admit",
    "age_raw",
    "weight_rounded",
    "md_submitterid",
    "md_edits",
    "hl_rounded",
    "md_attachmentspres",
    "md_attachmentsexp",
    "final_dose",
    "ses_hh_slept",
    "fdose_multivite",
    "fdose_pcm",
    "dose_pcm",
    "ses_livingchildren",
    "ses_hh_adults",
    "final_numweeksback",
    "dose_multivite",
    "fdose_pcm",
    "fdose_multivite",
    "final_dose_wtwin",
    "pp_temp",
    "fdose_zincsulf",
    "display_fullamoxdose",
    "rt_amoxdose_precalc",
    "fdose_otomed",
    "age_months_approx",
    "enr_approxage",
    "display_rt_fullamoxdose",
    "hl_measurement",
    "muac_measurement",
    "time_hours",
    "inac_weight",
    "ptonly_weight",
]
admit_current = admit_current.drop(
    columns=[col for col in collinear_columns_to_drop if col in admit_current.columns]
)
logger.debug(admit_current.shape)


# find_collinear_columns(admit_current,threshold=0.99,col_ct_threshold=100)

weekly_collinear_columns_to_drop = [
    "pull_lastvisitnum",
    "calc_visitnum",
    "weight_rounded",
    "ptonly_weight",
    "hl_rounded",
    "display_fullamoxdose",
    "fdose_otomed",
    "display_rt_fullamoxdose",
]

weekly_joined = weekly_joined.drop(
    columns=[col for col in weekly_collinear_columns_to_drop if col in weekly_joined.columns]
)

logger.debug(weekly_joined.shape)


# find_collinear_columns(weekly_joined,threshold=0.99,col_ct_threshold=10)

weekly_joined = weekly_joined.drop(columns=["finalhl", "pull_los", "inac_weight"])

# find_collinear_columns(weekly_joined,threshold=0.99,col_ct_threshold=10)

# get the columns we need to get the stats we need to find the gaps

weekly_joined["calcdate"] = pd.to_datetime(weekly_joined["calcdate"])
weekly_joined["calcdate_first"] = weekly_joined.groupby("pid")["calcdate"].transform("first")

weekly_joined["calcdate_days_since_first"] = (
    weekly_joined["calcdate"] - weekly_joined["calcdate_first"]
)

weekly_joined["calcdate_days_since_first"] = weekly_joined["calcdate_days_since_first"].dt.days

weekly_joined.sort_values(["pid", "calcdate"])

weekly_joined["calcdate_days_since_first_lag"] = weekly_joined.groupby("pid")[
    "calcdate_days_since_first"
].shift(1)

weekly_joined["calcdate_weeks_since_first"] = (
    weekly_joined["calcdate_days_since_first"] / 7
).round()
weekly_joined["calcdate_weeks_since_first_lag"] = weekly_joined.groupby("pid")[
    "calcdate_weeks_since_first"
].shift(1)

weekly_joined["calcdate_weeks_since_first_lag_diff"] = (
    weekly_joined["calcdate_weeks_since_first"] - weekly_joined["calcdate_weeks_since_first_lag"]
)


# prompt: count,min,max and mean of calcdate_weeks_since_first_lag_diff  by pid

# Group data by 'pid' and get the count, min, max, and mean of 'calcdate_weeks_since_first_lag_diff'
result = weekly_joined.groupby("pid")["calcdate_weeks_since_first_lag_diff"].agg(
    ["count", "min", "max", "mean"]
)


# prompt: deduplicate weekly_joined on calcdate_weeks_since_first_lag_diff using the highest calcdate_days_since_first

# Deduplicate weekly_admit based on the highest calcdate_days_since_first
weekly_joined = weekly_joined.loc[
    weekly_joined.groupby(["pid", "calcdate_weeks_since_first"])[
        "calcdate_days_since_first"
    ].idxmax()
]

# prompt: find columns in admit_weekly that are numeric, so we take the mean when filling in the imputed rows

numeric_cols = weekly_joined.select_dtypes(include=["number"]).columns


# prompt: get the prior row within pid where admit_weekly[(admit_weekly['calcdate_weeks_since_first_lag_diff'] == 2) & (admit_weekly['pid'].isin(skip_1week_pids))]

# Find rows where 'calcdate_weeks_since_first_lag_diff' is 2 and 'pid' is in skip_1week_pids
rows_to_duplicate = weekly_joined[weekly_joined["calcdate_weeks_since_first_lag_diff"] == 2]

# Get the index of the prior row for each duplicated row
new1_rows = pd.DataFrame()
for index, row in tqdm(rows_to_duplicate.iterrows()):
    pid = row["pid"]
    new_row = row.copy()
    current_date = row["calcdate"]
    prior_row = (
        weekly_joined[(weekly_joined["pid"] == pid) & (weekly_joined["calcdate"] < current_date)]
        .sort_values("calcdate", ascending=False)
        .iloc[0]
    )
    row_pair = pd.concat([row, prior_row], axis=1)
    row_pair = row_pair.T
    column_means = row_pair[numeric_cols].mean()
    # Transpose the series 'column_means' into a DataFrame row
    column_means_df = pd.DataFrame(column_means).T
    # assign mean values to the new row
    new_row[numeric_cols] = column_means_df.values[0]
    new_row["calcdate"] = current_date - pd.Timedelta(days=7)
    new_row["interpolated"] = True
    new1_rows = pd.concat([new1_rows, pd.DataFrame([new_row])])


# prompt: get the difference between new and prior_row

# Assuming 'prior_row' is defined and accessible in the current scope.
#  This example demonstrates how to calculate the difference, assuming 'new' and 'prior_row' are Series or DataFrames

# Example usage:
# new = weekly_joined.iloc[5]  # Replace with your 'new' row
# prior_row = weekly_joined.iloc[4] # Replace with your 'prior_row'


def get_difference_first_third(new, prior_row):
    """
    Calculates the difference between two rows (new and prior_row).
    Handles numeric columns and datetime columns differently.
    """

    difference = {}

    for column in new.index:
        if pd.api.types.is_numeric_dtype(new[column]):
            difference[column] = new[column] - prior_row[column]
        elif pd.api.types.is_datetime64_any_dtype(new[column]):
            difference[column] = (
                (new[column] - prior_row[column]).total_seconds() / (24 * 60 * 60) / 3
            )  # Difference in days
        else:
            difference[column] = prior_row[column] + (new[column] - prior_row[column]) / 3

    return difference


def get_difference_second_third(new, prior_row):
    """
    Calculates the difference between two rows (new and prior_row).
    Handles numeric columns and datetime columns differently.
    """

    difference = {}

    for column in new.index:
        if pd.api.types.is_numeric_dtype(new[column]):
            difference[column] = new[column] - prior_row[column]
        elif pd.api.types.is_datetime64_any_dtype(new[column]):
            difference[column] = (
                2 * (new[column] - prior_row[column]).total_seconds() / (24 * 60 * 60) / 3
            )  # Difference in days
        else:
            difference[column] = prior_row[column] + 2 * (new[column] - prior_row[column]) / 3

    return difference


# prompt: get the prior row within pid where admit_weekly[(admit_weekly['calcdate_weeks_since_first_lag_diff'] == 3) & (admit_weekly['pid'].isin(skip_2week_pids))]


rows_to_duplicate = weekly_joined[weekly_joined["calcdate_weeks_since_first_lag_diff"] == 3]

new2_rows = pd.DataFrame()
for index, row in tqdm(rows_to_duplicate.iterrows()):
    pid = row["pid"]
    new_row = pd.concat([row.copy(), row.copy()], axis=1)
    new_row = new_row.T
    current_date = row["calcdate"]
    prior_row = (
        weekly_joined[(weekly_joined["pid"] == pid) & (weekly_joined["calcdate"] < current_date)]
        .sort_values("calcdate", ascending=False)
        .iloc[0]
    )
    row_pair = pd.concat([row, prior_row], axis=1)
    row_pair = row_pair.T
    # column_means = row_pair[numeric_cols].mean()
    # Transpose the series 'column_means' into a DataFrame row
    # column_means_df = pd.DataFrame(column_means).T
    column_first_third = get_difference_first_third(row[numeric_cols], prior_row[numeric_cols])
    column_first_third_df = pd.DataFrame(column_first_third, index=[0])
    column_second_third = get_difference_second_third(row[numeric_cols], prior_row[numeric_cols])
    column_second_third_df = pd.DataFrame(column_second_third, index=[1])
    # assign mean values to the new row
    first_row = new_row.iloc[0]
    first_row[column_first_third_df.columns] = column_first_third_df.values[0]
    second_row = new_row.iloc[1]
    second_row[column_second_third_df.columns] = column_second_third_df.values[0]
    col_pos = new_row.columns.get_loc("calcdate")
    new_row.iloc[0, col_pos] = (current_date - pd.Timedelta(days=14)).date()
    new_row.iloc[1, col_pos] = (current_date - pd.Timedelta(days=7)).date()
    new_row["interpolated"] = True
    new2_rows = pd.concat([new2_rows, new_row])


# prompt: get the difference between new and prior_row

# Assuming 'prior_row' is defined and accessible in the current scope.
#  This example demonstrates how to calculate the difference, assuming 'new' and 'prior_row' are Series or DataFrames

# Example usage:
# new = weekly_joined.iloc[5]  # Replace with your 'new' row
# prior_row = weekly_joined.iloc[4] # Replace with your 'prior_row'


def get_difference_first_quarter(new, prior_row):
    """
    Calculates the difference between two rows (new and prior_row).
    Handles numeric columns and datetime columns differently.
    """

    difference = {}

    for column in new.index:
        if pd.api.types.is_numeric_dtype(new[column]):
            difference[column] = new[column] - prior_row[column]
        elif pd.api.types.is_datetime64_any_dtype(new[column]):
            difference[column] = (
                (new[column] - prior_row[column]).total_seconds() / (24 * 60 * 60) / 4
            )  # Difference in days
        else:
            difference[column] = prior_row[column] + (new[column] - prior_row[column]) / 4

    return difference


def get_difference_third_quarter(new, prior_row):
    """
    Calculates the difference between two rows (new and prior_row).
    Handles numeric columns and datetime columns differently.
    """

    difference = {}

    for column in new.index:
        if pd.api.types.is_numeric_dtype(new[column]):
            difference[column] = new[column] - prior_row[column]
        elif pd.api.types.is_datetime64_any_dtype(new[column]):
            difference[column] = (
                3 * (new[column] - prior_row[column]).total_seconds() / (24 * 60 * 60) / 4
            )  # Difference in days
        else:
            difference[column] = prior_row[column] + 3 * (new[column] - prior_row[column]) / 4

    return difference


# prompt: get the prior row within pid where admit_weekly[(admit_weekly['calcdate_weeks_since_first_lag_diff'] == 4) & (admit_weekly['pid'].isin(skip_2week_pids))]
rows_to_duplicate = weekly_joined[weekly_joined["calcdate_weeks_since_first_lag_diff"] == 4]

new3_rows = pd.DataFrame()
for index, row in tqdm(rows_to_duplicate.iterrows()):
    pid = row["pid"]
    new_row = pd.concat([row.copy(), row.copy(), row.copy()], axis=1)
    new_row = new_row.T
    current_date = row["calcdate"]
    prior_row = (
        weekly_joined[(weekly_joined["pid"] == pid) & (weekly_joined["calcdate"] < current_date)]
        .sort_values("calcdate", ascending=False)
        .iloc[0]
    )
    row_pair = pd.concat([row, prior_row], axis=1)
    row_pair = row_pair.T
    column_means = row_pair[numeric_cols].mean()
    # Transpose the series 'column_means' into a DataFrame row
    column_means_df = pd.DataFrame(column_means).T
    # assign mean values to the new row
    # new_row[numeric_cols] = column_means_df.values[0]
    second_row = new_row.iloc[1]
    second_row[numeric_cols] = column_means_df.values[0]
    column_first_quarter = get_difference_first_quarter(row[numeric_cols], prior_row[numeric_cols])
    column_first_quarter_df = pd.DataFrame(column_first_quarter, index=[0])

    column_third_quarter = get_difference_third_quarter(row[numeric_cols], prior_row[numeric_cols])
    column_third_quarter_df = pd.DataFrame(column_third_quarter, index=[2])
    # assign mean values to the new row
    first_row = new_row.iloc[0]
    first_row[column_first_quarter_df.columns] = column_first_quarter_df.values[0]
    third_row = new_row.iloc[2]
    third_row[column_third_quarter_df.columns] = column_third_quarter_df.values[0]

    col_pos = new_row.columns.get_loc("calcdate")
    new_row.iloc[0, col_pos] = (current_date - pd.Timedelta(days=21)).date()
    new_row.iloc[1, col_pos] = (current_date - pd.Timedelta(days=14)).date()
    new_row.iloc[2, col_pos] = (current_date - pd.Timedelta(days=7)).date()
    new_row["interpolated"] = True
    new3_rows = pd.concat([new3_rows, new_row])

logger.debug(f'weekly_joined {weekly_joined.shape}')

df = pd.concat([weekly_joined, new1_rows, new2_rows, new3_rows])

# prompt: apply weekly_joined dtypes to df


# Apply weekly_joined dtypes to df
df = df.astype(weekly_joined.dtypes)
weekly_joined = df.copy()

logger.debug(f'weekly_joined {weekly_joined.shape}')

# Sort the DataFrame by 'pid' and 'calcdate_weekly'
weekly_joined = weekly_joined.sort_values(["pid", "calcdate"])


weekly_joined["calcdate"] = pd.to_datetime(weekly_joined["calcdate"])
weekly_joined["calcdate_first"] = weekly_joined.groupby("pid")["calcdate"].transform("first")

weekly_joined["calcdate_days_since_first"] = (
    weekly_joined["calcdate"] - weekly_joined["calcdate_first"]
)

weekly_joined["calcdate_days_since_first"] = weekly_joined["calcdate_days_since_first"].dt.days

weekly_joined.sort_values(["pid", "calcdate"])

weekly_joined["calcdate_days_since_first_lag"] = weekly_joined.groupby("pid")[
    "calcdate_days_since_first"
].shift(1)

weekly_joined["calcdate_weeks_since_first"] = (
    weekly_joined["calcdate_days_since_first"] / 7
).round()
weekly_joined["calcdate_weeks_since_first_lag"] = weekly_joined.groupby("pid")[
    "calcdate_weeks_since_first"
].shift(1)

weekly_joined["calcdate_weeks_since_first_lag_diff"] = (
    weekly_joined["calcdate_weeks_since_first"] - weekly_joined["calcdate_weeks_since_first_lag"]
)

# prompt: count,min,max and mean of calcdate_weeks_since_first_lag_diff  by pid

# Group data by 'pid' and get the count, min, max, and mean of 'calcdate_weeks_since_first_lag_diff'
result2 = weekly_joined.groupby("pid")["calcdate_weeks_since_first_lag_diff"].agg(
    ["count", "min", "max", "mean"]
)


# prompt: find pid in weekly_joined with row count ==1

# Find pids with a row count of 1 in weekly_joined
pids_with_one_row = (
    weekly_joined.groupby("pid").size()[weekly_joined.groupby("pid").size() == 1].index.tolist()
)

# prompt: find pids in weekly_joined where result2['min'] != 1 or result2['max'] != 1

pids_to_delete = weekly_joined[
    (weekly_joined["pid"].isin(result2[(result2["min"] != 1) | (result2["max"] != 1)].index))
]["pid"].unique()

# prompt: pids_to_delete = pids_to_delete not in pids_with_one_row

pids_to_delete = [pid for pid in pids_to_delete if pid not in pids_with_one_row]


# prompt: drop rows in weekly_joined if weekly_joined['pid'].isin(pids_to_delete)
TRAIN_MODE = True
if TRAIN_MODE:
  logger.debug(weekly_joined.shape)
  weekly_joined = weekly_joined[~weekly_joined['pid'].isin(pids_to_delete)]
  logger.debug(weekly_joined.shape)

# prompt: add a column called sequence_num order of calcdate within pid in weekly_joined

weekly_joined["sequence_num"] = (
    weekly_joined.groupby("pid")["calcdate"].rank(method="first", ascending=True).astype(int)
)

# per definition "should the caregiver come back one week, or 2 weeks from now"
# default should be 1 for nulls

weekly_joined.loc[
    (
        (weekly_joined["final_numweeksback"] == 0)
        | ((weekly_joined["final_numweeksback"] > 1) & (weekly_joined["final_numweeksback"] < 2))
    ),
    "final_numweeksback",
] = 1

weekly_joined["final_numweeksback"] = weekly_joined["final_numweeksback"].fillna(1)

admit_raw1 = convert_to_bool(admit_raw1)
admit_current = convert_to_bool(admit_current)
weekly_joined = convert_to_bool(weekly_joined)

find_3val_bool(admit_current)
find_3val_bool(admit_raw1)


# only convert 3 val boolean if nulls is less than threshold (of 100 in this case)
convert_3val_bool(admit_current, 100)
convert_3val_bool(admit_raw1, 100)
# convert_3val_bool(weekly_joined,35000) none in weekly

# prompt: convert weight_weekly to numeric

# Convert 'weight_weekly' to numeric, coercing errors to NaN
weekly_joined["weight"] = pd.to_numeric(weekly_joined["weight"], errors="coerce")
admit["weight"] = pd.to_numeric(admit["weight"], errors="coerce")

weekly_joined["muac"] = pd.to_numeric(weekly_joined["muac"], errors="coerce")
weekly_joined["muac"] = pd.to_numeric(weekly_joined["muac"], errors="coerce")

# Concatenate admit to admit_weekly
anthros = pd.concat(
    [
        weekly_joined[["pid", "calcdate", "weight", "hl", "muac", "wfh", "hfa", "wfa"]],
        admit[["pid", "calcdate", "weight", "hl", "muac", "wfh", "hfa", "wfa"]],
    ],
    ignore_index=True,
)

# prompt: sort anthros by pid, calcdate

# Sort the 'anthros' DataFrame by 'pid' and then 'calcdate'
anthros = anthros.sort_values(by=["pid", "calcdate"])

# prompt: group anthros by pid, diff calcdate cumulative days from the first row in that group

# Group by 'pid' and calculate the cumulative difference in days from the first 'calcdate_weekly'
anthros["calcdate"] = pd.to_datetime(anthros["calcdate"])
anthros["days_since_first"] = anthros.groupby("pid")["calcdate"].diff().dt.days
anthros["cumulative_days"] = anthros.groupby("pid")["days_since_first"].cumsum().fillna(0)
anthros.drop(columns=["days_since_first"], inplace=True)


for anthro_col in ["hl", "wfh", "hfa", "wfa", "weight", "muac"]:
    logger.debug(anthro_col)
    # prompt: for each pid in admit call regress and add the first return value as f'{anthro_col}_trend'" and second as f'{anthro_col}_rsquared columns in admit

    # Apply the function to each unique 'pid' and create new columns
    results = []
    for pid in tqdm(anthros["pid"].unique()):
        trend, r_squared = regress(anthros, pid, anthro_col)
        results.append(
            {"pid": pid, f"{anthro_col}_trend": trend, f"{anthro_col}_rsquared": r_squared}
        )

    # Convert the list of dictionaries to a DataFrame
    results_df = pd.DataFrame(results)

    # Merge the results back into the 'admit' DataFrame
    admit_current = pd.merge(admit_current, results_df, on="pid", how="left")


# prompt: get row count by pid in admit_weekly and append that column to admit

# Assuming 'admit_weekly' and 'admit' DataFrames are already loaded and processed as in the provided code.

# Group by 'pid' and count the number of rows for each 'pid'
row_counts_by_pid = weekly_joined.groupby("pid")["pid"].count()

# Rename the 'pid' column to 'row_count'
row_counts_by_pid = row_counts_by_pid.rename("weekly_row_count_etl")

# Merge the row counts back into the 'admit' DataFrame
admit_current = pd.merge(
    admit_current, row_counts_by_pid, left_on="pid", right_index=True, how="left"
)

# prompt: left admit_current with weekly on pid


# Merge admit and weekly data on 'pid' with a left join as not all admit_current have weekly rows
admit_weekly = pd.merge(
    admit_current, weekly_joined, on="pid", how="left", suffixes=("_admit_current", "_weekly")
)

admit_weekly["calcdate_admit_current"] = pd.to_datetime(admit_weekly["calcdate_admit_current"])
# Calculate the difference in days between 'calcdate_weekly' and the previous 'calcdate_weekly' for each 'pid'
admit_weekly["calcdate_diff_weekly"] = admit_weekly.groupby("pid")["calcdate_weekly"].diff().dt.days

# Print some info
logger.debug(admit_weekly.shape)

# maor assumption that null, missing, is false
admit_current = convert_to_bool(admit_current)
admit_raw1 = convert_to_bool(admit_raw1)
admit_weekly = convert_to_bool(admit_weekly)
admit_current_mh = convert_to_bool(admit_current_mh)
admit_current_relapse = convert_to_bool(admit_current_relapse)
weekly_joined = convert_to_bool(weekly_joined)


# Find columns in 'admit' with nunique between 3 and 10 and aren't boolean
admit_current = make_categorical(admit_current)
admit_weekly = make_categorical(admit_weekly)
admit_current_mh = make_categorical(admit_current_mh)
admit_current_relapse = make_categorical(admit_current_relapse)
weekly_joined = make_categorical(weekly_joined)
admit_raw1 = make_categorical(admit_raw1)


# prompt: create a column weekly_joined['row_count']  by joining on 'pid' to weekly_joined.groupby('pid')['pid'].count()

# Group by 'pid' and get the count
pid_counts = weekly_joined.groupby("pid")["pid"].count()

# Rename the count column
pid_counts = pid_counts.rename("row_count")

# Merge the counts back into the weekly_joined DataFrame
weekly_joined = weekly_joined.merge(pid_counts, left_on="pid", right_index=True, how="left")


# dir = "/content/drive/My Drive/[PBA] Data/"
# for i in range(1,13):
#  df = weekly_joined[(weekly_joined['row_count'] >= i) & (weekly_joined['sequence_num'] <= i)]
#  with open(dir + f"analysis/weekly{i}.pkl", "wb") as f:
#    pickle.dump(df, f)

# with open(dir + f"analysis/admit_raw1.pkl", "wb") as f:
#  pickle.dump(admit_raw1, f)


weekly_joined.drop(columns=["row_count"], inplace=True)

# prompt: are there any duplicate pid in admit_current

duplicate_pids = admit_current[admit_current.duplicated(subset=["pid"], keep=False)]["pid"]
if duplicate_pids.empty:
    logger.debug("No duplicate pids found in admit_current.")
else:
    logger.critical("Duplicate pids found in admit_current:")
    logger.critical(duplicate_pids)
# prompt: abort if duplicate pids found in admit_current

if not duplicate_pids.empty:
    raise ValueError("Duplicate pids found in admit_current. Aborting.")


# prompt: are there any duplicate ['pid','calcdate'] in left_admit_weekly

# Assuming 'left_admit_weekly' DataFrame is already loaded as shown in the provided code.

duplicate_rows = admit_weekly[
    admit_weekly.duplicated(subset=["pid", "calcdate_weekly"], keep=False)
]

if duplicate_rows.empty:
    logger.debug("No duplicate ['pid', 'calcdate'] found in admit_weekly.")
else:
    logger.critical("Duplicate ['pid', 'calcdate'] found in admit_weekly:")
    logger.critical(duplicate_rows[["pid", "calcdate_weekly"]])

if not duplicate_rows.empty:
    raise ValueError("Duplicate rows found in left_admit_weekly. Aborting.")


logger.debug(f'{admit_current.shape},{admit_current_mh.shape},{admit_current_relapse.shape}, {admit_weekly.shape}')

# prompt: admit_raw is first 928 columns of admit_current
first_current_column = admit_current.columns.get_loc("phoneowner_current")
logger.debug(admit_current.columns[first_current_column - 1])

admit_raw = admit_current.iloc[:, :first_current_column].copy()


# prompt: write admit_current and left_admit_weekly to csv and export them to [PBA] Data\analysis
def write_to_analysis():
    # admit_current.to_csv(dir + "analysis/admit_current.csv", index=False)
    # admit_current_mh.to_csv(dir + "analysis/admit_current_mh.csv", index=False)
    # admit_current_relapse.to_csv(dir + "analysis/admit_current_relapse.csv", index=False)
    # admit_weekly.to_csv(dir + "analysis/admit_current_weekly.csv", index=False)
    # weekly_joined.to_csv(dir + "analysis/weekly.csv", index=False)
    # prompt: pickle admit_current and write to "analysis/admit_current.pkl"

    # Pickle admit_current and write to "analysis/admit_current.pkl"
    with open(dir + "analysis/admit_current.pkl", "wb") as f:
        pickle.dump(admit_current, f)
    with open(dir + "analysis/admit_current_mh.pkl", "wb") as f:
        pickle.dump(admit_current_mh, f)
    with open(dir + "analysis/admit_current_relapse.pkl", "wb") as f:
        pickle.dump(admit_current_relapse, f)
    with open(dir + "analysis/weekly.pkl", "wb") as f:
        pickle.dump(weekly_joined, f)
    with open(dir + "analysis/admit_weekly.pkl", "wb") as f:
        pickle.dump(admit_weekly, f)
    with open(dir + "analysis/admit_processed_raw.pkl", "wb") as f:
        pickle.dump(admit_raw, f)


logger.debug(f'shapes: {admit_current.shape},{admit_current_mh.shape},{admit_current_relapse.shape}, {admit_weekly.shape}')

logger.debug(f"pid ct: {admit_current['pid'].nunique()},{admit_current_mh['pid'].nunique()},{admit_current_relapse['pid'].nunique()}, {admit_weekly['pid'].nunique()}")


dir = "/content/drive/My Drive/[PBA] Data/"
write_to_analysis()
