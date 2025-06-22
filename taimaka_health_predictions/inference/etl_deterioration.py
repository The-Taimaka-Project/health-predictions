"""
This script will contain functions that take weekly data returned by functions in etl.py
and process it to create a time-series dataframe that is ready for inference.
"""

import os
import pickle
from warnings import simplefilter

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from google.colab import drive
from tqdm import tqdm
from util import convert_bool_to_int, infer_phq_score, regress
import json
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



# prompt: read google shared drive file

drive.mount("/content/drive")

dir = "/content/drive/My Drive/[PBA] Data/"

os.chdir("/content")


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)
simplefilter(action="ignore", category=FutureWarning)


# Load the pickle file
with open(dir + "analysis/admit_weekly.pkl", "rb") as f:
    admit_weekly = pickle.load(f)
with open(dir + "analysis/admit_processed_raw.pkl", "rb") as f:
    admit_raw = pickle.load(f)

# Load the mental health
with open(dir + "analysis/admit_current_mh.pkl", "rb") as f:
    admit_current_mh = pickle.load(f)
# Load admit/current (needed for phq inference)
with open(dir + "analysis/admit_current.pkl", "rb") as f:
    admit_current = pickle.load(f)

   


numeric_cols = admit_weekly.select_dtypes(include=np.number).columns
numeric_cat1_cols = [col for col in admit_weekly.columns if col.startswith("cat1_")]
numeric_cat2_cols = [col for col in admit_weekly.columns if col.startswith("cat2_")]


# prompt: find rows with current_status == 'nonresponse'

# Assuming admit_weekly DataFrame is already loaded as in the provided code.

# Find rows where 'current_status' is 'nonresponse'
nonresponse_rows = admit_weekly[admit_weekly["status"] == "nonresponse"]

admit_weekly["nonresponse"] = admit_weekly["status"] == "nonresponse"

# Display or further process the nonresponse rows

# Get the unique PIDs of patients with 'current_status' as 'nonresponse'
pids_nonresponse = nonresponse_rows["pid"].unique()


dead_rows = admit_weekly[admit_weekly["status_dead"] == True]
# Get the unique PIDs of patients with 'current_status' as 'dead'
pids_dead = dead_rows["pid"].unique()
logger.debug(len(pids_dead))
# admit_weekly.loc[admit_weekly['status_dead'] == True , 'status_dead_date'] = admit_weekly.loc[admit_weekly['status_dead'] == True, 'status_date']

# admit_weekly['status_dead_date'].notnull().sum()

# first_date_series = get_first_detn_date(admit_weekly,'status_dead',date_col='status_date')


# prompt: get admit_weekly where calcdate_weekly is null

# Assuming admit_weekly is already loaded as in the provided code
logger.debug(f"{admit_weekly['pid'].nunique()}, {admit_weekly.shape}")
# Filter for rows where 'calcdate_weekly' is null
admit_weekly_no_weekly = admit_weekly[admit_weekly["calcdate_weekly"].isnull()].copy()
logger.debug(f"Unique PIDs in admit_weekly_no_weekly: {admit_weekly_no_weekly['pid'].nunique()}, Shape of admit_weekly_no_weekly: {admit_weekly_no_weekly.shape}")
logger.debug(f"Difference in unique PIDs between admit_weekly and admit_weekly_no_weekly: {admit_weekly['pid'].nunique() - admit_weekly_no_weekly['pid'].nunique()}")
pids_with_visits = list(
    set(admit_weekly["pid"].unique()) - set(admit_weekly_no_weekly["pid"].unique())
)


# prompt: drop admit_weekly rows where calcdate_weekly is null as we're only interested in visit time sequences
admit_weekly_all = admit_weekly.copy()  # save all as death and nonresponse happen at admission, too

# Drop rows where 'calcdate_weekly' is null
admit_weekly.dropna(subset=["calcdate_weekly"], inplace=True)

# prompt: get max sequence_num by pid

# Assuming admit_weekly DataFrame is already loaded and processed as in the provided code.

# Group by 'pid' and get the maximum 'sequence_num' for each 'pid'
max_sequence_num_by_pid = admit_weekly.groupby("pid")["sequence_num"].max()

# prompt: left join admit_weekly to max_sequence_num_by_pid on pid

# Merge the DataFrames
admit_weekly = pd.merge(
    admit_weekly,
    max_sequence_num_by_pid.rename("max_sequence_num"),
    left_on="pid",
    right_index=True,
    how="left",
)

# prompt: get admit_weekly where sequence_num ==3


# Filter for sequence_num == 3
admit_weekly_seq3 = admit_weekly[admit_weekly["sequence_num"] == 3]


# get prior weight, lag_1
admit_weekly[f"weight_weekly_lag_1"] = admit_weekly.groupby("pid")["weight_weekly"].shift(1)

# Static weight or weight loss for 4 consecutive weeks
def static_or_weight_loss_4_weeks(df):
    # Create a boolean Series indicating whether the weight is static or decreased compared to 4 weeks prior
    df["static_weight_loss_1w"] = df["weight_weekly"] <= df["weight_weekly_lag_1"]

    # Group by 'pid' and check for 4 consecutive True values in 'static_or_loss_4w'
    # Using rolling window to check consecutive values
    static_or_loss_4w_consecutive = (
        df.groupby("pid")["static_weight_loss_1w"]
        .rolling(window=4, min_periods=4)
        .apply(lambda x: all(x), raw=True)
    )

    # Instead of direct assignment, use reset_index to align the index:
    df["static_or_weight_loss_4_weeks"] = static_or_loss_4w_consecutive.reset_index(
        level=0, drop=True
    ).fillna(False)
    pd.set_option("future.no_silent_downcasting", True)
    # prompt: convert 0 to False and 1 to True in admit_weekly['static_or_loss_4w_consecutive']
    df["static_or_weight_loss_4_weeks"] = df["static_or_weight_loss_4_weeks"].replace(
        {0: False, 1: True}
    )

    return df


admit_weekly = static_or_weight_loss_4_weeks(admit_weekly)

# prompt: Weight at week 3 is lower than weight at admission

# Assuming admit_weekly DataFrame is already loaded as in the provided code.

# Filter for sequence_num == 3
admit_weekly_seq3 = admit_weekly[admit_weekly["sequence_num"] == 3]

# Compare weight at week 3 to weight at admission
admit_weekly_seq3["weight_at_week3_lower_than_admission"] = (
    admit_weekly_seq3["weight_weekly"] < admit_weekly_seq3["weight_admit_current"]
)


# prompt: join admit_weekly_seq3 to admit_weekly on ['pid', 'calcdate_weekly']
# propagates future into the past however, so this is a data leak
admit_weekly = pd.merge(
    admit_weekly,
    admit_weekly_seq3[["pid", "calcdate_weekly", "weight_at_week3_lower_than_admission"]],
    on=["pid", "calcdate_weekly"],
    how="left",
)


# prompt: Poor weight gain (<5 g/kg/day) for 4 consecutive weeks

# Assuming admit_weekly DataFrame is already loaded as in the provided code.


def poor_weight_gain_4_weeks(df):
    # Calculate weight gain per day
    # df['weight_gain_per_day'] = df.groupby('pid')['weight_weekly'].diff() / 7  # Assuming weekly measurements
    df["weight_gain_per_day"] = (
        df["weight_diff_weekly"] * 1000 / df["weight_weekly"] / df["calcdate_diff_weekly"]
    )

    # Check for poor weight gain (<5 g/kg/day) for 4 consecutive weeks
    df["poor_weight_gain"] = df["weight_gain_per_day"] < 5  # Adjust 5 based on your requirement

    # Group by 'pid' and check for 4 consecutive True values in 'poor_weight_gain'
    poor_weight_gain_4w_consecutive = (
        df.groupby("pid")["poor_weight_gain"]
        .rolling(window=4, min_periods=4)
        .apply(lambda x: all(x), raw=True)
    )

    # Assign the result back to the DataFrame, handling potential index mismatches
    df["poor_weight_gain_4_weeks"] = poor_weight_gain_4w_consecutive.reset_index(
        level=0, drop=True
    ).fillna(False)
    df["poor_weight_gain_4_weeks"] = df["poor_weight_gain_4_weeks"].replace({0: False, 1: True})

    return df


admit_weekly = poor_weight_gain_4_weeks(admit_weekly)

# prompt: Weight loss for 3 consecutive weeks (not related to loss of oedema)


# Weight loss for 3 consecutive weeks (not related to loss of oedema)
def weight_loss_3_consecutive_weeks(df):
    # Check for weight loss in three consecutive weeks

    # Create a boolean Series indicating whether the weight decreased compared to prior
    df["strict_weight_loss_1w"] = df["weight_weekly"] < df["weight_weekly_lag_1"]

    weight_loss_3_weeks = (
        df.groupby("pid")["strict_weight_loss_1w"]
        .rolling(window=3, min_periods=3)
        .apply(lambda x: all(x), raw=True)
    )

    # Instead of direct assignment, use reset_index to align the index:
    df["weight_loss_3_weeks"] = weight_loss_3_weeks.reset_index(level=0, drop=True).fillna(False)
    pd.set_option("future.no_silent_downcasting", True)
    # prompt: convert 0 to False and 1 to True in admit_weekly['weight_loss_3_weeks']
    df["weight_loss_3_weeks"] = df["weight_loss_3_weeks"].replace({0: False, 1: True})
    # not related to loss of oedema
    df["weight_loss_3_weeks"] = (df["weight_loss_3_weeks"]) & (df["cat2_oedema_weekly"] == False)

    return df


admit_weekly = weight_loss_3_consecutive_weeks(admit_weekly)

# prompt: get the row with max calcdate_weekly for a pid and then select those where the max row admit_weekly['static_or_weight_loss_4_weeks'] == True

# Assuming admit_weekly DataFrame is already loaded as in the provided code.

# Group by 'pid' and get the row with the maximum 'calcdate_weekly' for each 'pid'
max_calcdate_rows = admit_weekly.loc[admit_weekly.groupby("pid")["calcdate_weekly"].idxmax()]

# Filter the rows where 'static_or_weight_loss_4_weeks' is True in the max 'calcdate_weekly' rows
pids_static_or_weight_loss_4_weeks_latest = max_calcdate_rows[
    max_calcdate_rows["static_or_weight_loss_4_weeks"] == True
]["pid"].unique()
pids_poor_weight_gain_4_weeks_latest = max_calcdate_rows[
    max_calcdate_rows["poor_weight_gain_4_weeks"] == True
]["pid"].unique()
pids_weight_loss_3_weeks_latest = max_calcdate_rows[
    max_calcdate_rows["weight_loss_3_weeks"] == True
]["pid"].unique()

pids_static_or_weight_loss_4_weeks = admit_weekly[
    admit_weekly["static_or_weight_loss_4_weeks"] == True
]["pid"].unique()
pids_poor_weight_gain_4_weeks = admit_weekly[admit_weekly["poor_weight_gain_4_weeks"] == True][
    "pid"
].unique()
pids_weight_loss_3_weeks = admit_weekly[admit_weekly["weight_loss_3_weeks"] == True]["pid"].unique()
pids_weight_at_week3_lower_than_admission = admit_weekly[
    admit_weekly["weight_at_week3_lower_than_admission"] == True
]["pid"].unique()


# prompt: admit_weekly[weight_loss_ever] = (static_or_weight_loss_4_weeks | poor_weight_gain_4_weeks | weight_loss_3_weeks| weight_at_week3_lower_than_admission)

admit_weekly["detn_weight_loss_ever"] = (
    admit_weekly["static_or_weight_loss_4_weeks"]
    | admit_weekly["poor_weight_gain_4_weeks"]
    | admit_weekly["weight_loss_3_weeks"]
    | admit_weekly["weight_at_week3_lower_than_admission"]
)

# prompt: admit_weekly[detn_weight_loss_latest] = (static_or_weight_loss_4_weeks | poor_weight_gain_4_weeks | weight_loss_3_weeks| weight_at_week3_lower_than_admission) & sequence_num == max_sequence_num

admit_weekly["detn_weight_loss_latest"] = (
    admit_weekly["static_or_weight_loss_4_weeks"]
    | admit_weekly["poor_weight_gain_4_weeks"]
    | admit_weekly["weight_loss_3_weeks"]
    | admit_weekly["weight_at_week3_lower_than_admission"]
) & (admit_weekly["sequence_num"] == admit_weekly["max_sequence_num"])

# prompt: pids_weight_loss_latest = admit_weekly[detn_weight_loss_latest] == True

pids_weight_loss_latest = admit_weekly[admit_weekly["detn_weight_loss_latest"] == True][
    "pid"
].unique()

# prompt: pids_weight_loss_latest =  set of pids_static_or_weight_loss_4_weeks_latest, pids_poor_weight_gain_4_weeks_latest, pids_weight_loss_3_weeks_latest
# TODO why are 8 less when doing this way?
# pids_weight_loss_latest = list(set(list(pids_static_or_weight_loss_4_weeks_latest) + list(pids_poor_weight_gain_4_weeks_latest) + list(pids_weight_loss_3_weeks_latest)))

pids_weight_loss_ever = admit_weekly[admit_weekly["detn_weight_loss_ever"] == True]["pid"].unique()
# pids_weight_loss_ever = list(set(list(pids_static_or_weight_loss_4_weeks) + list(pids_poor_weight_gain_4_weeks) + list(pids_weight_loss_3_weeks) + list(pids_weight_at_week3_lower_than_admission)))

# get the column names
# Filter columns that contain 'cat1' and end with '_weekly'
cat1_weekly_cols = [
    col for col in admit_weekly.columns if "cat1" in col and col.endswith("_weekly")
]
cat2_weekly_cols = [
    col for col in admit_weekly.columns if "cat2" in col and col.endswith("_weekly")
]

cat1_weekly_cols = admit_weekly[cat1_weekly_cols].select_dtypes(include=["bool"]).columns
cat2_weekly_cols = admit_weekly[cat2_weekly_cols].select_dtypes(include=["bool"]).columns

# Filter admit_raw columns that contain 'cat1' and can be summed
cat1_cols = (
    admit_raw[[col for col in admit_raw.columns if "cat1" in col]]
    .select_dtypes(include=["bool"])
    .columns
)
cat2_cols = (
    admit_raw[[col for col in admit_raw.columns if "cat2" in col]]
    .select_dtypes(include=["bool"])
    .columns
)


# prompt: get columns that contain cat1 and end in _weekly from admit_weekly
# prompt: sum cat1_cols by pid

# Group by 'pid' and sum the 'cat1' columns
cat1_sum_by_pid = admit_raw.groupby("pid")[cat1_cols].sum()
# Calculate the sum of each row in cat1_sum_by_pid
cat1_sum_by_pid = cat1_sum_by_pid.sum(axis=1)
cat1_sum_by_pid.name = "admit_cat1_complications"
# Group by 'pid' and sum the 'cat2' columns
cat2_sum_by_pid = admit_raw.groupby("pid")[cat2_cols].sum()
# Calculate the sum of each row in cat2_sum_by_pid
cat2_sum_by_pid = cat2_sum_by_pid.sum(axis=1)
cat2_sum_by_pid.name = "admit_cat2_complications"


def count_cat1_cat2(admit_weekly, cat1_weekly_cols, cat2_weekly_cols):
    # Group by 'pid' and sum the 'cat1' columns
    cat1_sum_by_pid_weekly = admit_weekly.groupby("pid")[cat1_weekly_cols].sum()
    # Calculate the sum of each row in cat1_sum_by_pid
    cat1_sum_by_pid_weekly = cat1_sum_by_pid_weekly.sum(axis=1)
    # Group by 'pid' and sum the 'cat2' columns
    cat2_sum_by_pid_weekly = admit_weekly.groupby("pid")[cat2_weekly_cols].sum()
    # Calculate the sum of each row in cat2_sum_by_pid
    cat2_sum_by_pid_weekly = cat2_sum_by_pid_weekly.sum(axis=1)
    cat1_sum_by_pid_weekly.name = "cat1_complications_weekly"
    cat2_sum_by_pid_weekly.name = "cat2_complications_weekly"
    return cat1_sum_by_pid_weekly, cat2_sum_by_pid_weekly


# prompt: append _weekly to weekly_raw cat1 columns


cat1_weekly = [
    col + "_weekly"
    for col in [
        "cat1_fever",
        "cat1_hypothermia",
        "cat1_measles",
        "cat1_breath",
        "cat1_vomiting",
        "cat1_bloodstool",
        "cat1_dehyd",
        "cat1_fissures",
        "cat1_orash",
        "cat1_ears",
        "cat1_noeat",
        "cat1_notests",
        "cat1_anemia",
        "cat1_overall",
    ]
]

# prompt: lag each column in cat1_weekly

# Assuming 'admit_weekly' DataFrame and 'cat1_weekly' list are already defined as in the previous code.

for col in cat1_weekly:
    admit_weekly[f"{col}_lag_1"] = admit_weekly.groupby("pid")[col].shift(1)

# prompt: create a map of selected_columns to cat1_weekly
# Create a dictionary to map selected_columns to their corresponding weekly columns
col_map = {
    "cat1_fever_admit_current": "cat1_fever_weekly",
    "cat1_hypothermia_admit_current": "cat1_hypothermia_weekly",
    "cat1_measles_admit_current": "cat1_measles_weekly",
    "cat1_breath_admit_current": "cat1_breath_weekly",
    "cat1_vomiting_admit_current": "cat1_vomiting_weekly",
    "cat1_bloodstool_admit_current": "cat1_bloodstool_weekly",
    "cat1_dehyd_admit_current": "cat1_dehyd_weekly",
    "cat1_fissures_admit_current": "cat1_fissures_weekly",
    "cat1_orash_admit_current": "cat1_orash_weekly",
    "cat1_ears_admit_current": "cat1_ears_weekly",
    "cat1_noeat_admit_current": "cat1_noeat_weekly",
    "cat1_notests_admit_current": "cat1_notests_weekly",
    "cat1_anemia_admit_current": "cat1_anemia_weekly",
    "cat1_overall_admit_current": "cat1_overall_weekly",
}

filtered_admit_weekly = admit_weekly[admit_weekly["sequence_num"] == 1]

for key, value in col_map.items():
    # Find rows where the lag of the current cat1 column is False and differs from the current value
    filtered_admit_weekly[f"{key}_diff_from_first_visit_and_admit_is_false"] = (
        filtered_admit_weekly[key] != filtered_admit_weekly[f"{value}"]
    ) & (filtered_admit_weekly[f"{key}"] == False)

rows_meeting_first_criteria = filtered_admit_weekly[
    filtered_admit_weekly[
        [f"{col}_diff_from_first_visit_and_admit_is_false" for col in col_map.keys()]
    ].any(axis=1)
].copy()

rows_meeting_first_criteria_pids = rows_meeting_first_criteria["pid"].unique()

rows_meeting_first_criteria["new_onset_medical_complication"] = True


# prompt: find rows where lag of cat1_weekly is false and differs from current value

# Assuming admit_weekly DataFrame and cat1_weekly list are already defined.

for col in cat1_weekly:
    # Find rows where the lag of the current cat1 column is False and differs from the current value
    admit_weekly[f"{col}_diff_from_lag_and_lag_is_false"] = (
        admit_weekly[col] != admit_weekly[f"{col}_lag_1"]
    ) & (admit_weekly[f"{col}_lag_1"] == False)

# Example: Display rows where any of the cat1 columns meet the criteria
rows_meeting_criteria = admit_weekly[
    admit_weekly[[f"{col}_diff_from_lag_and_lag_is_false" for col in cat1_weekly]].any(axis=1)
]

rows_meeting_criteria = rows_meeting_criteria[
    ~rows_meeting_criteria["pid"].isin(rows_meeting_first_criteria_pids)
].copy()

rows_meeting_criteria["new_onset_medical_complication"] = True

# prompt: concatenate rows_meeting_criteria and rows_meeting_first_criteria

# Concatenate the two DataFrames
concatenated_rows = pd.concat([rows_meeting_criteria, rows_meeting_first_criteria])

admit_weekly = pd.merge(
    admit_weekly,
    concatenated_rows[["pid", "calcdate_weekly", "new_onset_medical_complication"]],
    on=["pid", "calcdate_weekly"],
    how="left",
)


pids_with_new_onset_medical_complication = concatenated_rows["pid"].unique()


# prompt: set admit_weekly['new_onset_medical_complication_latest']  = (new_onset_medical_complication ==True & sequence_num == max_sequence_num)

# Assuming admit_weekly DataFrame, max_sequence_num column, and new_onset_medical_complication column are already defined.

admit_weekly["new_onset_medical_complication_latest"] = (
    admit_weekly["new_onset_medical_complication"] == True
) & (admit_weekly["sequence_num"] == admit_weekly["max_sequence_num"])

pids_with_new_onset_medical_complication_latest = admit_weekly[
    admit_weekly["new_onset_medical_complication_latest"] == True
]["pid"].unique()


# Find column names containing 'diff_from_lag_and_lag_is_false'
columns_with_diff_from_lag = [
    col for col in admit_weekly.columns if "diff_from_lag_and_lag_is_false" in col
]


# prompt: for col in admit_weekly columns_with_diff_from_lag remove _weekly_diff_from_lag_and_lag_is_false and prepend y_

# Assuming admit_weekly DataFrame and columns_with_diff_from_lag list are already defined.

for col in columns_with_diff_from_lag:
    new_col_name = "y_" + col.replace("_weekly_diff_from_lag_and_lag_is_false", "")
    admit_weekly = admit_weekly.rename(columns={col: new_col_name})


y_cat1 = columns_with_diff_from_lag.copy()
y_cat1 = ["y_" + col.replace("_weekly_diff_from_lag_and_lag_is_false", "") for col in y_cat1]


cols_admit_current_diff_from_first_visit_and_admit_is_false = [
    col
    for col in filtered_admit_weekly.columns
    if "diff_from_first_visit_and_admit_is_false" in col
]

for col in cols_admit_current_diff_from_first_visit_and_admit_is_false:
    new_col_name = "temp_" + col.replace(
        "_admit_current_diff_from_first_visit_and_admit_is_false", ""
    )
    filtered_admit_weekly = filtered_admit_weekly.rename(columns={col: new_col_name})


temp_cat1 = cols_admit_current_diff_from_first_visit_and_admit_is_false.copy()
temp_cat1 = [
    "temp_" + col.replace("_admit_current_diff_from_first_visit_and_admit_is_false", "")
    for col in temp_cat1
]


admit_weekly = admit_weekly.join(filtered_admit_weekly[temp_cat1], how="left")
for col in temp_cat1:
    admit_weekly.loc[
        (admit_weekly["sequence_num"] == 1), col.replace("temp_", "y_")
    ] = admit_weekly[col]

admit_weekly.drop(columns=temp_cat1, inplace=True)

# prompt: find cat1 columns in admit_weekly

# Filter columns that contain 'cat1' and end with '_weekly'
cat1_weekly_cols = [col for col in admit_weekly.columns if "cat1" in col]


# prompt: set value of None to False for admit_weekly['cat2_oedema_weekly']


admit_weekly["oedema_status_weekly"] = admit_weekly["oedema_status_weekly"].fillna("healthy")

# prompt: lag c_oedema_weekly,cat2_oedema_weekly

# Create lagged columns for 'c_oedema_weekly' and 'cat2_oedema_weekly'
admit_weekly["c_oedema_weekly_lag_1"] = admit_weekly.groupby("pid")["c_oedema_weekly"].shift(1)
admit_weekly["cat2_oedema_weekly_lag_1"] = admit_weekly.groupby("pid")["cat2_oedema_weekly"].shift(1)

# prompt: find rows where lag of cat2_oedema_weekly_lag_1 is false and differs from current value or c_oedema_weekly is greater than lagged value

# Find rows where the lag of 'cat2_oedema_weekly_lag_1' is False and differs from the current value
# Or where 'c_oedema_weekly' is greater than the lagged value

# Assuming admit_weekly DataFrame is already loaded and processed as in the provided code.

# Identify rows meeting the specified criteria
admit_weekly["oedema_criteria_met"] = (
    (admit_weekly["cat2_oedema_weekly"] != admit_weekly["cat2_oedema_weekly_lag_1"])
    & (admit_weekly["cat2_oedema_weekly_lag_1"] == False)
    | (admit_weekly["c_oedema_weekly"] > admit_weekly["c_oedema_weekly_lag_1"])
    | (admit_weekly["cat2_oedema_weekly"] == True)
    & (admit_weekly["cat2_oedema_admit_current"] == False)
)

# Display or further process the rows where the criteria is met
oedema_rows = admit_weekly[admit_weekly["oedema_criteria_met"]]
pids_oedema_criteria_met = oedema_rows["pid"].unique()


admit_weekly["oedema_initial_appearance"] = (
    admit_weekly["cat2_oedema_weekly"] != admit_weekly["cat2_oedema_weekly_lag_1"]
) & (admit_weekly["cat2_oedema_weekly_lag_1"] == False) | (
    admit_weekly["cat2_oedema_weekly"] == True
) & (
    admit_weekly["cat2_oedema_admit_current"] == False
)

# Display or further process the rows where the criteria is met
oedema_appearance_rows = admit_weekly[admit_weekly["oedema_initial_appearance"]]


# prompt: get those rows with pid, sequence_num+3 in oedema_appearance_rows

# Assuming oedema_appearance_rows DataFrame is already created as in the provided code.

# Create a new DataFrame with 'pid' and 'sequence_num+3'
new_oedema_appearance_rows = oedema_appearance_rows[
    ["pid", "sequence_num", "cat2_oedema_weekly"]
].copy()
new_oedema_appearance_rows["sequence_num"] = new_oedema_appearance_rows["sequence_num"] + 3


# prompt: join admit_weekly,new_oedema_appearance_rows on pid,sequence_num

# Assuming admit_weekly and new_oedema_appearance_rows DataFrames are already defined.

# Perform the merge operation
admit_weekly = pd.merge(
    admit_weekly,
    new_oedema_appearance_rows,
    on=["pid", "sequence_num"],
    how="left",
    suffixes=("", "_3rd_week"),
)

# Now 'merged_df' contains the joined data.  You can further process or analyze it as needed.

# prompt: find rows where cat2_oedema_weekly is True and cat2_oedema_weekly_3rd_week is True

# Assuming admit_weekly DataFrame is already loaded and processed as in the provided code.

# Find rows where both 'cat2_oedema_weekly' and 'cat2_oedema_weekly_3rd_week' are True
filtered_rows = admit_weekly[
    (admit_weekly["cat2_oedema_weekly"] == True)
    & (admit_weekly["cat2_oedema_weekly_3rd_week"].isnull())
]

admit_weekly["oedema_not_disappearing"] = (admit_weekly["cat2_oedema_weekly"] == True) & (
    admit_weekly["cat2_oedema_weekly_3rd_week"].isnull()
)


# Display or further process the filtered rows
pids_oedema_not_disappearing = filtered_rows["pid"].unique()


# prompt: admit_weekly['failure_to_lose_oedema_latest'] = (oedema_not_disappearing | oedema_criteria_met) & sequence_num == max_sequence_num

# Assuming admit_weekly, oedema_not_disappearing, oedema_criteria_met, and max_sequence_num are defined.

admit_weekly["failure_to_lose_oedema_latest"] = (
    (admit_weekly["oedema_not_disappearing"] == True)
    | (admit_weekly["oedema_criteria_met"] == True)
) & (admit_weekly["sequence_num"] == admit_weekly["max_sequence_num"])

pids_failure_to_lose_oedema_latest = admit_weekly[
    admit_weekly["failure_to_lose_oedema_latest"] == True
]["pid"].unique()


pids_failure_to_lose_oedema = list(
    set(list(pids_oedema_not_disappearing) + list(pids_oedema_criteria_met))
)

# prompt: find 'muac_weekly' <= prior 'muac_weekly' for 2 consecutive rows

# Assuming admit_weekly DataFrame is already loaded and processed as in the provided code.

# Create a lagged column for 'muac_weekly'
admit_weekly["muac_weekly_lag_1"] = admit_weekly.groupby("pid")["muac_weekly"].shift(1)

# Check for static or MUAC loss for 2 consecutive weeks
admit_weekly["muac_loss_2_weeks"] = admit_weekly["muac_weekly"] <= admit_weekly["muac_weekly_lag_1"]

# Group by 'pid' and check for 2 consecutive True values in 'muac_loss_2_weeks'
muac_loss_2_weeks_consecutive = (
    admit_weekly.groupby("pid")["muac_loss_2_weeks"]
    .rolling(window=2, min_periods=2)
    .apply(lambda x: all(x), raw=True)
)

# Assign the result back to the DataFrame, handling potential index mismatches
admit_weekly["muac_loss_2_weeks_consecutive"] = muac_loss_2_weeks_consecutive.reset_index(
    level=0, drop=True
).fillna(False)
# Now 'admit_weekly' contains a new column 'muac_loss_2_weeks_consecutive' indicating whether MUAC has been static or decreased for two consecutive weeks for each patient.

# Convert to boolean
admit_weekly["muac_loss_2_weeks_consecutive"] = admit_weekly[
    "muac_loss_2_weeks_consecutive"
].astype(bool)

# get the pids
pids_muac_loss = admit_weekly[admit_weekly["muac_loss_2_weeks_consecutive"] == True]["pid"].unique()

# prompt: admit_weekly[muac_loss_2_weeks_consecutive_latest] = muac_loss_2_weeks_consecutive & sequence_num = max_sequence_num

# Assuming admit_weekly and relevant columns are already defined as in the provided code.

# Create 'muac_loss_2_weeks_consecutive_latest' based on the condition
admit_weekly["muac_loss_2_weeks_consecutive_latest"] = (
    admit_weekly["muac_loss_2_weeks_consecutive"] == True
) & (admit_weekly["sequence_num"] == admit_weekly["max_sequence_num"])

# get the pids
pids_muac_loss_latest = admit_weekly[admit_weekly["muac_loss_2_weeks_consecutive_latest"] == True][
    "pid"
].unique()

# 23-0811
# admit_weekly[admit_weekly['pid'] == '24-2250'][['pid','sequence_num','calcdate_weekly','muac_weekly','muac_loss_2_weeks_consecutive','muac_loss_2_weeks_consecutive_latest','interpolated']]

#current = pd.read_csv(
#    "/content/drive/My Drive/[PBA] Full datasets/" + "FULL_pba_current_processed_2024-11-15.csv"
#)

logger.debug(admit_weekly["pid"].nunique())

pids_deterioration = list(
    set(
        list(pids_weight_loss_ever)
        + list(pids_with_new_onset_medical_complication)
        + list(pids_failure_to_lose_oedema)
        + list(pids_muac_loss)
        + list(pids_nonresponse)
        + list(pids_dead)
    )
)
logger.debug(len(pids_deterioration))

pids_deterioration_latest = list(
    set(
        list(pids_weight_loss_latest)
        + list(pids_with_new_onset_medical_complication_latest)
        + list(pids_failure_to_lose_oedema_latest)
        + list(pids_muac_loss_latest)
        + list(pids_nonresponse)
        + list(pids_dead)
    )
)

logger.debug(len(pids_deterioration_latest))

# prompt: list columns in admit_weekly starting with loc 980

first_added_col = admit_weekly.columns.get_loc("max_sequence_num")

# prompt: find columns that are single value and nonnull, then drop them

single_value_cols = [
    col
    for col in admit_weekly.columns
    if admit_weekly[col].nunique() == 1 and admit_weekly[col].notna().all()
]
logger.debug("Single value columns:")
logger.debug(single_value_cols)
admit_weekly.drop(columns=single_value_cols, inplace=True)


single_value_cols = [
    col
    for col in admit_weekly_all.columns
    if admit_weekly_all[col].nunique() == 1 and admit_weekly_all[col].notna().all()
]
logger.debug("Single value columns:")
logger.debug(single_value_cols)
admit_weekly_all.drop(columns=single_value_cols, inplace=True)


pd.set_option("future.no_silent_downcasting", True)


def convert_to_bool(df):
    # Identify columns that are True/False and convert them to boolean
    for col in df.columns:
        if pd.api.types.is_bool_dtype(df[col]):
            continue
        elif all(x in [True, False, 1, 0] for x in df[col].unique()):
            df[col] = df[col].astype(bool)
        elif all(x in [True, False, 1, 0, None] for x in df[col].unique()):
            df[col] = df[col].replace({None: False}).astype(bool)


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
                    logger.debug(
                        f"Found 3-val bool column '{col}' with null count: {null_ct} {null_ct/size*100:.1f}% sum:{sum}"
                    )
                else:
                    logger.debug(
                        f"Found 3-val bool column '{col}' with null count: {df[col].isnull().sum()} sum:{sum}"
                    )


# prompt: convert detn columns with unique values [True nan False] to boolean


# Identify columns with unique values [True, nan, False] and convert them to boolean
def convert_3val_bool(df, threshold):
    for col in df.columns:
        if "lag" in col.lower():
            continue
        if len(df[col].unique()) == 3:
            unique_vals = df[col].unique()
            if all(val in [True, False] or pd.isna(val) for val in unique_vals):
                null_ct = df[col].isnull().sum()
                if null_ct < threshold:
                    # print(f"Converting 3-val bool column '{col}' with null count: {null_ct}")
                    df[col] = df[col].fillna(False).astype(bool)


find_3val_bool(admit_weekly)

convert_3val_bool(admit_weekly, len(admit_weekly))
convert_3val_bool(admit_weekly_all, len(admit_weekly_all))

find_3val_bool(admit_weekly)

convert_to_bool(admit_weekly)
convert_to_bool(admit_weekly_all)
# prompt: get boolean columns in det

# Assuming 'detn' DataFrame is already loaded as in the provided code.

boolean_columns = admit_weekly.select_dtypes(include=["bool"]).columns
logger.debug("Boolean columns:")
logger.debug(boolean_columns.tolist())

# Convert boolean columns to numeric
for col in boolean_columns:
    admit_weekly[col] = admit_weekly[col].astype(int)

detn_cols = [
    "detn_weight_loss_ever",
    "new_onset_medical_complication",
    "muac_loss_2_weeks_consecutive",
    "oedema_not_disappearing",
    "nonresponse",
    "status_dead",
]

detn_weight_loss_cols = [
    "static_or_weight_loss_4_weeks",
    "poor_weight_gain_4_weeks",
    "weight_loss_3_weeks",
    "weight_at_week3_lower_than_admission",
]

logger.debug(admit_weekly[detn_cols].sum())

# add last weekly row to admit_row
start_col = admit_weekly.columns.get_loc("calcdate_weekly")
end_col = admit_weekly.columns.get_loc("sequence_num")

weekly_columns = admit_weekly.columns[start_col : end_col + 1]

# Add 'pid' to weekly_columns
weekly_columns = weekly_columns.tolist()  # Convert Index to list for mutability

weekly_columns.insert(0, "weight")
weekly_columns.insert(0, "muac")
weekly_columns.remove("weight_weekly")
weekly_columns.remove("muac_weekly")

weekly_columns.insert(0, "wfh")
weekly_columns.insert(0, "hfa")
weekly_columns.remove("wfh_weekly")
weekly_columns.remove("hfa_weekly")
weekly_columns.insert(0, "wfa")
weekly_columns.remove("wfa_weekly")


if "pid" not in weekly_columns:
    weekly_columns.insert(0, "pid")  # add pid to the beginning of the list


def trend(detn_prior, admit_weekly, admit, detn_col):
    # concatenate admit to admit_weekly['pid','calcdate_weekly','weight_weekly']
    # Concatenate admit to admit_weekly

    anthros = pd.concat(
        [
            detn_prior[["pid", "calcdate_weekly", "weight", "muac", "hl", "wfh", "hfa", "wfa"]],
            admit,
        ],
        ignore_index=True,
    )
    # prompt: sort anthros by pid, calcdate_weekly

    # Sort the 'anthros' DataFrame by 'pid' and then 'calcdate_weekly' so admittance row is first for each pid
    anthros = anthros.sort_values(by=["pid", "calcdate_weekly"])
    # prompt: group anthros by pid, diff calcdate_weekly cumulative days from the first row in that group

    # Group by 'pid' and calculate the cumulative difference in days from the first 'calcdate_weekly'
    anthros["calcdate_weekly"] = pd.to_datetime(anthros["calcdate_weekly"])
    anthros["days_since_first"] = anthros.groupby("pid")["calcdate_weekly"].diff().dt.days
    # cumulative days is the regressor column
    anthros["cumulative_days"] = anthros.groupby("pid")["days_since_first"].cumsum().fillna(0)
    anthros.drop(columns=["days_since_first"], inplace=True)

    # prompt: for each pid in admit call weight_regress and add the first return value as 'weight_trend" and second as weight-rsquared columns in admit
    trend_df = pd.DataFrame(columns=["pid"])
    positive_pids = admit_weekly.loc[admit_weekly[detn_col] == True, "pid"].unique()
    # prompt: for each anthro_col in
    # prompt: for each anthro_col in 'weight_weekly','muac_weekly','hl_weekly','wfhz_weekly', 'hfaz_weekly', 'wfaz_weekly':
    for anthro_col in ["wfh", "hfa", "wfa", "weight", "muac", "hl"]:
        logger.debug(anthro_col)
        # prompt: for each pid in admit call regress and add the first return value as f'{anthro_col}_trend'" and second as f'{anthro_col}_rsquared columns in admit

        # Apply the function to each unique 'pid' and create new columns
        results = []
        # only recalculate the trends for the partial weeklies for the pids with the deterioriation
        for pid in tqdm(positive_pids):
            trend, r_squared = regress(anthros, pid, anthro_col)
            results.append(
                {"pid": pid, f"{anthro_col}_trend": trend, f"{anthro_col}_rsquared": r_squared}
            )

        # Convert the list of dictionaries to a DataFrame
        results_df = pd.DataFrame(results)

        # Merge the results back into the 'admit' DataFrame
        trend_df = pd.merge(trend_df, results_df, on="pid", how="right")
        logger.debug(trend_df.shape)

    # np.-inf breaks downstream models
    rsquared_columns = [col for col in trend_df.columns if col.endswith("_rsquared")]
    trend_df[rsquared_columns] = trend_df[rsquared_columns].replace(-np.inf, 0)
    # just re-use the full weekly for the negative pids, to save time
    # Filter admit_weekly for rows where pid is NOT in positive_pids and sequence_num is 1
    filtered_admit_weekly = admit_weekly[
        ~admit_weekly["pid"].isin(positive_pids) & (admit_weekly["sequence_num"] == 1)
    ].copy()
    filtered_admit_weekly.rename(
        columns={"weight_weekly": "weight", "muac_weekly": "muac"}, inplace=True
    )
    filtered_admit_weekly.rename(
        columns={"wfa_weekly": "wfa", "wfh_weekly": "wfh", "hfa_weekly": "hfa"}, inplace=True
    )
    trend_df = pd.concat(
        [
            filtered_admit_weekly[
                [
                    "pid",
                    "wfh_trend",
                    "wfh_rsquared",
                    "hfa_trend",
                    "hfa_rsquared",
                    "wfa_trend",
                    "wfa_rsquared",
                    "weight_trend",
                    "weight_rsquared",
                    "muac_trend",
                    "muac_rsquared",
                    "hl_trend",
                    "hl_rsquared",
                ]
            ],
            trend_df,
        ],
        ignore_index=True,
    )
    # prompt: get row count by pid in admit_weekly and append that column to admit
    # Group by 'pid' and count the number of rows for each 'pid'
    row_counts_by_pid = detn_prior.groupby("pid")["pid"].count()

    # Rename the 'pid' column to 'row_count'
    row_counts_by_pid = row_counts_by_pid.rename("row_count")

    # Merge the row counts back into the 'admit' DataFrame
    trend_df = pd.merge(trend_df, row_counts_by_pid, left_on="pid", right_index=True, how="left")

    return trend_df


def convert_recent_weeklies_to_series(detn_prior, num_of_visits=2, weekly_columns=weekly_columns):
    # Group by 'pid' and assign rank within each group based on 'sequence_num'
    detn_prior["reverse_sequence_num"] = detn_prior.groupby("pid")["sequence_num"].rank(
        method="dense", ascending=False
    )
    latest_visits = detn_prior[
        detn_prior["reverse_sequence_num"].isin(np.arange(1, num_of_visits + 1))
    ][weekly_columns]
    latest_visits.loc[
        (
            (latest_visits["final_numweeksback"] == 0)
            | (
                (latest_visits["final_numweeksback"] > 1)
                & (latest_visits["final_numweeksback"] < 2)
            )
        ),
        "final_numweeksback",
    ] = 1
    latest_visits["final_numweeksback"] = latest_visits["final_numweeksback"].fillna(1)
    # Replace NaN values with 1 as values are only 1 and 2
    latest_visits.sort_values(by=["pid", "sequence_num"], ascending=[True, False], inplace=True)
    # make wk1 the most recent week
    visit_series = (
        latest_visits.assign(col=latest_visits.groupby("pid").cumcount() + 1)
        .set_index(["pid", "col"])
        .unstack("col")
        .sort_index(level=(1, 0), axis=1)
    )
    visit_series.columns = [f"wk{y}_{x}" for x, y in visit_series.columns]
    # prompt: make visit_series.index a column named 'pid'
    visit_series = visit_series.reset_index()
    return visit_series


def remove_active_most_recent_weekly(admit_weekly):
    # prompt: get admit_weekly unique pids with status=='active'
    recent_pids = admit_weekly[(admit_weekly["status"] == "active")]["pid"].unique()

    # prompt: delete the most recent calcdate_weekly from admit_weekly where pid in recent_pids
    # Group by pid and find the maximum calcdate_weekly for each pid in recent_pids
    max_calcdate_weekly = (
        admit_weekly[admit_weekly["pid"].isin(recent_pids)].groupby("pid")["calcdate_weekly"].max()
    )

    # Merge the maximum calcdate_weekly back into the original dataframe
    admit_weekly = admit_weekly.merge(
        max_calcdate_weekly.rename("max_calcdate_weekly"),
        left_on="pid",
        right_index=True,
        how="left",
    )

    # Filter out rows with calcdate_weekly equal to the maximum for each pid in recent_pids
    rows_to_delete = admit_weekly[
        (admit_weekly["pid"].isin(recent_pids))
        & (admit_weekly["calcdate_weekly"] == admit_weekly["max_calcdate_weekly"])
    ]

    # Delete the rows
    admit_weekly = admit_weekly.drop(rows_to_delete.index)

    # Drop the temporary 'max_calcdate_weekly' column
    admit_weekly = admit_weekly.drop("max_calcdate_weekly", axis=1)
    return admit_weekly


# get the admittance date, weight and muac
admit = admit_weekly[
    [
        "pid",
        "calcdate_admit_current",
        "weight_admit_current",
        "muac_admit_current",
        "hl_admit",
        "wfh_admit_current",
        "hfa_admit_current",
        "wfa_admit_current",
    ]
].drop_duplicates(subset=["pid"], keep="last")

# make the admit columns look like the weekly ones
admit.rename(
    columns={
        "calcdate_admit_current": "calcdate_weekly",
        "weight_admit_current": "weight",
        "muac_admit_current": "muac",
        "hl_admit": "hl",
        "wfa_admit_current": "wfa",
        "hfa_admit_current": "hfa",
        "wfa_admit_current": "wfa",
    },
    inplace=True,
)


# prompt: keep the most recent num_recent-most calcdate_weekly from admit_weekly groupby('pid') and pid in recent_pids


def remove_recent_weeklies(admit_weekly, recent_pids, num_recent=4):
    """Removes the most recent weekly entries for each pid.

    Args:
        admit_weekly: DataFrame.
        num_recent: The number of recent entries to remove.
        recent_pids: List of PIDs for which to remove recent entries.

    Returns:
        DataFrame: Modified DataFrame with recent entries removed.
    """

    # Group by 'pid' and rank the rows by 'calcdate_weekly' in descending order.
    admit_weekly["rank"] = (
        admit_weekly[admit_weekly["pid"].isin(recent_pids)]
        .groupby("pid")["calcdate_weekly"]
        .rank(method="dense", ascending=False)
    )

    # Identify rows to delete (most recent num_recent entries)
    rows_to_delete = admit_weekly[admit_weekly["rank"] <= num_recent]

    # Drop the identified rows
    admit_weekly = admit_weekly.drop(rows_to_delete.index)
    admit_weekly.drop(columns=["rank"], inplace=True)
    return admit_weekly


# Example usage (assuming recent_pids is defined):
# admit_weekly = remove_recent_weeklies(admit_weekly, recent_pids)


def weekly_agg(detn_prior, admit):
    anthros = pd.concat(
        [
            detn_prior[["pid", "calcdate_weekly", "weight", "muac", "hl", "wfh", "hfa", "wfa"]],
            admit,
        ],
        ignore_index=True,
    )
    # prompt: sort anthros by pid, calcdate_weekly

    # Sort the 'anthros' DataFrame by 'pid' and then 'calcdate_weekly' so admittance row is first for each pid
    anthros = anthros.sort_values(by=["pid", "calcdate_weekly"])

    weekly_agg = anthros.groupby("pid").agg(
        weekly_row_count=("pid", "count"),
        weekly_first_admit=("calcdate_weekly", "first"),
        weekly_last_admit=("calcdate_weekly", "last"),
        weekly_last_muac=("muac", "last"),
        weekly_first_muac=("muac", "first"),
        weekly_avg_muac=("muac", "mean"),
        weekly_first_weight=("weight", "first"),
        weekly_last_weight=("weight", "last"),
        weekly_avg_weight=("weight", "mean"),
        weekly_first_hl=("hl", "first"),
        weekly_last_hl=("hl", "last"),
        weekly_min_hl=("hl", "min"),
        weekly_max_hl=("hl", "max"),
        weekly_avg_hl=("hl", "mean"),
        weekly_first_wfh=("wfh", "first"),
        weekly_last_wfh=("wfh", "last"),
        weekly_min_wfh=("wfh", "min"),
        weekly_max_wfh=("wfh", "max"),
        weekly_avg_wfh=("wfh", "mean"),
        weekly_first_hfa=("hfa", "first"),
        weekly_last_hfa=("hfa", "last"),
        weekly_min_hfa=("hfa", "min"),
        weekly_max_hfa=("hfa", "max"),
        weekly_avg_hfa=("hfa", "mean"),
        weekly_first_wfa=("wfa", "first"),
        weekly_last_wfa=("wfa", "last"),
        weekly_min_wfa=("wfa", "min"),
        weekly_max_wfa=("wfa", "max"),
        weekly_avg_wfa=("wfa", "mean"),
    )

    weekly_agg["muac_diff"] = weekly_agg["weekly_last_muac"] - weekly_agg["weekly_first_muac"]
    weekly_agg["weight_diff"] = weekly_agg["weekly_last_weight"] - weekly_agg["weekly_first_weight"]
    weekly_agg["calcdate_diff"] = weekly_agg["weekly_last_admit"] - weekly_agg["weekly_first_admit"]
    weekly_agg["calcdate_diff"] = weekly_agg["calcdate_diff"].dt.total_seconds() / (24 * 60 * 60)
    weekly_agg["hl_diff"] = weekly_agg["weekly_last_hl"] - weekly_agg["weekly_first_hl"]
    weekly_agg["wfh_diff"] = weekly_agg["weekly_last_wfh"] - weekly_agg["weekly_first_wfh"]
    weekly_agg["hfa_diff"] = weekly_agg["weekly_last_hfa"] - weekly_agg["weekly_first_hfa"]
    weekly_agg["wfa_diff"] = weekly_agg["weekly_last_wfa"] - weekly_agg["weekly_first_wfa"]

    weekly_agg["weight_diff_ratio"] = weekly_agg["weight_diff"] / weekly_agg["weekly_first_weight"]
    weekly_agg["weight_diff_ratio_rate"] = (
        weekly_agg["weight_diff_ratio"] / weekly_agg["calcdate_diff"]
    )
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

    return weekly_agg


admit_current_mh = convert_bool_to_int(admit_current_mh)


def only_rows_before_detn(detn, detn_col):
    # Get unique PIDs where 'detn_col' is True in the detn DataFrame
    detn_ever_pids = detn.loc[detn[detn_col] == True, "pid"].unique()
    # Find PIDs in 'detn' that are NOT in 'detn_ever_pids'
    pids_not_in_ever_pids = detn.loc[~detn["pid"].isin(detn_ever_pids), "pid"].unique()

    # prompt: remove rows with sequence_number >= first detn_ever group by pid

    # Group by 'pid' and find the first occurrence of 'detn_ever' == True
    if detn_col == "nonresponse":
        first_detn_ever = (
            detn.loc[detn[detn_col] == True].groupby("pid")["sequence_num"].max().reset_index()
        )
    else:
        first_detn_ever = (
            detn.loc[detn[detn_col] == True].groupby("pid")["sequence_num"].min().reset_index()
        )

    # prompt: get admit_weekly[y_cat1] for max sequence_number by pid

    # Get admit_weekly[y_cat1] for max sequence_number by pid
    y_cat1_copy = y_cat1.copy()
    y_cat1_copy.insert(0, "pid")

    # Rename the 'sequence_number' column to 'first_detn_seq' for clarity
    first_detn_ever = first_detn_ever.rename(columns={"sequence_num": "first_detn_seq"})

    # detn.drop(columns=['first_detn_seq'], inplace=True)
    # Merge the 'first_detn_seq' back into the original DataFrame
    detn = pd.merge(detn, first_detn_ever, on="pid", how="left")

    y_detn_cat1 = detn[detn["sequence_num"] == detn["first_detn_seq"]][y_cat1_copy].copy()

    # Filter out rows where 'sequence_number' is greater than or equal to 'first_detn_seq'
    # max_sequence_rows = detn.loc[detn.groupby('pid')['sequence_num'].idxmax()]

    # Filter out rows where 'sequence_number' is greater than or equal to 'first_detn_seq'

    seq_ct = 0
    if detn_col == "nonresponse":
        # for nonresponse, discard the few rows before the event happened to discourage the model from keying on los or duration
        seq_ct = 3

    detn_prior = detn[
        ((detn["sequence_num"] + seq_ct) < detn["first_detn_seq"])
        & (detn["pid"].isin(detn_ever_pids))
    ].copy()

    # detn_prior contains only rows before the first deterioration for each patient plus all non-deteriorated patients with all their rows
    # Concatenate detn_prior and detn.loc[detn['detn_ever'] == False]
    detn_prior = pd.concat([detn_prior, detn[~detn["pid"].isin(detn_ever_pids)]])
    # clean up the working column
    detn.drop(columns=["first_detn_seq"], inplace=True)

    # Create a pandas Series where the index is detn_ever_pids and the value is True
    detn_ever_pids_series = pd.Series(index=detn_ever_pids, data=True)

    pids_not_in_ever_pids = pd.Series(index=pids_not_in_ever_pids, data=False)
    # Concatenate the two Series
    y_detn = pd.concat([detn_ever_pids_series, pids_not_in_ever_pids])
    # Rename the Series
    y_detn.name = detn_col

    y_detn_cat1 = pd.merge(y_detn_cat1, y_detn, how="right", left_on="pid", right_on=y_detn.index)

    y_detn_cat1.fillna(False, inplace=True)
    for col in y_cat1:
        y_detn_cat1[col] = y_detn_cat1[col].astype(int)
    return detn_prior, y_detn, y_detn_cat1


def prepare_export(detn_col="new_onset_medical_complication"):
    # get rows prior to the deterioration
    y_detn_cat1 = pd.DataFrame()

    recent_pids = admit_weekly[(admit_weekly["status"] == "active")]["pid"].unique()
    if detn_col in ["new_onset_medical_complication"]:
        # remove the row that may have the deterioration we want to predict, if pid is currently active
        detn_prior, y_detn, y_detn_cat1 = only_rows_before_detn(
            remove_recent_weeklies(admit_weekly, recent_pids, num_recent=1), detn_col
        )
    elif detn_col in ["oedema_not_disappearing"]:
        # remove the row that may have the deterioration we want to predict, if pid is currently active
        detn_prior, y_detn, _ = only_rows_before_detn(
            remove_recent_weeklies(admit_weekly, recent_pids, num_recent=1), detn_col
        )
    elif detn_col == "muac_loss_2_weeks_consecutive":
        # remove the 2 rows that may have the deterioration we want to predict, if pid is currently active
        detn_prior, y_detn, _ = only_rows_before_detn(
            remove_recent_weeklies(admit_weekly, recent_pids, num_recent=2), detn_col
        )
    elif detn_col == "detn_weight_loss_ever":
        # remove the 4 rows that may have the deterioration we want to predict, if pid is currently active
        detn_prior, y_detn, _ = only_rows_before_detn(
            remove_recent_weeklies(admit_weekly, recent_pids, num_recent=4), detn_col
        )
    elif detn_col == "status_dead":
        y_detn = pd.Series(index=admit_weekly_all["pid"].unique(), dtype=bool)
        y_detn[:] = 0  # Initialize all values to False
        y_detn[pids_dead] = 1
        y_detn.rename(detn_col, inplace=True)
        # remove no rows as death status is set from current status
        detn_prior = admit_weekly_all.copy()
    elif detn_col == "nonresponse":
        # y_detn = pd.Series(index=admit_weekly_all['pid'].unique(), dtype=bool)
        # y_detn[:] = 0  # Initialize all values to False
        # y_detn[pids_nonresponse] = 1
        # y_detn.rename(detn_col,inplace=True)
        # remove no rows as nonresponse is set from current status
        # detn_prior = admit_weekly_all.copy()
        detn_prior, y_detn, _ = only_rows_before_detn(
            remove_recent_weeklies(admit_weekly, recent_pids, num_recent=4), detn_col
        )

    else:
        detn_prior, y_detn, _ = only_rows_before_detn(admit_weekly, detn_col)

    # get weekly aggregate stats
    detn_prior.rename(columns={"weight_weekly": "weight", "muac_weekly": "muac"}, inplace=True)
    detn_prior.rename(
        columns={"wfa_weekly": "wfa", "wfh_weekly": "wfh", "hfa_weekly": "hfa"}, inplace=True
    )
    detn_prior.sort_values(by=["pid", "calcdate_weekly"], inplace=True)
    weekly_agg_stats = weekly_agg(detn_prior, admit)
    detn_prior.rename(columns={"weight_weekly": "weight", "muac_weekly": "muac"}, inplace=True)
    detn_prior.rename(
        columns={"wfa_weekly": "wfa", "wfh_weekly": "wfh", "hfa_weekly": "hfa"}, inplace=True
    )
    # get trend for those rows
    trend_stats = trend(detn_prior, admit_weekly, admit, detn_col)
    visit_series = convert_recent_weeklies_to_series(
        detn_prior, num_of_visits=3, weekly_columns=weekly_columns
    )
    export = pd.merge(
        admit_raw, visit_series, on="pid", how="left"
    )  # no overlap so suffix isn't used
    # add weekly stats columns to admit_raw
    export = pd.merge(export, weekly_agg_stats, on="pid", how="left")
    # add trends to admit_raw
    export = pd.merge(export, trend_stats, on="pid", how="left")

    # Merge with admit_raw cat2_sum_by_pid based on the 'pid' column
    export = pd.merge(export, cat1_sum_by_pid, on="pid", how="left")
    export = pd.merge(export, cat2_sum_by_pid, on="pid", how="left")

    # get weekly cat1, cat2 counts up to deterioration
    numeric_cols = detn_prior.select_dtypes(include=["number", "bool"]).columns
    numeric_cat1_cols = [col for col in numeric_cols if col.startswith("cat1_")]
    numeric_cat2_cols = [col for col in numeric_cols if col.startswith("cat2_")]

    cat1_sum_by_pid_weekly, cat2_sum_by_pid_weekly = count_cat1_cat2(
        detn_prior, numeric_cat1_cols, numeric_cat2_cols
    )
    # cat1_sum_by_pid_weekly, cat2_sum_by_pid_weekly = count_cat1_cat2(detn_prior, cat1_weekly_cols, cat2_weekly_cols)

    export = pd.merge(export, cat1_sum_by_pid_weekly, on="pid", how="left")
    export = pd.merge(export, cat2_sum_by_pid_weekly, on="pid", how="left")

    # prompt: filter export where pid in pids_with_visits
    # as deterioration by definition requires us to look at a change since admission
    # export = export[export['pid'].isin(pids_with_visits)]

    # prompt: find columns that are single value and nonnull, then drop them

    single_value_cols = [
        col for col in export.columns if export[col].nunique() == 1 and export[col].notna().all()
    ]

    export.drop(columns=single_value_cols, inplace=True)
    convert_3val_bool(export, len(export))
    convert_to_bool(export)
    boolean_columns = export.select_dtypes(include=["bool"]).columns
    # Convert boolean columns to numeric
    for col in boolean_columns:
        export[col] = export[col].astype(int)
    export = infer_phq_score(admit_current_mh, admit_current, export)

    return export, y_detn, y_detn_cat1


deterioration_types = [
    "detn_weight_loss_ever",
    "new_onset_medical_complication",
    "muac_loss_2_weeks_consecutive",
    "oedema_not_disappearing",
    "nonresponse",
    "status_dead",
]


def get_first_detn_date(admit_weekly, variable, date_col="calcdate_weekly"):
    # Group by 'pid' and filter for 'new_onset_medical_complication' == True
    filtered_df = admit_weekly[admit_weekly[variable] == True].groupby("pid")
    # 'status_date' for nonresponse variable
    # Get the minimum 'calcdate_weekly' for each group
    min_calcdate = filtered_df[date_col].min()

    min_calcdate.rename(f"{variable}_date", inplace=True)
    min_calcdate = min_calcdate.reset_index()

    return min_calcdate


for col in deterioration_types:
    logger.debug(col)
    export, y_detn, y_detn_cat1 = prepare_export(detn_col=col)

    # get date of when deterioration first occurred and set it (for hazard analysis)
    if col in ["nonresponse", "status_dead"]:
        first_detn_date = get_first_detn_date(admit_weekly_all, col, "status_date")
    else:
        first_detn_date = get_first_detn_date(admit_weekly, col, "calcdate_weekly")

    # first_detn_date= get_first_detn_date(admit_weekly,col,date_col)
    export = pd.merge(export, first_detn_date, on="pid", how="left")
    # prompt: add series y_detn_ever as column to admit_raw
    # do inner join which discards patients w/no visit as their deterioration status needs to be decided still,
    # TODO probably could include death cases
    # current = pd.read_csv(dir+"train_pba_current_processed_2024-11-02.csv")

    # just include all pids
    detn_ever_pids = admit_weekly.loc[admit_weekly[col] == True, "pid"].unique()
    detn_ever_pids_series = pd.Series(index=detn_ever_pids, data=True)
    pids_not_in_ever_pids = admit_weekly.loc[
        ~admit_weekly["pid"].isin(detn_ever_pids), "pid"
    ].unique()
    pids_not_in_ever_pids_series = pd.Series(index=pids_not_in_ever_pids, data=False)
    # Concatenate the two Series
    y_detn_all = pd.concat([pids_not_in_ever_pids_series, detn_ever_pids_series])
    # Rename the Series
    y_detn_all.name = col
    logger.debug(y_detn_all.sum())
    if col == "new_onset_medical_complication":
        export = export.merge(y_detn_cat1, on="pid", how="left")
    elif col in ["nonresponse", "status_dead"]:
        y_detn.name = col
        export = export.merge(y_detn, left_on="pid", right_index=True, how="left")
    else:
        export = export.merge(y_detn_all, left_on="pid", right_index=True, how="left")
    export = export.replace(-np.inf, 0)
    export[col].fillna(False, inplace=True)
    export[col] = export[col].astype(int)
    export["row_count"].fillna(0, inplace=True)
    export["weekly_row_count"].fillna(0, inplace=True)
    logger.debutg(f'{export.shape}, {export["pid"].nunique()}, {export[col].sum()}')
    with open(dir + f"analysis/{col}.pkl", "wb") as f:
        pickle.dump(export, f)

