# data_loader.py


import os
import re
from datetime import date

import gspread
import numpy as np
import pandas as pd
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from gspread_dataframe import get_as_dataframe
from pygrowup_erknet import Calculator
from pyodk.client import Client
from sqlalchemy import create_engine, text
from memory_profiler import profile

load_dotenv()

TRAIN= os.getenv("TRAIN", "False").lower() == "true"


# --- PostgreSQL Connection ---
DB_NAME = "cmam"
DB_HOST = "taimaka-internal.org"
DB_PORT = "5432"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ODK_USERNAME = os.getenv("ODK_USERNAME")
ODK_PASSWORD = os.getenv("ODK_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# --- Load DataFrames from SQL ---
def load_table(schema, table):
    query = text(f'SELECT * FROM "{schema}"."{table}"')
    return pd.read_sql(query, con=engine)


current_df = load_table("data", "current")
current_pids = current_df["pid"].dropna().unique().tolist()
admit = load_table("data", "dict_clean").drop(columns=["b_assignedtocct"], errors="ignore")
weekly = load_table("data", "weekly_clean")
mh = load_table("data", "mmh_dict")
mh_pids = mh["pid"].dropna().unique().tolist()
deaths_df = load_table("data", "deaths")
relapse = load_table("data", "relapse_dict")
relapse_pids = relapse["pid"].dropna().unique().tolist()

# --- Manual Exclusions Filtering ---
exclusions_raw = load_table("data", "manual_exclusions")
exclusions = (
    exclusions_raw.sort_values(["pid", "dt", "change_timestamp"], ascending=[True, False, True])
    .drop_duplicates(subset=["pid"], keep="first")
    .reset_index(drop=True)
)

# odk_loader.py

client = Client(config_path="odk.toml")


ODK_URL = "https://taimaka-internal.org:7443"

def load_admit_raw():
    # Get submissions from ODK Central
    data = client.submissions.get_table(form_id="admit", project_id=9)

    # Flatten JSON data into a dataframe
    df = pd.json_normalize(data["value"])
    df = df.dropna(how="all", axis=1)  # Drop columns that are all NaN
    df = df.rename(columns={k: k.split(".")[-1] for k in df.columns})

    # Strip "uuid:" prefix from `id`
    df["uuid"] = df["instanceID"].str.replace("^uuid:", "", regex=True)

    return df


admit_raw_df = load_admit_raw()

# pid_assignment.py


# pid_map = list of dictionaries with 'name', 'todate', and 'pid'
manual_pid_assignments = [
    {"name": "Badariya hashimu ", "todate": "2023-05-31", "pid": "23-0211"},
    {"name": "Al amin sidi ali ", "todate": "2023-05-31", "pid": "23-0210"},
    {"name": "Ibrahim sadiq", "todate": "2023-05-31", "pid": "23-0203"},
    {"name": "Abdullazeez Rufai", "todate": "2023-05-31", "pid": "23-0212"},
    {"name": "Abubakar sunusi ", "todate": "2023-05-31", "pid": "23-0204"},
    {"name": "Farida ali", "todate": "2023-05-31", "pid": "23-0205"},
    {"name": "Muhammadu Faisal samaila", "todate": "2023-05-31", "pid": "23-0206"},
    {"name": "Yusuf auwal ", "todate": "2023-05-31", "pid": "23-0202"},
    {"name": "Salma adamu ", "todate": "2023-05-31", "pid": "23-0201"},
    {"name": "Hussaini sulaiman ", "todate": "2023-06-05", "pid": "23-0219"},
    {"name": "Kamaluddeen muhammadu", "todate": "2023-06-08", "pid": "23-0281"},
    {"name": "Muhammad Muhammad ", "todate": "2023-07-03", "pid": "23-0431"},
    {"name": "Hafiz sanusi", "todate": "2023-07-04", "pid": "23-0295"},
    {"name": "Muhammad abdullahi ", "todate": "2023-07-04", "pid": "23-0428"},
    {"name": "Marsiya abubakar", "todate": "2023-05-31", "pid": "23-0214"},
    {"name": "Saadatu zahradin", "todate": "2023-06-02", "pid": "23-0286"},
    {"name": "Abubakar Khalid", "todate": "2023-07-03", "pid": "23-0390"},
    {"name": "Nura ayayaji", "todate": "2023-07-05", "pid": "23-0429"},
    {"name": "Usaina nasiru ", "todate": "2023-07-11", "pid": "23-0868"},
    {"name": "Shaaban Muhammad ", "todate": "2023-07-11", "pid": "23-0545"},
    {"name": "Aishatu abdullahi ", "todate": "2023-07-12", "pid": "23-0435"},
    {"name": "Fatima Abdulhamid ", "todate": "2023-09-25", "pid": "23-1404"},
    {"name": "Asmau Mohammed ", "todate": "2023-10-31", "pid": "23-3160"},
    {"name": "Ibrahim Ibrahim ", "todate": "2023-10-03", "pid": "23-0870"},
    {"name": "Aisha Babangida", "todate": "2023-10-26", "pid": "23-1420"},
    {"name": "Balkisu isah ", "todate": "2023-11-16", "pid": "23-0872"},
    {"name": "Yasir lawan", "todate": "2023-11-16", "pid": "23-0871"},
    {"name": "Muhammad abdussalam", "todate": "2024-01-08", "pid": "24-0366"},
    {"name": "Sulaiman Yahaya ", "todate": "2023-11-28", "pid": "23-3088"},
    {"name": "Abdullahi Mohammed ", "todate": "2023-12-04", "pid": "23-1419"},
    {"name": "Salisu musa", "todate": "2024-01-23", "pid": "24-1415"},
    {"name": "Usman Adamu ", "todate": "2024-02-14", "pid": "24-0851"},
    {"name": "Aishatu Bello", "todate": "2024-02-27", "pid": "24-0865"},
    {"name": "Adamu Abdullahi ", "todate": "2024-02-27", "pid": "24-1063"},
    {"name": "Basiru Muhammad ", "todate": "2024-02-29", "pid": "24-0854"},
    {"name": "Abdulkadir muhd ", "todate": "2024-03-11", "pid": "24-0853"},
    {"name": "Ahmadu Muhammad ", "todate": "2024-03-14", "pid": "24-0858"},
    {"name": "Zulai Adamu ", "todate": "2024-03-18", "pid": "24-0861"},
    {"name": "Habiba Bello", "todate": "2024-03-18", "pid": "24-0859"},
    {"name": "Adamu Ibrahim ", "todate": "2024-03-21", "pid": "24-0860"},
    {"name": "Aisha Hussaini", "todate": "2024-04-04", "pid": "24-1228"},
    {"name": "Amina muhammadu ", "todate": "2024-04-15", "pid": "24-1227"},
    {"name": "Samaila Muhammadu ", "todate": "2024-04-18", "pid": "24-1524"},
    {"name": "Muhammad Musa ", "todate": "2024-03-11", "pid": "24-1518"},
    {"name": "Aisha Yusuf ", "todate": "2024-05-07", "pid": "24-1937"},
    {"name": "Musa Adamu ", "todate": "2024-05-13", "pid": "24-1954"},
    {"name": "hussaina babangida", "todate": "2024-03-18", "pid": "24-0980"},
    {"name": "Abdul mudallab auwal", "todate": "2024-03-25", "pid": "24-0981"},
    {"name": "Ainau ayuba", "todate": "2024-05-06", "pid": "24-1606"},
    {"name": "Habiba Abubakar", "todate": "2024-04-26", "pid": "24-1942"},
    {"name": "Muhd Adamu", "todate": "2024-04-26", "pid": "24-1935"},
    {"name": "Abdullahi Usman ", "todate": "2024-04-29", "pid": "24-1938"},
    {"name": "Musa Yahaya", "todate": "2024-05-09", "pid": "24-1758"},
    {"name": "Maryama Muhd", "todate": "2024-05-15", "pid": "24-1958"},
    {"name": "Abubakar muhammadu ", "todate": "2024-07-05", "pid": "24-1607"},
    {"name": "Muhammad Musa ", "todate": "2024-04-22", "pid": "24-1518"},
    {"name": "Amina haruna", "todate": "2024-05-23", "pid": "24-1950"},
    {"name": "Adamu jauro", "todate": "2024-05-27", "pid": "24-1956"},
    {"name": "Muhammad Muhammad ", "todate": "2024-06-05", "pid": "24-2128"},
    {"name": "Aisha Abdussalam", "todate": "2024-06-13", "pid": "24-1957"},
    {"name": "Aliyu Abubakar ", "todate": "2024-06-19", "pid": "24-1944"},
    {"name": "Saidu Musa", "todate": "2024-06-26", "pid": "24-2752"},
    {"name": "Amina Abdullahi ", "todate": "2024-06-27", "pid": "24-2751"},
    {"name": "Hussaina Musa ", "todate": "2024-07-01", "pid": "24-2756"},
    {"name": "Maryam Muhammad ", "todate": "2024-07-01", "pid": "24-2757"},
    {"name": "Hassana Saidu ", "todate": "2024-07-08", "pid": "24-2830"},
    {"name": "Aisha Ahmadu ", "todate": "2024-08-14", "pid": "24-2761"},
    {"name": "Abubakar Ibrahim ", "todate": "2024-09-10", "pid": "24-4137"},
    {"name": "Hassana Wada ", "todate": "2024-09-25", "pid": "24-2753"},
    {"name": "Muhd muhd", "todate": "2024-09-27", "pid": "24-4147"},
    {"name": "Abdullahi Muhd", "todate": "2024-07-22", "pid": "24-2763"},
    {"name": "Hauwa Muhammad ", "todate": "2024-09-30", "pid": "24-2765"},
    {"name": "Ali Adamu", "todate": "2023-07-03", "pid": "23-9999"},
    {"name": "Muhammad lawanda ", "todate": "2023-07-04", "pid": "23-9998"},
    {"name": "Laure Adamu ", "todate": "2023-07-14", "pid": "23-9997"},
    {"name": "Aisha Umaru ", "todate": "2024-01-31", "pid": "24-9999"},
    {"name": "Zainab Tijjani ", "todate": "2024-10-21", "pid": "24-9991"},
    {"name": "Habiba Muhd", "todate": "2023-10-02", "pid": "23-9996"},
    {"name": "Muhammad Muhammad ", "todate": "2023-11-20", "pid": "23-9995"},
    {"name": "Aisha Muhammad", "todate": "2024-03-25", "pid": "24-9997"},
    {"name": "Fatu Magaji ", "todate": "2024-07-17", "pid": "24-9996"},
    {"name": "Bello Umar", "todate": "2024-07-19", "pid": "24-9995"},
    {"name": "Hassan umaru ", "todate": "2024-09-09", "pid": "24-9994"},
    {"name": "Mommy Ibrahim", "todate": "2024-10-17", "pid": "24-9993"},
    {"name": "Fatima Rabiu ", "todate": "2024-09-02", "pid": "24-9992"},
    {"name": "", "todate": "", "pid": "24-9989"},  # From UUID match
    {"name": "", "todate": "", "pid": "24-9988"},  # From UUID match
]

@profile
def assign_pids(df, assignments):
    df["todate"] = pd.to_datetime(df["todate"], errors="coerce")
    df["pid"] = df["pid"].astype("string")

    for row in assignments:
        condition = (
            (df["pid"].isna())
            & (df["name"] == row["name"])
            & (df["todate"] == pd.to_datetime(row["todate"]))
        )
        df.loc[condition, "pid"] = row["pid"]

    return df


admit_raw = assign_pids(admit_raw_df, manual_pid_assignments)


# current_extension.py

def extend_current(current, admit_raw_df):
    new_pids = [
        "23-9999",
        "23-9998",
        "23-9997",
        "24-9999",
        "24-9991",
        "24-9998",
        "24-9990",
        "23-9996",
        "23-9995",
        "24-9997",
        "24-9996",
        "24-9995",
        "24-9994",
        "24-9993",
        "24-9992",
        "24-9989",
        "24-9988",
    ]

    add_current = pd.DataFrame(
        {
            "pid": new_pids,
            "phone": [
                "701-729-1416",
                None,
                "903-395-7213",
                "902-236-5364",
                None,
                "803-769-9436",
                None,
                None,
                "806-380-3885",
                "708-720-7492",
                "706-611-7182",
                "903-754-3375",
                "802-670-5817",
                None,
                "901-131-0883",
                "704-738-1330",
                "703-379-5483",
            ],
            "b_phoneconsent": [None] * 17,
            "langpref": [None] * 17,
            "phoneowner": [None] * 17,
            "site": [
                "sangaru",
                "kuri",
                "jalingo",
                "sangaru",
                "kuri",
                "kurjale",
                None,
                "sangaru",
                "sangaru",
                "sangaru",
                "sangaru",
                "sangaru",
                "sangaru",
                "sangaru",
                "jalingo",
                "kuri",
                "sangaru",
            ],
            "status": [
                "dead",
                "dead",
                "excluded",
                "dead",
                "dead",
                "default",
                "excluded",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "dead",
                "default",
            ],
            "status_detail": [
                None,
                None,
                "defect",
                None,
                None,
                "final",
                "other",
                "final",
                "final",
                "final",
                "final",
                "final",
                "final",
                "final",
                "final",
                None,
                "returnable",
            ],
            "movement_detail": [None] * 17,
            "status_date": pd.to_datetime(
                [
                    "2023-07-05",
                    "2023-07-04",
                    "2023-07-26",
                    "2024-02-02",
                    "2024-10-23",
                    "2024-03-04",
                    "2024-03-13",
                    "2023-10-23",
                    "2023-12-11",
                    "2024-03-15",
                    "2024-07-05",
                    "2024-07-05",
                    "2024-10-07",
                    "2024-11-11",
                    "2024-09-30",
                    "2024-12-05",
                    "2024-12-09",
                ]
            ),
            "nvdate": [None] * 17,
            "los": [0] * 17,
            "dischqualanthro": [False] * 17,
        }
    )

    # Join to get `em_age` and `todate` from admit_raw
    emage = admit_raw_df[admit_raw_df["pid"].isin(new_pids)][["em_age", "pid", "todate"]]
    emage["todate"] = pd.to_datetime(emage["todate"], errors="coerce")

    add_current = add_current.merge(emage, on="pid", how="left")

    # Manually assign missing values
    add_current.loc[add_current["pid"] == "24-9998", "em_age"] = 2
    add_current.loc[add_current["pid"] == "24-9990", "em_age"] = 10
    add_current.loc[add_current["pid"] == "24-9998", "todate"] = pd.to_datetime("2024-02-15")
    add_current.loc[add_current["pid"] == "24-9990", "todate"] = pd.to_datetime("2024-03-04")

    # Calculate age in months
    today = pd.to_datetime(date.today())
    add_current["age"] = add_current["em_age"].fillna(0).astype(int) + (
        (today - add_current["todate"]).dt.days // 30
    )

    return pd.concat(
        [current, add_current.drop(columns=["em_age", "todate"])], ignore_index=True
    )

def extend_deaths(deaths_df):
    add_deaths = pd.DataFrame(
        {
            "pid": ["23-9999", "23-9998", "24-9999", "24-9991", "24-9989"],
            "dod": pd.to_datetime(
                ["2023-07-05", "2023-07-04", "2024-02-02", "2024-10-23", "2024-12-05"]
            ),
            "notes": [
                "Per ITP, patient died on transit to FTH (tertiary facility). Died in ITP care, before PID assigned; PID 23-9999 assigned retroactively by Jenn during data cleaning.",
                "Per ITP, patient died on arrival to ITP, before PID was assigned. PID 23-9998 assigned retroactively by Jenn during data cleaning.",
                "Died in ITP before PID assigned; PID 24-9999 assigned retroactively by Jenn during data cleaning.",
                "Died in ITP before PID assigned; PID 24-9991 assigned retroactively by Jenn during data cleaning.",
                "Died in ITP before PID assigned; PID 24-9989 assigned retroactively by Jenn during data cleaning.",
            ],
            "change_timestamp": [None] * 5,
        }
    )

    return pd.concat([deaths_df, add_deaths], ignore_index=True)


# Assuming you've already loaded `current`, `admit_raw`, `exclusions`, `deaths`...

# from current_extension import extend_current, extend_exclusions, extend_deaths

current_df = extend_current(current_df, admit_raw_df)
# exclusions = extend_exclusions(exclusions)
deaths_df = extend_deaths(deaths_df)


# merge_notes.py
# make sure to have pip install gspread oauth2client pandas gspread_dataframe

def merge_notes(current_df, exclusions_df, deaths_df, new_pids):
    df = current_df.merge(exclusions_df[["pid", "notes"]], on="pid", how="left").rename(
        columns={"notes": "exclusion_notes"}
    )

    df = df.drop(columns=["updt_id", "change_timestamp", "dt"], errors="ignore")

    df = df.merge(deaths_df[["pid", "notes"]], on="pid", how="left").rename(
        columns={"notes": "death_notes"}
    )

    df = df.drop(columns=["change_timestamp", "dod"], errors="ignore")

    df["cleaning_note"] = df["pid"].apply(
        lambda pid: "PID assigned retroactively during data cleaning." if pid in new_pids else None
    )

    return df

def add_rows_to_admit_raw(admit_raw_df):
    add_admit_raw = pd.DataFrame(
        {
            "pid": ["24-9998", "24-9990"],
            "todate": pd.to_datetime(["2024-02-15", "2024-03-04"]),
            "site": ["gh_deba", "gh_deba"],
            "site_type": ["itp", "itp"],
            "b_cpalate": ["false", "false"],
            "b_hydrocephalus": ["false", "false"],
            "b_prolap_hernia": ["false", "false"],
            "b_downsynd": ["false", "false"],
            "b_sicklecell": ["false", "false"],
            "b_def_oth": ["false", "false"],
            "b_fract_traum": ["false", "false"],
            "def_pres": ["false", "false"],
            "state": ["gombe", "other"],
            "other_state": [None, "unknown"],
            "name": ["Saifullahi Yahaya", "Abdullahi Ismail"],
            "c_sex": ["male", "male"],
            "age": [2, 10],
            "muac": [7.2, 10.0],
            "muac_status": ["sam", "sam"],
            "maln_status": ["sam", "sam"],
            "eref_u6sam": ["true", "false"],
            "ref_overall": ["true", "true"],
            "eff_eref_overall": ["true", None],
            "eff_ref_overall": ["true", "true"],
            "q_override_eref": ["false", None],
            "q_cgaccept_eref": ["true", None],
            "override_eref": ["false", None],
            "b_correctadmittype": ["true", "true"],
            "precalc_admit_type_pp": ["New Admission", "New Admission"],
            "admit_type": ["new", "new"],
            "q_override_lref": [None, "false"],
            "override_lref": [None, "false"],
            "q_cgaccept_lref": [None, "true"],
            "eff_lref_overall": [None, "true"],
            "cat1_overall": [None, "true"],
            "id": ["fc41d7c0-1803-400b-81da-40d96f747497", "4e0d9d40-e3b5-45db-8584-4b271bbd7dd9"],
            "additionalnotes": [
                "Presented in ITP and discharged as stabilized. Not assigned PID b/c the ITP stocked out of admission forms...",
                "Presented in ITP. Treated, then excluded due to being from outside the catchment area...",
            ],
        }
    )

    # Ensure consistency of types
    add_admit_raw["age"] = add_admit_raw["age"].astype("Int64")
    add_admit_raw["muac"] = add_admit_raw["muac"].astype(float)

    return pd.concat([admit_raw_df, add_admit_raw], ignore_index=True)

def annotate_admit_raw(admit_raw_df, new_pids):
    imci_emergency = admit_raw_df[admit_raw_df["imci_emergency_otp"] == "true"]
    imci_pids = imci_emergency["pid"].dropna().unique()

    def set_cleaning_note(row):
        if row["pid"] in new_pids:
            return "PID assigned retroactively during data cleaning."
        if row["pid"] in imci_pids and row.get("imci_emergency_otp") == "true":
            return "Presented as emergency case at OTP and referred to ITP with no PID assigned; linked with PID retroactively during data cleaning."
        return None

    admit_raw_df["cleaning_note"] = admit_raw_df.apply(set_cleaning_note, axis=1)
    return admit_raw_df, imci_pids


def build_admit_processed(admit_raw_df, admit_df, new_pids, imci_pids):
    add_admit_processed = admit_raw_df[admit_raw_df["pid"].isin(new_pids)].copy()

    # ... perform all column mutations like assigning `md_starttime`, `autosite`, `b_presented_emergency`, etc.
    # Example:
    add_admit_processed["calcdate"] = pd.to_datetime(add_admit_processed["todate"], errors="coerce")
    add_admit_processed["md_starttime"] = pd.to_datetime(
        add_admit_processed["start_time"], errors="coerce"
    )
    add_admit_processed["b_referred_emergency"] = (
        add_admit_processed["imci_emergency_otp"] == "true"
    )
    add_admit_processed["b_presented_emergency"] = (
        add_admit_processed["imci_emergency_itp"] == "true"
    )
    # Repeat for all other relevant columns as in the original mutate()...

    # Remove "wfhz" if needed
    vector_of_names = admit_df.columns.tolist()
    final_columns = [col for col in vector_of_names if col != "wfhz"]
    add_admit_processed = add_admit_processed[final_columns]

    # Append
    combined_admit = pd.concat([admit_df, add_admit_processed], ignore_index=True)

    # Cleaning note
    def set_note(row):
        if row["pid"] in new_pids:
            return "PID assigned retroactively during data cleaning."
        if row["pid"] in imci_pids and row.get("b_referred_emergency") is True:
            return "Presented as emergency case at OTP and referred to ITP with no PID assigned; linked with PID retroactively during data cleaning."
        return None

    combined_admit["cleaning_note"] = combined_admit.apply(set_note, axis=1)
    return combined_admit


# itp_loader.py

def load_google_sheet(url, worksheet_index=0):
    # Extract the sheet ID from the URL
    sheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", url).group(1)

    # Authenticate with Google using google-auth (via gspread)
    gc = gspread.service_account(filename="service_account.json")

    # Open the sheet and get the specified worksheet
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.get_worksheet(worksheet_index)

    # Load as DataFrame
    return get_as_dataframe(worksheet, evaluate_formulas=True, dtype=str).dropna(how="all")


# Example usage
itp_2023 = load_google_sheet(
    "https://docs.google.com/spreadsheets/d/1Pb_bGGaHRIyzwHhIBzebFqQhd-6HQttWKDeTvRR58-o/edit#gid=0"
)
# -- Clean 2023 --
itp_2023_clean = itp_2023[itp_2023["Facility"].notna()].assign(
    itp=itp_2023["Facility"],
    otp=itp_2023["OTP"],
    age=itp_2023["Age (months)"],
    sex=itp_2023["Sex (m/f)"],
    initial_dx=itp_2023["Diagnosis"],
    outcome=itp_2023["Outcome"],
    outcome_date=pd.to_datetime(
        itp_2023["Outcome date"].combine_first(itp_2023["LOS"]), errors="coerce"
    ),
    pid=itp_2023["Reg. No"],
    los_days=itp_2023["Week #"].astype(str),
    admit_date=pd.to_datetime(itp_2023["Date of admission"], errors="coerce"),
    ref_w_pid=itp_2023["Yes"],
    case_notes=itp_2023[["Mgt checked in folder", "notes_2", "notes_3"]]
    .astype(str)
    .agg(", ".join, axis=1),
)

# Clean up case_notes
itp_2023_clean["case_notes"] = (
    itp_2023_clean["case_notes"]
    .str.replace(r"NA, |, NA", "", regex=True)
    .str.replace(r", , ", ", ", regex=True)
    .str.replace(r"^NA$", "", regex=True)
    .str.replace(r"2023-11-08|2023-10-03|2023-09-26|2023-09-18", "", regex=True)
)

# Manual PID fixes
itp_2023_clean.loc[itp_2023_clean["pid"] == "-", "pid"] = itp_2023_clean["Name"].map(
    {"Mohmmed Lawadu": "23-9998", "Ali Adamu": "23-9999", "Laure Adamu": "23-9997"}
)

# Drop unnecessary columns
itp_2023_clean = itp_2023_clean.drop(
    columns=[
        "Reg. No",
        "Week #",
        "Name",
        "LOS",
        "Outcome date",
        "Referred with Reg. No",
        "Yes",
        "Mgt checked in folder",
        "notes_2",
        "notes_3",
        "Checked date",
        "Date of admission",
        "Facility",
        "OTP",
        "Age (months)",
        "Sex (m/f)",
        "Diagnosis",
        "Outcome",
    ],
    errors="ignore",
)

# ------------- Repeat Process for 2024 and 2025 ---------------- #

def clean_itp_roster(df, year):
    df["Pid"] = df["Pid"].replace("24--4534", "24-4534") if year == 2024 else df["Pid"]

    df = df.assign(
        sex=df["Sex (m/f)"],
        itp=df["Facility"],
        otp=df["OTP"],
        initial_dx=df["Initial Diagnosis"],
        outcome=df["Outcome"],
        final_dx=df["Final diagnosis"],
        age=df["Age (months)"].astype(str),
        muac=df["MUAC (cm)"].astype(str),
        pid=df["Pid"],
        admit_date=pd.to_datetime(df["Date of Admission"], errors="coerce"),
        outcome_date=pd.to_datetime(df["Outcome Date"], errors="coerce"),
        los_days=df["LOS"].astype(str),
        ref_w_pid=df["Referred with PID"],
        case_notes=df[["Note", "Folder checked"]].astype(str).agg(", ".join, axis=1),
    )

    # Clean up
    df["case_notes"] = (
        df["case_notes"]
        .str.replace(r"NA, |, NA", "", regex=True)
        .str.replace(r", , ", ", ", regex=True)
        .str.replace(r"^NA$", "", regex=True)
    )

    # Manual PID fixes
    pid_fixes = {
        "Aisha Umar": "24-9999",
        "Saifullahi Yahaya": "24-9998",
        "Abdullahi Ismail": "24-9990",
        "Maryam Muhd": "24-2757",
        "zainab Tijjani": "24-9991",
        "Hassan Usman": "24-0001",
        "Muhd Yahaya": "24-1953",
        "Jamila Lawai": "24-9989",
    }
    df.loc[df["pid"] == "-", "pid"] = df["Name"].map(pid_fixes)

    # Remove unneeded columns
    return df.drop(
        columns=[
            "Pid",
            "Age (months)",
            "MUAC (cm)",
            "Date of Admission",
            "Outcome Date",
            "LOS",
            "Folder checked",
            "Referred with PID",
            "Note",
            "?prob",
            "Name",
            "Facility",
            "OTP",
            "Sex (m/f)",
            "Initial Diagnosis",
            "Outcome",
            "Final diagnosis",
        ],
        errors="ignore",
    )


itp_2024 = load_google_sheet(
    "https://docs.google.com/spreadsheets/d/1kWdcIL7ajRxHfmtS7R3BNkUzUgEac-XCWlzM4xNlkuA/edit?gid=0"
)
itp_2024_clean = clean_itp_roster(itp_2024, year=2024)

itp_2025 = load_google_sheet(
    "https://docs.google.com/spreadsheets/d/11LqjmNJeHNLirCaYSijpJms0Rp1NZbJDKZ0WxtSAqkk/edit?gid=0"
)
itp_2025_clean = clean_itp_roster(itp_2025, year=2025)

# Additional LOS calculation
itp_2025_clean["los_days"] = (
    pd.to_datetime(itp_2025_clean["outcome_date"]) - pd.to_datetime(itp_2025_clean["admit_date"])
).dt.days.astype("float")

# ------------- Combine All ------------- #

itp_roster = pd.concat([itp_2024_clean, itp_2023_clean, itp_2025_clean], ignore_index=True)


# Final cleaning
def clean_itp_final(df):
    return (
        df.assign(
            otp=df["otp"].replace(
                {
                    "Kurjelli": "kurjale",
                    "PHC Kurjele": "kurjale",
                    "Ashaka": "jalingo",
                    "PHC Ashaka": "jalingo",
                    "Kuri": "kuri",
                    "PHC Kuri": "kuri",
                    "Sangaru": "sangaru",
                    "PHC Sangaru": "sangaru",
                }
            ),
            itp=df["itp"].replace(
                {"FTH": "fth", "SSHG": "ssh", "GH Deba": "gh_deba", "GH Bajoga": "gh_bajoga"}
            ),
            age=df["age"].replace({"20days": "0.67", "NULL": np.nan}),
            muac=df["muac"].replace({"-": np.nan, "NULL": np.nan}).astype(float),
            sex=df["sex"].replace({"M|": "m", "M": "m", "F": "f"}),
            los_days=pd.to_numeric(df["los_days"], errors="coerce"),
            case_notes=df.apply(
                lambda row: (
                    "Treated as out patient"
                    if row["los_days"] == "Treated as out patient"
                    else row["case_notes"]
                ),
                axis=1,
            ),
        )
        .replace({"los_days": {"Treated as out patient": np.nan}})
        .drop_duplicates()
    )


itp_roster_clean = clean_itp_final(itp_roster)


# admit_filter.py
def clean_admit_raw(admit_raw_df):
    # Step 1: Filter by review_state and consent
    filtered = admit_raw_df[
        (admit_raw_df["reviewState"].isna() | (admit_raw_df["reviewState"] != "rejected"))
        & (admit_raw_df["nophone_consent"].isna() | (admit_raw_df["nophone_consent"] != "false"))
        & (admit_raw_df["phone_consent"].isna() | (admit_raw_df["phone_consent"] != "false"))
    ].copy()

    # Step 2: Add b_has_phone_number
    filtered["b_has_phone_number"] = filtered["phone"].notna() | filtered["em_phone"].notna()

    # Step 3: Drop columns
    cols_to_drop = [
        "phone",
        "em_phone",
        "cg_name",
        "hoh_name",
        "homedesc",
        "hoh_wkname",
        "tradleader",
        "pp_phone",
        "pp_hoh",
        "em_pp_phone",
        "em_cg_name",
        "em_homedesc",
        "name",
        "pull_prev_name",
        "pull_prev_cg_name",
        "twin_name",
        "cg_wkname",
        "instance_name",
    ]
    filtered = filtered.drop(columns=cols_to_drop, errors="ignore")

    # Step 4: Extract admit_pids
    admit_pids = filtered["pid"].dropna().unique().tolist()

    return filtered, admit_pids


admit_raw_2, admit_pids = clean_admit_raw(admit_raw)

# weekly_loader.py

def load_weekly_raw():
    # Get submissions from ODK Central
    data = client.submissions.get_table(form_id="weekly", project_id=9)

    # Flatten JSON data into a dataframe
    df = pd.json_normalize(data["value"])
    df = df.dropna(how="all", axis=1)  # Drop columns that are all NaN
    df = df.rename(columns={k: k.split(".")[-1] for k in df.columns})

    # Strip "uuid:" prefix from `id`
    df["uuid"] = df["instanceID"].str.replace("^uuid:", "", regex=True)

    return df

def clean_weekly_raw(weekly_raw, admit_pids):
    df = weekly_raw.copy()

    df = df[
        (df["reviewState"].isna() | (df["reviewState"] != "rejected"))
        & (df["phone_consent"].isna() | (df["phone_consent"] != "false"))
        & (df["pid"].isin(admit_pids))
    ].copy()

    df["b_added_phone_number"] = df["phone"].notna()

    # Drop unnecessary fields
    drop_cols = ["phone", "pull_homedesc", "pull_cg_name", "pull_phone", "pull_name"]
    df = df.drop(columns=drop_cols, errors="ignore")

    return df


weekly_raw = load_weekly_raw()
weekly_raw_2 = clean_weekly_raw(weekly_raw, admit_pids)


def load_relapse_raw():
    # Get submissions from ODK Central
    data = client.submissions.get_table(form_id="relapse", project_id=12)

    # Flatten JSON data into a dataframe
    df = pd.json_normalize(data["value"])
    df = df.dropna(how="all", axis=1)  # Drop columns that are all NaN
    df = df.rename(columns={k: k.split(".")[-1] for k in df.columns})

    # Strip "uuid:" prefix from `id`
    df["uuid"] = df["instanceID"].str.replace("^uuid:", "", regex=True)

    return df

relapse_raw_df = load_relapse_raw()

def clean_relapse_raw(relapse_raw_df, relapse_pids, current_pids):
    df = relapse_raw_df[
        (relapse_raw_df["pid"].isin(relapse_pids))
        & (relapse_raw_df["pid"].isin(current_pids))
        & (
            (relapse_raw_df["set_final_consent"].isna())
            | (relapse_raw_df["set_final_consent"] != "refused")
        )
        & ((relapse_raw_df["c_consent"].isna()) | (relapse_raw_df["c_consent"] != "refused"))
        & ((relapse_raw_df["reviewState"].isna()) | (relapse_raw_df["reviewState"] != "rejected"))
    ].copy()

    # Drop all sensitive columns
    pii_cols = [
        col
        for col in df.columns
        if col.startswith("pull_")
        or col.startswith("homedesc")
        or "phone" in col
        or "contact" in col
        or "address" in col
        or "cg" in col
        or "name" in col
    ]
    df = df.drop(columns=pii_cols, errors="ignore")

    return df

def load_mh_raw():
    # Get submissions from ODK Central
    data = client.submissions.get_table(form_id="mmhs", project_id=11)

    # Flatten JSON data into a dataframe
    df = pd.json_normalize(data["value"])
    df = df.dropna(how="all", axis=1)  # Drop columns that are all NaN
    df = df.rename(columns={k: k.split(".")[-1] for k in df.columns})

    # Strip "uuid:" prefix from `id`
    df["uuid"] = df["instanceID"].str.replace("^uuid:", "", regex=True)

    return df

mh_raw_df = load_mh_raw()

def clean_mh_raw(mh_raw_df, mh_pids, current_pids):
    df = mh_raw_df[
        (mh_raw_df["session"] == "0")
        & (mh_raw_df["study_consent"] != "false")
        & (mh_raw_df["ineligible"] != "false")
        & (mh_raw_df["reviewState"].isna() | (mh_raw_df["reviewState"] != "rejected"))
        & (mh_raw_df["pid"].isin(mh_pids))
        & (mh_raw_df["pid"].isin(current_pids))
    ].copy()

    df["name"] = df["name"].str.replace("san", "ru", regex=False)

    # Drop PII columns
    pii_cols = [
        col
        for col in df.columns
        if "pull_" in col
        or "name" in col
        or "address" in col
        or "phone" in col
        or "cg" in col
        or "leader" in col
        or "hoh" in col
    ]
    df = df.drop(columns=pii_cols, errors="ignore")

    return df

def finalize_datasets(current_df, admit, weekly, itp_roster_df, current_pids):
    # Process `current`
    current_processed = current_df.copy()
    current_processed["age_on_20250315"] = current_processed["age"]
    current_processed["b_has_phone_number"] = current_processed["phone"].notna()
    current_processed = current_processed.drop(columns=["phone", "age"], errors="ignore")

    # Process `admit`
    admit_processed = admit.copy()
    admit_processed["b_has_phone_number"] = admit_processed["phone"].notna()
    admit_processed = admit_processed.drop(
        columns=[
            "name",
            "phone",
            "cg_name",
            "hoh_name",
            "homedesc",
            "hoh_wkname",
            "tradleader_name",
        ],
        errors="ignore",
    )
    admit_processed = admit_processed[admit_processed["pid"].isin(current_pids)]

    # Process `weekly`
    weekly_processed = weekly.copy()
    weekly_processed["b_added_phone_number"] = weekly_processed["phone"].notna()
    weekly_processed = weekly_processed.drop(columns=["phone"], errors="ignore")
    weekly_processed = weekly_processed[weekly_processed["pid"].isin(current_pids)]

    # Add sex from admit
    weekly_processed = weekly_processed.merge(admit_processed[["pid", "sex"]], on="pid", how="left")

    # Filter itp_roster
    itp_clean = itp_roster_df[itp_roster_df["pid"].isin(current_pids)].copy()

    return current_processed, admit_processed, weekly_processed, itp_clean


relapse_raw_2 = clean_relapse_raw(relapse_raw_df, relapse_pids, current_pids)
mh_raw_2 = clean_mh_raw(mh_raw_df, mh_pids, current_pids)
current_processed, admit_processed, weekly_processed, itp_roster_clean = finalize_datasets(
    current_df, admit, weekly, itp_roster_clean, current_pids
)


calc = Calculator()

def prepare_zscore_inputs(
    df,
    age_column="age",
    weight_column="weight",
    height_column="finalhl",
    domhl_column="domhl",
    sex_column="sex",
):
    df = df.copy()
    df["weight_col"] = pd.to_numeric(df[weight_column], errors="coerce")
    df["height_col"] = pd.to_numeric(df[height_column], errors="coerce")
    df["age"] = pd.to_numeric(df[age_column], errors="coerce")
    df["age_zscore"] = df["age"] * (365.25 / 12)

    df["standing"] = np.select(
        [df[domhl_column] == "height", df[domhl_column] == "length"], [1, 2], default=3
    )

    # Safe conversion: lowercase, strip whitespace, map known values, else np.nan
    df["sex_str"] = (
        df[sex_column]
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"male": "M", "female": "F"})
    )

    return df

def add_zscores(df, weight_col="weight", height_col="finalhl", age_col="age", sex_col="sex_str", source_name=""):
    df = df.copy()

    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")
    df[height_col] = pd.to_numeric(df[height_col], errors="coerce")
    df[age_col] = pd.to_numeric(df[age_col], errors="coerce")

    print(f"Sample values from {source_name} before z-score calc:")
    print(df[[age_col, sex_col, weight_col, height_col]].dropna().head(10))

    def safe_calc(func, row, *args):
        try:
            return func(*args)
        except Exception as e:
            print(f"[{source_name}] Skipped {func.__name__}: uuid={row.get('uuid')}, age={row[age_col]}, sex={row[sex_col]}, "
                  f"weight={row[weight_col]}, height={row[height_col]} — {type(e).__name__}: {e}")
            return np.nan

    df["wfaz"] = df.apply(
        lambda row: safe_calc(calc.wfa, row, row[weight_col], row[age_col], row[sex_col])
        if pd.notnull(row[weight_col]) and pd.notnull(row[age_col]) and pd.notnull(row[sex_col]) and row[age_col] <= 60
        else np.nan,
        axis=1
    )

    df["hfaz"] = df.apply(
        lambda row: safe_calc(calc.lhfa, row, row[height_col], row[age_col], row[sex_col], row.get("standing", 1) == 2)
        if pd.notnull(row[height_col]) and pd.notnull(row[age_col]) and pd.notnull(row[sex_col])
        else np.nan,
        axis=1
    )

    df["wfhz"] = df.apply(
        lambda row: safe_calc(calc.wfh, row, row[weight_col], row[sex_col], row[height_col], row.get("standing", 1) == 2)
        if pd.notnull(row[weight_col]) and pd.notnull(row[height_col]) and pd.notnull(row[sex_col])
        else np.nan,
        axis=1
    )

    return df

def add_deficiency_flags(df):
    df = df.copy()

    # Coerce to numeric
    df["muac"] = pd.to_numeric(df["muac"], errors="coerce")
    df["wfhz"] = pd.to_numeric(df["wfhz"], errors="coerce")
    df["hfaz"] = pd.to_numeric(df["hfaz"], errors="coerce")
    df["wfaz"] = pd.to_numeric(df["wfaz"], errors="coerce")

    df["b_wast"] = np.where(
        (df["wfhz"] < -3) & (df["hfaz"] < -3),
        True,
        np.where(df[["wfhz", "hfaz"]].isnull().any(axis=1), np.nan, False),
    )

    df["b_muac_waz"] = np.where(
        (df["muac"] < 11.5) & (df["wfaz"] < -3),
        True,
        np.where(df[["muac", "wfaz"]].isnull().any(axis=1), np.nan, False),
    )

    df["b_muac_wfh"] = np.where(
        (df["muac"] < 11.5) & (df["wfhz"] < -3),
        True,
        np.where(df[["muac", "wfhz"]].isnull().any(axis=1), np.nan, False),
    )

    return df

def add_breastfeeding_flags(df):
    df = df.copy()
    df["bfeed_age"] = np.select(
        [
            (df["age"] < 6) & (df["b_curr_bfeed"] == "true") & (df["alt_foods_bfeed"] == "false"),
            (df["b_curr_bfeed"] == "true")
            & (df["age"] > 5)
            & (df["age"] < 24)
            & df["alt_foods_bfeed"].astype(str).str.contains("food", na=False),
            (df["age"] > 24),
            (df["b_curr_bfeed"].isnull()),
        ],
        [True, True, np.nan, np.nan],
        default=False,
    )

    df["bfeed_exc"] = np.select(
        [
            (df["age"] < 6) & (df["b_curr_bfeed"] == "true") & (df["alt_foods_bfeed"] == "false"),
            (df["age"] > 5) & (df["age_takewater"] > 5) & (df["age_takefamily"] > 5),
            (df["age"] > 5) & (df["age_takefamily"].isnull()),
            (df["age"] > 5) & (df["age_takewater"].isnull()),
            (df["b_curr_bfeed"].isnull()),
        ],
        [True, True, np.nan, np.nan, np.nan],
        default=False,
    )

    df["bfeed_exc_food"] = np.select(
        [
            (df["age"] < 6) & (df["b_curr_bfeed"] == "true") & (df["alt_foods_bfeed"] == "false"),
            (df["age"] < 6)
            & (df["b_curr_bfeed"] == "true")
            & df["alt_foods_bfeed"].astype(str).str.contains("food", na=False),
            (df["age"] > 5) & (df["age_takefamily"] > 5),
            (df["age"] > 5) & (df["age_takefamily"].isnull()),
            (df["b_curr_bfeed"].isnull()),
        ],
        [True, True, True, np.nan, np.nan],
        default=False,
    )

    df["bfeed_intro_food"] = np.select(
        [
            (df["age"] < 6) & (df["b_curr_bfeed"] == "true") & (df["alt_foods_bfeed"] == "false"),
            (df["age"] > 5) & (df["age_takefamily"] == 6),
            (df["age"] > 5) & (df["age_takefamily"].isnull()),
            (df["b_curr_bfeed"].isnull()),
        ],
        [True, True, np.nan, np.nan],
        default=False,
    )

    return df

def add_season_flags(df, date_column):
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df["lean_season"] = df[date_column].dt.month.isin([6, 7, 8, 9])
    df["rainy_season"] = df[date_column].dt.month.isin([5, 6, 7, 8, 9, 10])
    return df

# === Apply to each dataset ===

# admit_processed and weekly_processed
admit_processed = prepare_zscore_inputs(
    admit_processed,
    age_column="enr_age",
    height_column="finalhl",
    domhl_column="domhl",
    sex_column="sex"
)
admit_processed = add_zscores(admit_processed, source_name="admit_processed")
admit_processed = add_deficiency_flags(admit_processed)
admit_processed = add_season_flags(admit_processed, date_column="calcdate")



weekly_processed = prepare_zscore_inputs(
    weekly_processed,
    age_column="wkl_age",
    height_column="finalhl",
    domhl_column="domhl",
    sex_column="sex"
)
weekly_processed = add_zscores(weekly_processed, source_name="weekly_processed")
weekly_processed = add_deficiency_flags(weekly_processed)
weekly_processed = add_season_flags(weekly_processed, date_column="calcdate")

# admit_raw_2
admit_raw_2 = prepare_zscore_inputs(
    admit_raw_2,
    age_column="age",
    height_column="hl",
    domhl_column="direction_of_measure",
    sex_column="c_sex"
)
admit_raw_2 = add_zscores(admit_raw_2, height_col="hl", source_name="admit_raw")
admit_raw_2 = add_deficiency_flags(admit_raw_2)
admit_raw_2 = add_breastfeeding_flags(admit_raw_2)
admit_raw_2 = add_season_flags(admit_raw_2, date_column="todate")

# weekly_raw_2
weekly_raw_2 = prepare_zscore_inputs(
    weekly_raw_2,
    age_column="age",
    height_column="hl",
    domhl_column="direction_of_measure",
    sex_column="c_sex"
)
weekly_raw_2 = add_zscores(weekly_raw_2, height_col="hl", source_name="weekly_raw")
weekly_raw_2 = add_deficiency_flags(weekly_raw_2)
weekly_raw_2 = add_season_flags(weekly_raw_2, date_column="todate")

# current_processed
current_processed = add_season_flags(current_processed, date_column="status_date")
