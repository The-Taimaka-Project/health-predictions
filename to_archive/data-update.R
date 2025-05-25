"""
This script will not work as-is. It is intended to be migrated to a Python script.
Currently, we are editing it to remove sections that we don't think will be needed
at inference time.

Notes:
- The original version of this script pulled data to use for training models. It
  is being adapted to pull data for running inference with those models.
- The script pulls data from 
  - a Postgres database
  - ODK (Open Data Kit) submissions
  - Google Sheets

TODO:
- At inference time, will we need data from both the Postgres database and ODK?
- This script also pulls five Google sheets. I've left questions in the code
  about whether we need to do this at inference time.
"""


library(pool)
library(dbplyr)
library(tidyverse)
library(data.table)
library(lubridate)
library(dplyr)

### Load DB: Current, Admit, Weekly, MH, Relapse ###
db <- dbPool(
  RPostgres::Postgres(),
  dbname = "cmam",
  host = "taimaka-internal.org",
  port = "5432",
  user = "jostrowski",
  password = "<redacted!>"
)

current <- data.frame(db %>% tbl(in_schema("data", "current")))

admit <- data.frame(db %>% tbl(in_schema("data", "dict"))) %>% 
  select(-b_assignedtocct)

weekly <- data.frame(db %>% tbl(in_schema("data", "weekly")))

mh <- data.frame(db %>% tbl(in_schema("data", "mmh_dict")))
mh_pids <- mh$pid

relapse <- data.frame(db %>% tbl(in_schema("data", "relapse_dict")))
relapse_pids <- relapse$pid

exclusions <- data.frame(db %>% tbl(in_schema("data", "manual_exclusions"))) %>% 
  group_by(pid) %>% 
  filter(dt == max(dt)) %>% ## Select the most recent dt value for a given PID (appears to be how current table handles these cases)
  filter(change_timestamp == min(change_timestamp)) %>% ## for dt values with the same dt, select the one that was entered earliest (not intutive, but seems to align with current table) 
  ungroup()

deaths <- data.frame(db %>% tbl(in_schema("data", "deaths")))

pool::poolClose(db)

### Load ODK: Admit, Weekly, MH, Relapse ### 
library(ruODK)

ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/9/forms/admit.scv',
  un = 'jennifer@taimaka.org',
  pw = '<redacted!>', 
  verbose = TRUE
)

admit_raw <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

### Deleted a bunch of stuff here

# Pull notes on exclusions and deaths into the current df
current <- current %>% 
  left_join(exclusions, by = 'pid') %>% 
  mutate(exclusion_notes = notes) %>% 
  select(-updt_id, -change_timestamp, -notes, -dt) %>% 
  left_join(deaths, by = 'pid') %>% 
  mutate(death_notes = notes) %>% 
  select(-change_timestamp, -notes, -dod)

current <- current %>% 
  mutate(cleaning_note = ifelse(pid %in% new_pids, 'PID assigned retroactively during data cleaning.', NA)
  )

# Deleted more stuff here

### Load and clean ITP data ###
library(googlesheets4)
library(stringr)

# Deleted ITP roster loading code that started from google sheets

# TODO: Want to check-- do we need the below section?

### Additional cleaning of raw ODK admissions data ###
admit_raw_2 <- admit_raw %>% 
  filter(is.na(review_state) | review_state != "rejected") %>% 
  filter(is.na(nophone_consent) | nophone_consent != 'false') %>% 
  filter(is.na(phone_consent) | phone_consent != 'false') %>% 
  mutate(staffmember = ifelse(staffmember == 'other', otherstaff, staffmember),
         staffmember = ifelse(staffmember == "Fatsuma chiroma ", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima  Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISA GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata ", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi ", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba\n", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba.", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abbao", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda a", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i\n", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar ", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "\nMuhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Far\nFatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "FATIMA I SAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa  galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima.", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman \n", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman a ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa .", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila  I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar \n", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Mugammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar ", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Abubakar Adamu", "other", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim ", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu ", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Blessing Nathan ", "bnathan", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "grace yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Hajara ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "hdantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "hmohammed", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri ", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Martha", "other", staffmember),
         staffmember = ifelse(staffmember == "Martha ", "other", staffmember),
         staffmember = ifelse(staffmember == "Maryam abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa  Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang", "other", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda\n\n", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Usman abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman anubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "\nHadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "08024404147", "other", staffmember),
         staffmember = ifelse(staffmember == "09076459432", "other", staffmember),
         staffmember = ifelse(staffmember == "2028886446", "other", staffmember),
         staffmember = ifelse(staffmember == "23-2420", "other", staffmember),
         staffmember = ifelse(staffmember == "Alibaba", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Dr. Umar ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Hadiza  Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammadu ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hajara Nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata \n", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata the", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "James jesse ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Seun Adejumo", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Usman ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Zulaihat Sani", "other", staffmember),
         staffmember = ifelse(staffmember == "Haraja Nuhu", 'hnuhu', staffmember)
  ) %>% 
  mutate(b_has_phone_number = ifelse(!is.na(phone) | !is.na(em_phone), TRUE, FALSE)) %>% 
  select(-phone, -em_phone, -cg_name, -hoh_name, -homedesc, -hoh_wkname, -tradleader,
         -pp_phone, -pp_hoh, -em_pp_phone, -em_cg_name, -em_homedesc, -name,
         -pull_prev_name, -pull_prev_cg_name, -twin_name, -cg_wkname, -instance_name)

admit_pids <- admit_raw_2$pid

### Load raw weekly data ###
ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/9/forms/weekly.scv',
  un = 'jennifer@taimaka.org',
  pw = '<redacted!>', 
  verbose = TRUE
)

weekly_raw <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

# TODO: Do we need staff member cleaning at inference time?

weekly_raw_2 <- weekly_raw %>% 
  mutate(id = gsub("^uuid:", "", id)) %>% 
  filter(is.na(review_state) | review_state != "rejected") %>% 
  filter(is.na(phone_consent) | phone_consent != 'false') %>% 
  filter(pid %in% admit_pids) %>% 
  mutate(staffmember = ifelse(staffmember == 'other', otherstaff, staffmember),
         staffmember = ifelse(staffmember == "Fatsuma chiroma ", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima  Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISA GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata ", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi ", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba\n", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba.", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abbao", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda a", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i\n", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar ", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "\nMuhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Far\nFatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "FATIMA I SAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa  galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima.", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman \n", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman a ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa .", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila  I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar \n", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Mugammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar ", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Abubakar Adamu", "other", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim ", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu ", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Blessing Nathan ", "bnathan", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "grace yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Hajara ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "hdantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "hmohammed", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri ", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Martha", "other", staffmember),
         staffmember = ifelse(staffmember == "Martha ", "other", staffmember),
         staffmember = ifelse(staffmember == "Maryam abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa  Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang", "other", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda\n\n", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Usman abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman anubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "\nHadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "08024404147", "other", staffmember),
         staffmember = ifelse(staffmember == "09076459432", "other", staffmember),
         staffmember = ifelse(staffmember == "2028886446", "other", staffmember),
         staffmember = ifelse(staffmember == "23-2420", "other", staffmember),
         staffmember = ifelse(staffmember == "Alibaba", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Dr. Umar ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Hadiza  Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammadu ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hajara Nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata \n", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata the", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "James jesse ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Seun Adejumo", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Usman ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Zulaihat Sani", "other", staffmember),
         staffmember = ifelse(staffmember == "Haraja Nuhu", 'hnuhu', staffmember)
  ) %>% 
  mutate(b_added_phone_number = ifelse(!is.na(phone), TRUE, FALSE)) %>% 
  select(-phone, -pull_homedesc, -pull_cg_name, -pull_phone, -pull_name)

# update staff member names in the processed data
admit <- admit %>% 
  mutate(staffmember = ifelse(staffmember == 'other', staffmember_other, staffmember),
         staffmember = ifelse(staffmember == "Fatsuma chiroma ", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima  Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISA GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata ", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi ", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba\n", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba.", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abbao", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda a", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i\n", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar ", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "\nMuhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Far\nFatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "FATIMA I SAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa  galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima.", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman \n", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman a ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa .", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila  I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar \n", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Mugammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar ", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Abubakar Adamu", "other", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim ", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu ", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Blessing Nathan ", "bnathan", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "grace yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Hajara ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "hdantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "hmohammed", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri ", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Martha", "other", staffmember),
         staffmember = ifelse(staffmember == "Martha ", "other", staffmember),
         staffmember = ifelse(staffmember == "Maryam abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa  Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang", "other", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda\n\n", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Usman abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman anubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "\nHadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "08024404147", "other", staffmember),
         staffmember = ifelse(staffmember == "09076459432", "other", staffmember),
         staffmember = ifelse(staffmember == "2028886446", "other", staffmember),
         staffmember = ifelse(staffmember == "23-2420", "other", staffmember),
         staffmember = ifelse(staffmember == "Alibaba", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Dr. Umar ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Hadiza  Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammadu ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hajara Nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata \n", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata the", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "James jesse ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Seun Adejumo", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Usman ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Zulaihat Sani", "other", staffmember),
         staffmember = ifelse(staffmember == "Haraja Nuhu", 'hnuhu', staffmember))

weekly <- weekly %>% 
  mutate(staffmember = ifelse(staffmember == 'other', staffmember_other, staffmember),
         staffmember = ifelse(staffmember == "Fatsuma chiroma ", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima  Isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISA GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata ", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa ", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi ", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba\n", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abba.", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa Abbao", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda a", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila husaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i\n", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila Hussaini i ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila I Hussaini ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar ", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "\nMuhammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Far\nFatsuma Usman Liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "FATIMA I SAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa  galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima isa galadima ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatima Isa galadima.", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA ", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "FATIMA ISAH GALADIMA", "fgaladima", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman \n", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman Liman a ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Hadiza", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hauwa", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Huldah Sunday Tarfa .", "hstarfa", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Abdullahi ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda ", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Samaila ", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Samaila  I Hussaini", "iibrahim", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar \n", "uabubakar", staffmember),
         staffmember = ifelse(staffmember == "Mugammad Musa Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar ", "aabubakar", staffmember),
         staffmember = ifelse(staffmember == "Abubakar Adamu", "other", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Adamu Ibrahim ", "aibrahim", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Aishatu Abubakar Salihu ", "aabubakarsalihu", staffmember),
         staffmember = ifelse(staffmember == "Blessing Nathan ", "bnathan", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma Usman liman ", "fliman", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "grace yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Grace Yamta ", "gyamta", staffmember),
         staffmember = ifelse(staffmember == "Hajara ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hajara nuhu ", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "hdantata", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "hmohammed", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Idris Muhammad Waziri ", "imwaziri", staffmember),
         staffmember = ifelse(staffmember == "Martha", "other", staffmember),
         staffmember = ifelse(staffmember == "Martha ", "other", staffmember),
         staffmember = ifelse(staffmember == "Maryam abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Maryam Abdullahi", "mabdullahi", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa  Abba", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Muhammad Musa ", "mabba", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang", "other", staffmember),
         staffmember = ifelse(staffmember == "Rautha Esau Shayang ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya Sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Rukayya sulaiman Ahmad ", "other", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad Dauda", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Saddiqa Muhammad dauda\n\n", "sdauda", staffmember),
         staffmember = ifelse(staffmember == "Usman abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Abubakar dantata ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman anubakar dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Yahaya Mohammed Umar ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "\nHadiza Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "08024404147", "other", staffmember),
         staffmember = ifelse(staffmember == "09076459432", "other", staffmember),
         staffmember = ifelse(staffmember == "2028886446", "other", staffmember),
         staffmember = ifelse(staffmember == "23-2420", "other", staffmember),
         staffmember = ifelse(staffmember == "Alibaba", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Dr. Umar ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Fatsuma", "fchiroma", staffmember),
         staffmember = ifelse(staffmember == "Hadiza  Muhammad ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hadiza Muhammadu ", "hmuhammad", staffmember),
         staffmember = ifelse(staffmember == "Hajara Nuhu", "hnuhu", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata \n", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "Hauwa Muhammad Dantata the", "hmdantata", staffmember),
         staffmember = ifelse(staffmember == "James jesse ", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Seun Adejumo", "ostaff", staffmember),
         staffmember = ifelse(staffmember == "Usman ", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Usman Dantata", "udantata", staffmember),
         staffmember = ifelse(staffmember == "Yahaya ", "yumar", staffmember),
         staffmember = ifelse(staffmember == "Zulaihat Sani", "other", staffmember),
         staffmember = ifelse(staffmember == "Haraja Nuhu", 'hnuhu', staffmember))

current_pids <- current$pid

### Load Relapse Study Data ###
ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/12/forms/relapse.scv',
  un = 'jennifer@taimaka.org',
  pw = '<redacted!>', 
  verbose = TRUE
)

relapse_raw <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

relapse_raw_2 <- relapse_raw %>% 
  filter(pid %in% relapse_pids,
         pid %in% current_pids,
         is.na(set_final_consent) | set_final_consent != 'refused',
         is.na(c_consent) | c_consent != 'refused') %>% 
  filter(is.na(review_state) | review_state != "rejected")

relapse_raw_2 <- relapse_raw_2 %>% 
  select(-pull_phone_otp, -pull_phone_relapse, -pull_phone, -homedesc, -full_address, 
         -leader, -hoh, -hoh_wkname, -cg_name, -new_prim_cg, -verify_cg, -prim_cg, -alt_cg,
         -new_homedesc, -set_homedesc, -new_leader, -set_tradleader, -new_hoh, -set_hoh_name,
         -phone, -set_phone, -sec_con_info, -update_sec_con, -instruct_contact, -sec_cont_one, 
         -update_sec_con, -set_sec_con_name, -set_sec_con_wkname,
         -contact_rel_to_cg, -otherrelations_text, -sec_con_rel_final, -comm_sec_cont, 
         -set_comm_sec_cont, -check_sec_cont_phone, -sec_cont_phone, -set_sec_con_phone, 
         -find_contact, -find_sec_con, -sub_sec_con_name,           
         -ent_sec_con_name, -curr_sec_con_name, -sub_sec_con_wkname,   
         -ent_sec_con_wkname, -curr_sec_con_wkname, -sub_sec_con_rel_final,     
         -ent_sec_con_rel_final, -curr_sec_con_rel_final, -sub_comm_sec_cont,         
         -ent_comm_sec_cont, -curr_comm_sec_cont, -sub_sec_con_phone,         
         -ent_sec_con_phone, -curr_sec_con_phone, -sub_find_sec_con,          
         -ent_find_sec_con, -curr_find_sec_con, -ent_phone,                 
         -curr_phone, -display_phone, -pt_contact, -set_cgname, -child_name,
         -sub_phone_owner_name, -ent_phone_owner_name, -curr_phone_owner_name,
         -new_cg, -new_name, -set_ptname, -sub_geopoints, -ent_geopoints, 
         -curr_geopoints, -home_location, -set_geopoints, -phone_owner_name, 
         -set_phone_owner_name, -sec_cont_local_name, -vconsent_name)

### Load Mental Health Study Data ###
ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/11/forms/mmhs.scv',
  un = 'jennifer@taimaka.org',
  pw = '<redacted!>', 
  verbose = TRUE
)

mh_raw <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

mh_raw$name <- gsub("san", "ru", mh_raw$name)

mh_raw_2 <- mh_raw %>% 
  filter(session == '0',
         study_consent != 'false',
         ineligible != 'false',
         is.na(review_state) | review_state != 'rejected',
         pid %in% mh_pids,
         pid %in% current_pids)

mh_raw_2 <- mh_raw_2 %>% 
  select(-pull_phone, -pull_address, -pull_leader, -pull_hoh_name, -pull_hoh_wkname,
         -pull_cg_name, -b_cr_address, -new_address, -new_address_reason, -new_address_reason_other,
         -b_cr_phone, -new_phone, -b_cr_leader, -new_leader, -b_cr_hoh, -new_hohname,
         -child_name)

### Additional cleaning of DB datasets ###
# Removal of unnecesssary PII 
current_processed <- current %>% 
  mutate(age_on_20250315 = age) %>% 
  mutate(b_has_phone_number = ifelse(!is.na(phone), TRUE, FALSE)) %>% 
  select(-phone, -age) 

admit_processed <- admit %>% 
  mutate(b_has_phone_number = ifelse(!is.na(phone), TRUE, FALSE)) %>% 
  select(-name, -phone, -cg_name, -hoh_name, -homedesc, -hoh_wkname, -tradleader_name) %>% 
  filter(pid %in% current_pids)

weekly_processed <- weekly %>% 
  mutate(b_added_phone_number = ifelse(!is.na(phone), TRUE, FALSE)) %>% 
  select(-phone) %>% 
  filter(pid %in% current_pids)

weekly_processed <- weekly_processed %>%
  left_join(admit_processed %>% select(pid, sex), by = "pid")

itp_roster <- itp_roster %>% 
  filter(pid %in% current_pids)

### Load Geolocation Data ###
ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/9/forms/Coordinate%20Tracker%20Form%20(1).scv',
  un = 'jennifer@taimaka.org',
  pw = '<redacted!>', 
  verbose = TRUE
)

settlement_loc <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

settlement_loc2 <- settlement_loc %>% 
  filter(is.na(review_state) | review_state != "rejected")

### Cleaning of extreme values ###
admit_processed$ses_hh_adults[admit_processed$ses_hh_adults == 1511] <- NA
admit_raw_2$household_adults[admit_raw_2$household_adults == 1511] <- NA

admit_processed$finalhl[admit_processed$finalhl == 27.5] <- 70
admit_raw_2$hl[admit_raw_2$hl == 27.5] <- 70

admit_processed$finalhl[admit_processed$finalhl == 31] <- 72.2
admit_raw_2$hl[admit_raw_2$hl == 31] <- 72.2

admit_processed$finalhl[admit_processed$finalhl == 7.2] <- 82
admit_raw_2$hl[admit_raw_2$hl == 7.2] <- 82

# TODO: How to handle two visits in a single day at inference time?

### Cleaning of 2 visits in a single day ###
pid_dup_visits <- read_sheet("https://docs.google.com/spreadsheets/d/1of2XyX_-zz-JWTNd3Xls26QPbKzmRISDshZF3OzNVBs/edit?gid=0#gid=0") %>% 
  as.data.frame() %>% 
  select(-to_change, -pid)

weekly_processed <- weekly_processed %>%
  left_join(pid_dup_visits, by = c("uuid" = "uuid")) %>%
  mutate(pid = if_else(!is.na(new_value), new_value, pid)) %>% 
  select(-new_value)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(pid_dup_visits, by = c("id" = "uuid")) %>%
  mutate(new_value = as.character(new_value)) %>% 
  mutate(pid = if_else(!is.na(new_value), new_value, pid)) %>% 
  select(-new_value)

# Removed historical filtering of weekly processed data by UUID

# TODO: do we need a weekly age update at inference time?

# Incorporate weekly age updates from google spreadsheet/csv
weekly_age_update <- read_sheet("https://docs.google.com/spreadsheets/d/1f34ZlqbOt5nyY-TbVvpX3BBtIcCBDWsg7bwFTYadhpw/edit?usp=sharing") %>%
  as.data.frame() %>% 
  mutate(calc_age = as.integer(calc_age),
         uuid = as.character(uuid)) %>% 
  select(-pid, -rec_age)

weekly_processed <- weekly_processed %>%
  mutate(wkl_age = as.integer(wkl_age))

weekly_raw_2 <- weekly_raw_2 %>% 
  mutate(age = as.integer(age),
         id = as.character(id))

weekly_processed <- weekly_processed %>%
  left_join(weekly_age_update, by = c("uuid" = "uuid")) %>%
  mutate(wkl_age = if_else(!is.na(calc_age), calc_age, wkl_age)) %>% 
  select(-calc_age)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_age_update, by = c("id" = "uuid")) %>%
  mutate(todate = as.Date(todate)) %>% 
  mutate(age = if_else(!is.na(calc_age), calc_age, age)) %>% 
  select(-calc_age)

# Incorporate weekly extremes updates from google spreadsheet/csv
library(tidyverse)
library(googlesheets4)

# TODO: do we need weekly extremes updates at inference time?

# Read and preprocess the weekly_extremes data
weekly_extremes <- read_sheet("https://docs.google.com/spreadsheets/d/1v6ZjgaZ-TZOzoVi3HdBCzNr4ec-Omg7SmZ379-STzXc/edit?usp=sharing") %>%
  select(-todate, -pid) %>%
  mutate(to_change = as.character(to_change),
         new_value_c = as.character(new_value_c),
         new_value_n = sapply(new_value_n, function(x) if (is.null(x)) NA else x),
         new_value_n = as.numeric(new_value_n),
         uuid = as.character(uuid))  # Ensure uuid is character

# Ensure id in weekly_raw_2 and weekly_processed are character for matching
weekly_raw_2 <- weekly_raw_2 %>%
  mutate(id = as.character(id),
         hl = as.numeric(hl), 
         hl_rounded = as.numeric(hl_rounded),
         muac = as.numeric(muac),
         wfh_sam_threshold = as.numeric(wfh_sam_threshold),
         wfh_mam_threshold = as.numeric(wfh_mam_threshold))

weekly_processed <- weekly_processed %>%
  mutate(uuid = as.character(uuid))

weekly_extremes_finalhl <- weekly_extremes %>%
  filter(to_change == 'finalhl') %>%
  mutate(new_value_n = as.numeric(new_value_n))

weekly_extremes_hl_rounded <- weekly_extremes %>%
  filter(to_change == 'hl_rounded') %>%
  mutate(new_value_n = as.numeric(new_value_n))

weekly_extremes_muac <- weekly_extremes %>%
  filter(to_change == 'muac') %>%
  mutate(new_value_n = as.numeric(new_value_n))

weekly_extremes_muac_status <- weekly_extremes %>%
  filter(to_change == 'muac_status')

weekly_extremes_wfh_lookup_calc <- weekly_extremes %>%
  filter(to_change == 'wfh_lookup_calc')

weekly_extremes_wfh_maln_status <- weekly_extremes %>%
  filter(to_change == 'wfh_maln_status')

weekly_extremes_wfh_sam_threshold <- weekly_extremes %>%
  filter(to_change == 'wfh_sam_threshold') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

weekly_extremes_wfh_mam_threshold <- weekly_extremes %>%
  filter(to_change == 'wfh_mam_threshold') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

weekly_extremes_weight <- weekly_extremes %>%
  filter(to_change == 'weight') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

weekly_extremes_weight_rounded <- weekly_extremes %>%
  filter(to_change == 'weight_rounded') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

weekly_extremes_c_sex <- weekly_extremes %>%
  filter(to_change == 'c_sex')

weekly_extremes_sex_abriev <- weekly_extremes %>%
  filter(to_change == 'sex_abriev')

weekly_extremes_wkl_age <- weekly_extremes %>%
  filter(to_change == 'wkl_age') %>%
  mutate(new_value_n = as.numeric(new_value_n))

weekly_processed <- weekly_processed %>%
  left_join(weekly_extremes_finalhl, by = c("uuid" = "uuid")) %>%
  mutate(
    finalhl = if_else(!is.na(new_value_n) & to_change == "finalhl", new_value_n, finalhl)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_processed <- weekly_processed %>%
  left_join(weekly_extremes_muac, by = c("uuid" = "uuid")) %>%
  mutate(
    muac = if_else(!is.na(new_value_n) & to_change == "muac", new_value_n, muac)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_processed <- weekly_processed %>% 
  left_join(weekly_extremes_muac_status, by = c("uuid" = "uuid")) %>%
  mutate(
    ms_muac = if_else(!is.na(new_value_c) & to_change == "muac_status", new_value_c, ms_muac)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_processed <- weekly_processed %>% 
  left_join(weekly_extremes_wfh_maln_status, by = c("uuid" = "uuid")) %>%
  mutate(
    ms_wfh = if_else(!is.na(new_value_c) & to_change == "wfh_maln_status", new_value_c, ms_wfh)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_processed <- weekly_processed %>% 
  left_join(weekly_extremes_weight, by = c("uuid" = "uuid")) %>%
  mutate(
    weight = if_else(!is.na(new_value_n) & to_change == "weight", new_value_n, weight)
  ) %>%
  select(-to_change, -new_value_n)

weekly_processed <- weekly_processed %>%
  left_join(weekly_extremes_c_sex, by = c("uuid" = "uuid")) %>%
  mutate(
    sex = if_else(!is.na(new_value_c) & to_change == "c_sex", new_value_c, sex)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_processed <- weekly_processed %>%
  left_join(weekly_extremes_wkl_age, by = c("uuid" = "uuid")) %>% 
  mutate(
    wkl_age = if_else(!is.na(new_value_n) & to_change == "wkl_age", new_value_n, wkl_age)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_finalhl, by = c("id" = "uuid")) %>%
  mutate(
    hl = if_else(!is.na(new_value_n) & to_change == "finalhl", new_value_n, hl)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_hl_rounded, by = c("id" = "uuid")) %>%
  mutate(
    hl_rounded = if_else(!is.na(new_value_n) & to_change == "hl_rounded", new_value_n, hl_rounded)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_muac, by = c("id" = "uuid")) %>%
  mutate(
    muac = if_else(!is.na(new_value_n) & to_change == "muac", new_value_n, muac)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_muac_status, by = c("id" = "uuid")) %>%
  mutate(
    muac_status = if_else(!is.na(new_value_c) & to_change == "muac_status", new_value_c, muac_status)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_wfh_lookup_calc, by = c("id" = "uuid")) %>%
  mutate(
    wfh_lookup_calc = if_else(!is.na(new_value_c) & to_change == "wfh_lookup_calc", new_value_c, wfh_lookup_calc)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_wfh_maln_status, by = c("id" = "uuid")) %>%
  mutate(
    wfh_maln_status = if_else(!is.na(new_value_c) & to_change == "wfh_maln_status", new_value_c, wfh_maln_status)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_wfh_sam_threshold, by = c("id" = "uuid")) %>%
  mutate(
    wfh_sam_threshold = if_else(!is.na(new_value_n) & to_change == "wfh_sam_threshold", new_value_n, wfh_sam_threshold)
  ) %>% 
  select(-to_change, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_wfh_mam_threshold, by = c("id" = "uuid")) %>%
  mutate(
    wfh_mam_threshold = if_else(!is.na(new_value_n) & to_change == "wfh_mam_threshold", new_value_n, wfh_mam_threshold)
  ) %>%
  select(-to_change, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_weight, by = c("id" = "uuid")) %>%
  mutate(
    weight = as.numeric(weight),
    weight = if_else(!is.na(new_value_n) & to_change == "weight", new_value_n, weight)
  ) %>%
  select(-to_change, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>% 
  left_join(weekly_extremes_weight_rounded, by = c("id" = "uuid")) %>%
  mutate(
    weight_rounded = as.numeric(weight_rounded),
    weight_rounded = if_else(!is.na(new_value_n) & to_change == "weight_rounded", new_value_n, weight_rounded)
  ) %>%
  select(-to_change, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_c_sex, by = c("id" = "uuid")) %>%
  mutate(
    c_sex = if_else(!is.na(new_value_c) & to_change == "c_sex", new_value_c, c_sex)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_sex_abriev, by = c("id" = "uuid")) %>%
  mutate(
    sex_abriev = if_else(!is.na(new_value_c) & to_change == "sex_abriev", new_value_c, sex_abriev)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(weekly_extremes_wkl_age, by = c("id" = "uuid")) %>% 
  mutate(
    age = if_else(!is.na(new_value_n) & to_change == "wkl_age", new_value_n, age)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

# TODO: do we need admit extremes updates at inference time?

# Incorporate admit extremes updates from google spreadsheet/csv
admit_extremes <- read_sheet("https://docs.google.com/spreadsheets/d/1aaLUNUBK0Run9A999EZpVBTT77mnwRKP9u3GrDwoHlM/edit?gid=1821666321#gid=1821666321") %>% 
  as.data.frame() %>% 
  select(to_change, uuid, new_value_n, new_value_c) %>%
  mutate(to_change = as.character(to_change),
         new_value_c = as.character(new_value_c),
         uuid = as.character(uuid))

admit_raw_2 <- admit_raw_2 %>%
  mutate(id = as.character(id),
         hl = as.numeric(hl), 
         hl_rounded = as.numeric(hl_rounded),
         muac = as.numeric(muac))

admit_extremes_finalhl <- admit_extremes %>%
  filter(to_change == 'finalhl')%>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_hl_rounded <- admit_extremes %>%
  filter(to_change == 'hl_rounded')%>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_muac <- admit_extremes %>%
  filter(to_change == 'muac')%>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_muac_status <- admit_extremes %>%
  filter(to_change == 'muac_status')

admit_extremes_wfh_lookup_calc <- admit_extremes %>%
  filter(to_change == 'wfh_lookup_calc')

admit_extremes_wfh_maln_status <- admit_extremes %>%
  filter(to_change == 'wfh_maln_status')

admit_extremes_wfh_sam_threshold <- admit_extremes %>%
  filter(to_change == 'wfh_sam_threshold') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_wfh_mam_threshold <- admit_extremes %>%
  filter(to_change == 'wfh_mam_threshold') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_weight <- admit_extremes %>%
  filter(to_change == 'weight') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_extremes_weight_rounded <- admit_extremes %>%
  filter(to_change == 'weight_rounded') %>%
  mutate(new_value_n = as.numeric(new_value_n)) %>%
  select(-new_value_c)

admit_processed <- admit_processed %>%
  left_join(admit_extremes_finalhl, by = c("uuid" = "uuid")) %>%
  mutate(
    finalhl = if_else(!is.na(new_value_n) & to_change == "finalhl", new_value_n, finalhl)
  ) %>%
  select(-to_change, -new_value_n)

admit_processed <- admit_processed %>%
  left_join(admit_extremes_hl_rounded, by = c("uuid" = "uuid")) %>%
  mutate(
    roundedhl = if_else(!is.na(new_value_n) & to_change == "hl_rounded", new_value_n, roundedhl)
  ) %>%
  select(-to_change, -new_value_n)

admit_processed <- admit_processed %>%
  left_join(admit_extremes_muac, by = c("uuid" = "uuid")) %>%
  mutate(
    muac = if_else(!is.na(new_value_n) & to_change == "muac", new_value_n, muac)
  ) %>%
  select(-to_change, -new_value_n)

admit_processed <- admit_processed %>% 
  left_join(admit_extremes_muac_status, by = c("uuid" = "uuid")) %>%
  mutate(
    ms_muac = if_else(!is.na(new_value_c) & to_change == "muac_status", new_value_c, ms_muac)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

admit_processed <- admit_processed %>% 
  left_join(admit_extremes_wfh_maln_status, by = c("uuid" = "uuid")) %>%
  mutate(
    ms_wfh = if_else(!is.na(new_value_c) & to_change == "wfh_maln_status", new_value_c, ms_wfh)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

admit_processed <- admit_processed %>% 
  left_join(admit_extremes_weight, by = c("uuid" = "uuid")) %>%
  mutate(
    weight = if_else(!is.na(new_value_n) & to_change == "weight", new_value_n, weight)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>%
  left_join(admit_extremes_finalhl, by = c("id" = "uuid")) %>%
  mutate(
    hl = if_else(!is.na(new_value_n) & to_change == "finalhl", new_value_n, hl)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>%
  left_join(admit_extremes_hl_rounded, by = c("id" = "uuid")) %>%
  mutate(
    hl_rounded = if_else(!is.na(new_value_n) & to_change == "hl_rounded", new_value_n, hl_rounded)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>%
  left_join(admit_extremes_muac, by = c("id" = "uuid")) %>%
  mutate(
    muac = if_else(!is.na(new_value_n) & to_change == "muac", new_value_n, muac)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_muac_status, by = c("id" = "uuid")) %>%
  mutate(
    muac_status = if_else(!is.na(new_value_c) & to_change == "muac_status", new_value_c, muac_status)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_wfh_lookup_calc, by = c("id" = "uuid")) %>%
  mutate(
    wfh_lookup_calc = if_else(!is.na(new_value_c) & to_change == "wfh_lookup_calc", new_value_c, wfh_lookup_calc)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_wfh_maln_status, by = c("id" = "uuid")) %>%
  mutate(
    wfh_maln_status = if_else(!is.na(new_value_c) & to_change == "wfh_maln_status", new_value_c, wfh_maln_status)
  ) %>%
  select(-to_change, -new_value_c, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_wfh_sam_threshold, by = c("id" = "uuid")) %>%
  mutate(
    wfh_sam_threshold = as.numeric(wfh_sam_threshold),
    wfh_sam_threshold = if_else(!is.na(new_value_n) & to_change == "wfh_sam_threshold", new_value_n, wfh_sam_threshold)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_wfh_mam_threshold, by = c("id" = "uuid")) %>%
  mutate(
    wfh_mam_threshold = as.numeric(wfh_mam_threshold),
    wfh_mam_threshold = if_else(!is.na(new_value_n) & to_change == "wfh_mam_threshold", new_value_n, wfh_mam_threshold)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_weight, by = c("id" = "uuid")) %>%
  mutate(
    weight = as.numeric(weight),
    weight = if_else(!is.na(new_value_n) & to_change == "weight", new_value_n, weight)
  ) %>%
  select(-to_change, -new_value_n)

admit_raw_2 <- admit_raw_2 %>% 
  left_join(admit_extremes_weight_rounded, by = c("id" = "uuid")) %>%
  mutate(
    weight_rounded = as.numeric(weight_rounded),
    weight_rounded = if_else(!is.na(new_value_n) & to_change == "weight_rounded", new_value_n, weight_rounded)
  ) %>%
  select(-to_change, -new_value_n)

# TODO: do we need birthdate and age updates at inference time?

#birthdate and subsequent age changes
birthdate_changes <- read_sheet("https://docs.google.com/spreadsheets/d/1l_Xm4UeWqZNs5z5_QVS7pvHVSrCgOiLe6B3hcY7hIHw/edit?gid=0#gid=0") %>% 
  as.data.frame() %>% 
  select(-form, -pid)

admit_processed <- admit_processed %>% 
  mutate(enr_age = as.numeric(enr_age),
         enr_approxage = as.numeric(enr_approxage),
         birthdate = as.Date(birthdate))

admit_raw_2 <- admit_raw_2 %>% 
  mutate(age = as.numeric(age),
         birthdate = as.Date(birthdate),
         age_months_approx = as.numeric(age_months_approx))

birthdate <- birthdate_changes %>%
  filter(to_change == "birthdate")

enr_approxage <- birthdate_changes %>%
  filter(to_change == "enr_approxage") %>%
  select(-new_date)

age_ext <- birthdate_changes %>%
  filter(to_change == "age")

enr_age <- birthdate_changes %>%
  filter(to_change == "enr_age")

weekly_processed <- weekly_processed %>% 
  mutate(wkl_age = as.numeric(wkl_age))

admit_processed <- admit_processed %>%
  left_join(enr_age, by = c("uuid" = "uuid")) %>% 
  mutate(
    enr_age = if_else(!is.na(new_value) & to_change == "enr_age", new_value, enr_age)) %>% 
  select(-new_value, -new_date, -to_change) 

admit_processed <- admit_processed %>%
  left_join(birthdate, by = c("uuid" = "uuid")) %>%
  mutate(
    birthdate = if_else(!is.na(new_date) & to_change == "birthdate", new_date, birthdate)) %>% 
  select(-new_value, -new_date, -to_change) 

admit_processed <- admit_processed %>%
  left_join(enr_approxage, by = c("uuid" = "uuid")) %>%
  mutate(
    enr_approxage = if_else(!is.na(new_value) & to_change == "enr_approxage", new_value, enr_approxage)) %>% 
  select(-new_value, -to_change)

admit_raw_2 <- admit_raw_2 %>%
  left_join(enr_approxage, by = c("id" = "uuid")) %>%
  mutate(
    age_months_approx = if_else(!is.na(new_value) & to_change == "enr_approxage", new_value, age_months_approx)) %>% 
  select(-new_value, -to_change)

admit_raw_2 <- admit_raw_2 %>%
  left_join(birthdate, by = c("id" = "uuid")) %>% 
  mutate(
    birthdate = if_else(!is.na(new_date) & to_change == "birthdate", new_date, birthdate)) %>% 
  select(-new_value, -new_date, -to_change) 

admit_raw_2 <- admit_raw_2 %>%
  left_join(age_ext, by = c("id" = "uuid")) %>%
  mutate(
    age = if_else(!is.na(new_value) & to_change == "enr_age", new_value, age)) %>% 
  select(-new_value, -new_date, -to_change)

weekly_processed <- weekly_processed %>%
  left_join(age_ext, by = c("uuid" = "uuid")) %>%
  mutate(
    wkl_age = if_else(!is.na(new_value) & to_change == "age", new_value, wkl_age)) %>% 
  select(-new_value, -new_date, -to_change)

weekly_raw_2 <- weekly_raw_2 %>%
  left_join(age_ext, by = c("id" = "uuid")) %>%
  mutate(
    age = if_else(!is.na(new_value) & to_change == "age", new_value, age)) %>% 
  select(-new_value, -new_date, -to_change)

# Removed z-scores

## Co-occurring anthro deficiencies

admit_raw_2 <- admit_raw_2 %>%
  mutate(bfeed_age = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    b_curr_bfeed == 'true' & age > 5 & age < 24 & str_detect(alt_foods_bfeed, 'food') ~ TRUE,
    age > 24 ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

admit_processed <- admit_processed %>% 
  mutate(b_wast = case_when(
    is.na(wfhz) ~ NA,
    is.na(hfaz) ~ NA, 
    wfhz < -3 & hfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_waz = case_when(
    is.na(muac) ~ NA,
    is.na(wfaz) ~ NA, 
    muac < 11.5 & wfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_wfh = case_when(
    is.na(muac) ~ NA,
    is.na(wfhz) ~ NA, 
    muac < 11.5 & wfhz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  select(-sex_num, -age_zscore, -standing)

weekly_processed <- weekly_processed %>% 
  mutate(b_wast = case_when(
    is.na(wfhz) ~ NA,
    is.na(hfaz) ~ NA, 
    wfhz < -3 & hfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_waz = case_when(
    is.na(muac) ~ NA,
    is.na(wfaz) ~ NA, 
    muac < 11.5 & wfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_wfh = case_when(
    is.na(muac) ~ NA,
    is.na(wfhz) ~ NA, 
    muac < 11.5 & wfhz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  select(-sex_num, -age_zscore, -standing)

admit_raw_2 <- admit_raw_2 %>% 
  mutate(b_wast = case_when(
    is.na(wfhz) ~ NA,
    is.na(hfaz) ~ NA, 
    wfhz < -3 & hfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_waz = case_when(
    is.na(muac) ~ NA,
    is.na(wfaz) ~ NA, 
    muac < 11.5 & wfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_wfh = case_when(
    is.na(muac) ~ NA,
    is.na(wfhz) ~ NA, 
    muac < 11.5 & wfhz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  select(-sex_num, -age_zscore, -standing)

weekly_raw_2 <- weekly_raw_2 %>% 
  mutate(b_wast = case_when(
    is.na(wfhz) ~ NA,
    is.na(hfaz) ~ NA, 
    wfhz < -3 & hfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_waz = case_when(
    is.na(muac) ~ NA,
    is.na(wfaz) ~ NA, 
    muac < 11.5 & wfaz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  mutate(b_muac_wfh = case_when(
    is.na(muac) ~ NA,
    is.na(wfhz) ~ NA, 
    muac < 11.5 & wfhz < -3 ~ TRUE,
    TRUE ~ FALSE)) %>% 
  select(-sex_num, -age_zscore, -standing)

### Breastfeeding for age ###
admit_raw_2 <- admit_raw_2 %>%
  mutate(bfeed_age = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    b_curr_bfeed == 'true' & age > 5 & age < 24 & str_detect(alt_foods_bfeed, 'food') ~ TRUE,
    age > 24 ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

weekly_raw_2 <- weekly_raw_2 %>%
  mutate(bfeed_age = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    b_curr_bfeed == 'true' & age > 5 & age < 24 & str_detect(alt_foods_bfeed, 'food') ~ TRUE,
    age > 24 ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

### Exclusive breastfeeding 
admit_raw_2 <- admit_raw_2 %>%
  mutate(bfeed_exc = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    age > 5 & age_takewater > 5 & age_takefamily > 5 ~ TRUE,
    age > 5 & is.na(age_takefamily) ~ NA,
    age > 5 & is.na(age_takewater) ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

### Exclusive breastfeeding, food only 
admit_raw_2 <- admit_raw_2 %>%
  mutate(bfeed_exc_food = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    age < 6 & b_curr_bfeed == 'true' & str_detect(alt_foods_bfeed, 'food') ~ TRUE,
    age > 5 & age_takefamily > 5 ~ TRUE,
    age > 5 & is.na(age_takefamily) ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

### Food introduced at correct age 
admit_raw_2 <- admit_raw_2 %>%
  mutate(bfeed_intro_food = case_when(
    age < 6 & b_curr_bfeed == 'true' & alt_foods_bfeed == 'false' ~ TRUE,
    age > 5 & age_takefamily == 6 ~ TRUE,
    age > 5 & is.na(age_takefamily) ~ NA,
    is.na(b_curr_bfeed) ~ NA,
    TRUE ~ FALSE
  ))

### Admitted during lean season 
admit_raw_2 <- admit_raw_2 %>%
  mutate(lean_season = month(todate) %in% c(6, 7, 8, 9),
         rainy_season = month(todate) %in% c(5, 6, 7, 8, 9, 10))

admit_processed <- admit_processed %>%
  mutate(lean_season = month(calcdate) %in% c(6, 7, 8, 9),
         rainy_season = month(calcdate) %in% c(5, 6, 7, 8, 9, 10))

### Visit during the lean seasn
weekly_raw_2 <- weekly_raw_2 %>%
  mutate(lean_season = month(todate) %in% c(6, 7, 8, 9),
         rainy_season = month(todate) %in% c(5, 6, 7, 8, 9, 10))

weekly_processed <- weekly_processed %>%
  mutate(lean_season = month(calcdate) %in% c(6, 7, 8, 9),
         rainy_season = month(calcdate) %in% c(5, 6, 7, 8, 9, 10))

### Exited program during lean season 
current_processed <- current_processed %>%
  mutate(lean_season = month(status_date) %in% c(6, 7, 8, 9),
         rainy_season = month(status_date) %in% c(5, 6, 7, 8, 9, 10))

### BRIANNA Save full CSV files locally ###
FULL_admit_processed <- admit_processed %>%
  filter(pid %in% current_pids)
full_admit_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_admit_processed_", Sys.Date(), ".csv")
write.csv(FULL_admit_processed, file = full_admit_processed_file_name, row.names = FALSE)

FULL_weekly_processed <- weekly_processed %>%
  filter(pid %in% current_pids)
full_weekly_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_weekly_processed_", Sys.Date(), ".csv")
write.csv(FULL_weekly_processed, file = full_weekly_processed_file_name, row.names = FALSE)

FULL_current_processed <- current_processed %>%
  filter(pid %in% current_pids)
full_current_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_current_processed_", Sys.Date(), ".csv")
write.csv(FULL_current_processed, file = full_current_processed_file_name, row.names = FALSE)

FULL_admit_raw_2 <- admit_raw_2 %>%
  filter(pid %in% current_pids)
full_admit_raw_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_admit_raw_", Sys.Date(), ".csv")
write.csv(FULL_admit_raw_2, file = full_admit_raw_processed_file_name, row.names = FALSE)

FULL_weekly_raw_2 <- weekly_raw_2 %>%
  filter(pid %in% current_pids)
full_weekly_raw_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_weekly_raw_", Sys.Date(), ".csv")
write.csv(FULL_weekly_raw_2, file = full_weekly_raw_processed_file_name, row.names = FALSE)

FULL_itp_roster <- itp_roster %>%
  filter(pid %in% current_pids)
full_itp_roster_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_itp_roster_", Sys.Date(), ".csv")
write.csv(FULL_itp_roster, file = full_itp_roster_file_name, row.names = FALSE)

FULL_relapse_raw_2 <- relapse_raw_2 %>%
  filter(pid %in% current_pids)
full_relapse_raw_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_relapse_raw", Sys.Date(), ".csv")
write.csv(FULL_relapse_raw_2, file = full_relapse_raw_file_name, row.names = FALSE)

FULL_mh_raw_2 <- mh_raw_2 %>%
  filter(pid %in% current_pids)
full_mh_raw_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/FULL_pba_mh_raw", Sys.Date(), ".csv")
write.csv(FULL_mh_raw_2, file = full_mh_raw_file_name, row.names = FALSE)

settlement_loc_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/pba_settlement_loc_", Sys.Date(), ".csv")
write.csv(settlement_loc2, file = settlement_loc_file_name, row.names = FALSE)

#files for google drive
FULL_admit_processed_for_google <- admit_processed %>%
  filter(pid %in% current_pids)
full_admit_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_admit_processed_2024-11-15.csv")
write.csv(FULL_admit_processed, file = full_admit_processed_file_name, row.names = FALSE)

FULL_weekly_processed_for_google <- weekly_processed %>%
  filter(pid %in% current_pids)
full_weekly_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_weekly_processed_2024-11-15.csv")
write.csv(FULL_weekly_processed, file = full_weekly_processed_file_name, row.names = FALSE)

FULL_current_processed_for_google <- current_processed %>%
  filter(pid %in% current_pids)
full_current_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_current_processed_2024-11-15.csv")
write.csv(FULL_current_processed, file = full_current_processed_file_name, row.names = FALSE)

FULL_admit_raw_2_for_google <- admit_raw_2 %>%
  filter(pid %in% current_pids)
full_admit_raw_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_admit_raw_2024-11-15.csv")
write.csv(FULL_admit_raw_2, file = full_admit_raw_processed_file_name, row.names = FALSE)

FULL_weekly_raw_2_for_google <- weekly_raw_2 %>%
  filter(pid %in% current_pids)
full_weekly_raw_processed_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_weekly_raw_2024-11-15.csv")
write.csv(FULL_weekly_raw_2, file = full_weekly_raw_processed_file_name, row.names = FALSE)

FULL_itp_roster_for_google <- itp_roster %>%
  filter(pid %in% current_pids)
full_itp_roster_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_itp_roster_2024-11-15.csv")
write.csv(FULL_itp_roster, file = full_itp_roster_file_name, row.names = FALSE)

FULL_relapse_raw_2_for_google <- relapse_raw_2 %>%
  filter(pid %in% current_pids)
full_relapse_raw_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_relapse_raw2024-11-15.csv")
write.csv(FULL_relapse_raw_2, file = full_relapse_raw_file_name, row.names = FALSE)

FULL_mh_raw_2_for_google <- mh_raw_2 %>%
  filter(pid %in% current_pids)
full_mh_raw_file_name <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/FULL_pba_mh_raw2024-11-15.csv")
write.csv(FULL_mh_raw_2, file = full_mh_raw_file_name, row.names = FALSE)

settlement_loc_file_name_for_google <- paste0("C:/Users/beale/Documents/Taimaka/taimaka_data/google/pba_settlement_loc_2024-11-15.csv")
write.csv(settlement_loc2, file = settlement_loc_file_name, row.names = FALSE)