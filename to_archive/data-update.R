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

### Deleted mental health and relaspse study data

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

# Staff member cleaning is required for inference time

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

### Deleted relapse and mental health data

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

### Deleted geolocation data ###

### Cleaning of extreme values ###
admit_processed$ses_hh_adults[admit_processed$ses_hh_adults == 1511] <- NA
admit_raw_2$household_adults[admit_raw_2$household_adults == 1511] <- NA

admit_processed$finalhl[admit_processed$finalhl == 27.5] <- 70
admit_raw_2$hl[admit_raw_2$hl == 27.5] <- 70

admit_processed$finalhl[admit_processed$finalhl == 31] <- 72.2
admit_raw_2$hl[admit_raw_2$hl == 31] <- 72.2

admit_processed$finalhl[admit_processed$finalhl == 7.2] <- 82
admit_raw_2$hl[admit_raw_2$hl == 7.2] <- 82

# TODO: How to handle two visits in a single day at inference time? Check with Brian

### Deleted the cleaning of two visits in one day because they are now in the form_correction in SQL.

# Removed historical filtering of weekly processed data by UUID
# Removed age updates and admission extreme value processing

### Add z-scores ###
admit_processed <- admit_processed %>% 
  mutate(age = as.numeric(enr_age)) %>% 
  mutate(age_zscore = enr_age * (365.25 / 12),
         standing = ifelse(domhl == "height", 1, 
                           ifelse(domhl == "length", 2, 3)),
         sex_num = ifelse(sex == "male", 1, 
                          ifelse(sex == "female", 2, NA))) 

weekly_processed <- weekly_processed %>% 
  mutate(age = as.numeric(wkl_age)) %>% 
  mutate(age_zscore = wkl_age * (365.25 / 12),
         standing = ifelse(domhl == "height", 1, 
                           ifelse(domhl == "length", 2, 3)),
         sex_num = ifelse(sex == "male", 1, 
                          ifelse(sex == "female", 2, NA))) 

admit_raw_2 <- admit_raw_2 %>% 
  mutate(age = as.numeric(age)) %>% 
  mutate(age_zscore = age * (365.25 / 12),
         standing = ifelse(direction_of_measure == "height", 1, 
                           ifelse(direction_of_measure == "length", 2, 3)),
         sex_num = ifelse(c_sex == "male", 1, 
                          ifelse(c_sex == "female", 2, NA))) 

weekly_raw_2 <- weekly_raw_2 %>% 
  mutate(age_zscore = age * (365.25 / 12),
         standing = ifelse(direction_of_measure == "height", 1, 
                           ifelse(direction_of_measure == "length", 2, 3)),
         sex_num = ifelse(c_sex == "male", 1, 
                          ifelse(c_sex == "female", 2, NA))) 

library(zscorer)

admit_processed <- addWGSR(data = admit_processed, 
                           sex = "sex_num", 
                           firstPart = "weight",
                           secondPart = "finalhl", 
                           standing = "standing", 
                           index = "wfh")

admit_processed <- addWGSR(data = admit_processed, 
                           sex = "sex_num", 
                           firstPart = "finalhl",
                           secondPart = "age_zscore", 
                           standing = "standing", 
                           index = "hfa") 

admit_processed <- addWGSR(data = admit_processed, 
                           sex = "sex_num", 
                           firstPart = "weight",
                           secondPart = "age_zscore", 
                           index = "wfa") 

weekly_processed <- addWGSR(data = weekly_processed, 
                            sex = "sex_num", 
                            firstPart = "weight",
                            secondPart = "finalhl", 
                            standing = "standing", 
                            index = "wfh")

weekly_processed <- addWGSR(data = weekly_processed, 
                            sex = "sex_num", 
                            firstPart = "finalhl",
                            secondPart = "age_zscore", 
                            standing = "standing", 
                            index = "hfa") 

weekly_processed <- addWGSR(data = weekly_processed, 
                            sex = "sex_num", 
                            firstPart = "weight",
                            secondPart = "age_zscore", 
                            index = "wfa") 

admit_raw_2 <- addWGSR(data = admit_raw_2, 
                       sex = "sex_num", 
                       firstPart = "weight",
                       secondPart = "hl", 
                       standing = "standing", 
                       index = "wfh")

admit_raw_2 <- addWGSR(data = admit_raw_2, 
                       sex = "sex_num", 
                       firstPart = "hl",
                       secondPart = "age_zscore", 
                       standing = "standing", 
                       index = "hfa") 

admit_raw_2 <- addWGSR(data = admit_raw_2, 
                       sex = "sex_num", 
                       firstPart = "weight",
                       secondPart = "age_zscore", 
                       index = "wfa") 

weekly_raw_2 <- addWGSR(data = weekly_raw_2, 
                        sex = "sex_num", 
                        firstPart = "weight",
                        secondPart = "hl", 
                        standing = "standing", 
                        index = "wfh")

weekly_raw_2 <- addWGSR(data = weekly_raw_2, 
                        sex = "sex_num", 
                        firstPart = "hl",
                        secondPart = "age_zscore", 
                        standing = "standing", 
                        index = "hfa") 

weekly_raw_2 <- addWGSR(data = weekly_raw_2, 
                        sex = "sex_num", 
                        firstPart = "weight",
                        secondPart = "age_zscore", 
                        index = "wfa") 

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

### Deleted all the code used to save the files locally. Just need to make sure our outputs of this code match the inputs for Brian. 
