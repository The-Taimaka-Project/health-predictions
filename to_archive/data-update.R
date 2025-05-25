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
  password = "bobbing-tidy-untagged"
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
  pw = 'deutsch6TH', 
  verbose = TRUE
)

admit_raw <- odata_submission_get(
  table = "Submissions", 
  url = "https://taimaka-internal.org:7443",
  wkt=TRUE,
  download=FALSE,
  parse=FALSE
) %>% odata_submission_rectangle(names_sep=NULL)

### Assignment of PIDs ###

# Edit in OTP data only, typically reflecting a PID later assigned to a child presenting with an emergency sign: 
admit_raw <- admit_raw %>%
  mutate(id = gsub("^uuid:", "", id)) %>% 
  mutate(pid = if_else(is.na(pid) & name == "Badariya hashimu " & todate == '2023-05-31', "23-0211", pid), # cross-checked address
         pid = if_else(is.na(pid) & name == "Al amin sidi ali " & todate == '2023-05-31', "23-0210", pid), # cross-checked address
         pid = if_else(is.na(pid) & name == "Ibrahim sadiq" & todate == '2023-05-31', "23-0203", pid), 
         pid = if_else(is.na(pid) & name == "Abdullazeez Rufai" & todate == '2023-05-31', "23-0212", pid), 
         pid = if_else(is.na(pid) & name == "Abubakar sunusi " & todate == '2023-05-31', "23-0204", pid), 
         pid = if_else(is.na(pid) & name == "Farida ali" & todate == '2023-05-31', "23-0205", pid), 
         pid = if_else(is.na(pid) & name == "Muhammadu Faisal samaila" & todate == '2023-05-31', "23-0206", pid), 
         pid = if_else(is.na(pid) & name == "Yusuf auwal " & todate == '2023-05-31', "23-0202", pid), 
         pid = if_else(is.na(pid) & name == "Salma adamu " & todate == '2023-05-31', "23-0201", pid),
         pid = if_else(is.na(pid) & name == "Hussaini sulaiman " & todate == '2023-06-05', "23-0219", pid), # cross-checked address 
         pid = if_else(is.na(pid) & name == "Kamaluddeen muhammadu" & todate == '2023-06-08', "23-0281", pid),
         pid = if_else(is.na(pid) & name == "Muhammad Muhammad " & todate == '2023-07-03', "23-0431", pid),
         pid = if_else(is.na(pid) & name == "Hafiz sanusi" & todate == '2023-07-04', "23-0295", pid), # cross-checked address and CG name
         pid = if_else(is.na(pid) & name == "Muhammad abdullahi " & todate == '2023-07-04', "23-0428", pid), # cross-checked address and CG name
         pid = if_else(is.na(pid) & name == "Marsiya abubakar" & todate == '2023-05-31', "23-0214", pid), # em OTP submission listed CG name in place of child name, leader name in place of settlement
         pid = if_else(is.na(pid) & name == "Saadatu zahradin" & todate == '2023-06-02', "23-0286", pid), # traced using phone number, not in ITP Google Sheet
         pid = if_else(is.na(pid) & name == "Abubakar Khalid" & todate == '2023-07-03', "23-0390", pid), # cross-checked address, CG name, phone
         pid = if_else(is.na(pid) & name == "Nura ayayaji" & todate == '2023-07-05', "23-0429", pid), # cross-checked CG and HoH names, but address does not match
         pid = if_else(is.na(pid) & name == "Usaina nasiru " & todate == '2023-07-11', "23-0868", pid), # cross-checked CG name and address
         pid = if_else(is.na(pid) & name == "Shaaban Muhammad " & todate == '2023-07-11', "23-0545", pid), # appears child returned to OTP the next day and was assigned a PID, but didn't go to ITP until the following week
         pid = if_else(is.na(pid) & name == "Aishatu abdullahi " & todate == '2023-07-12', "23-0435", pid), # cross-checked CG name and address
         pid = if_else(is.na(pid) & name == "Fatima Abdulhamid " & todate == '2023-09-25', "23-1404", pid), # cross-checked CG and and address
         pid = if_else(is.na(pid) & name == "Asmau Mohammed " & todate == '2023-10-31', "23-3160", pid), # cross-checked CG name
         pid = if_else(is.na(pid) & name == "Ibrahim Ibrahim " & todate == '2023-10-03', "23-0870", pid),
         pid = if_else(is.na(pid) & name == "Aisha Babangida" & todate == '2023-10-26', "23-1420", pid), #cross-checked CG name and child age
         pid = if_else(is.na(pid) & name == "Balkisu isah " & todate == '2023-11-16', "23-0872", pid), 
         pid = if_else(is.na(pid) & name == "Yasir lawan" & todate == '2023-11-16', "23-0871", pid),
         pid = if_else(is.na(pid) & name == "Muhammad abdussalam" & todate == '2024-01-08', "24-0366", pid),
         pid = if_else(is.na(pid) & name == "Sulaiman Yahaya " & todate == '2023-11-28', "23-3088", pid),
         pid = if_else(is.na(pid) & name == "Abdullahi Mohammed " & todate == '2023-12-04', "23-1419", pid), # cross-checked CG name and child age
         pid = if_else(is.na(pid) & name == "Salisu musa" & todate == '2024-01-23', "24-1415", pid),
         pid = if_else(is.na(pid) & name == "Usman Adamu " & todate == '2024-02-14', "24-0851", pid),
         pid = if_else(is.na(pid) & name == "Aishatu Bello" & todate == '2024-02-27', "24-0865", pid),
         pid = if_else(is.na(pid) & name == "Adamu Abdullahi " & todate == '2024-02-27', "24-1063", pid),
         pid = if_else(is.na(pid) & name == "Basiru Muhammad " & todate == '2024-02-29', "24-0854", pid),
         pid = if_else(is.na(pid) & name == "Abdulkadir muhd " & todate == '2024-03-11', "24-0853", pid),
         pid = if_else(is.na(pid) & name == "Ahmadu Muhammad " & todate == '2024-03-14', "24-0858", pid),
         pid = if_else(is.na(pid) & name == "Zulai Adamu " & todate == '2024-03-18', "24-0861", pid),
         pid = if_else(is.na(pid) & name == "Habiba Bello" & todate == '2024-03-18', "24-0859", pid),
         pid = if_else(is.na(pid) & name == "Adamu Ibrahim " & todate == '2024-03-21', "24-0860", pid),
         pid = if_else(is.na(pid) & name == "Aisha Hussaini" & todate == '2024-04-04', "24-1228", pid),
         pid = if_else(is.na(pid) & name == "Amina muhammadu " & todate == '2024-04-15', "24-1227", pid),
         pid = if_else(is.na(pid) & name == "Samaila Muhammadu " & todate == '2024-04-18', "24-1524", pid),
         pid = if_else(is.na(pid) & name == "Muhammad Musa " & todate == '2024-03-11', "24-1518", pid),
         pid = if_else(is.na(pid) & name == "Aisha Yusuf " & todate == '2024-05-07', "24-1937", pid),
         pid = if_else(is.na(pid) & name == "Musa Adamu " & todate == '2024-05-13', "24-1954", pid),
         pid = if_else(is.na(pid) & name == "hussaina babangida" & todate == '2024-03-18', "24-0980", pid),
         pid = if_else(is.na(pid) & name == "Abdul mudallab auwal" & todate == '2024-03-25', "24-0981", pid),
         pid = if_else(is.na(pid) & name == "Ainau ayuba" & todate == '2024-05-06', "24-1606", pid),
         pid = if_else(is.na(pid) & name == "Habiba Abubakar" & todate == '2024-04-26', "24-1942", pid), # cross-checked CG name and address, but age mismatches
         pid = if_else(is.na(pid) & name == "Muhd Adamu" & todate == '2024-04-26', "24-1935", pid), # cross-checked with phone number
         pid = if_else(is.na(pid) & name == "Abdullahi Usman " & todate == '2024-04-29', "24-1938", pid), # cross-checked with phone number
         pid = if_else(is.na(pid) & name == "Musa Yahaya" & todate == '2024-05-09', "24-1758", pid),
         pid = if_else(is.na(pid) & name == "Maryama Muhd" & todate == '2024-05-15', "24-1958", pid),
         pid = if_else(is.na(pid) & name == "Abubakar muhammadu " & todate == '2024-07-05', "24-1607", pid),
         pid = if_else(is.na(pid) & name == "Muhammad Musa " & todate == '2024-04-22', "24-1518", pid),
         pid = if_else(is.na(pid) & name == "Amina haruna" & todate == '2024-05-23', "24-1950", pid),
         pid = if_else(is.na(pid) & name == "Adamu jauro" & todate == '2024-05-27', "24-1956", pid),
         pid = if_else(is.na(pid) & name == "Muhammad Muhammad " & todate == '2024-06-05', "24-2128", pid),
         pid = if_else(is.na(pid) & name == "Aisha Abdussalam" & todate == '2024-06-13', "24-1957", pid),
         pid = if_else(is.na(pid) & name == "Aliyu Abubakar " & todate == '2024-06-19', "24-1944", pid),
         pid = if_else(is.na(pid) & name == "Saidu Musa" & todate == '2024-06-26', "24-2752", pid),
         pid = if_else(is.na(pid) & name == "Amina Abdullahi " & todate == '2024-06-27', "24-2751", pid),
         pid = if_else(is.na(pid) & name == "Hussaina Musa " & todate == '2024-07-01', "24-2756", pid),
         pid = if_else(is.na(pid) & name == "Maryam Muhammad " & todate == '2024-07-01', "24-2757", pid),
         pid = if_else(is.na(pid) & name == "Hassana Saidu " & todate == '2024-07-08', "24-2830", pid),
         pid = if_else(is.na(pid) & name == "Aisha Ahmadu " & todate == '2024-08-14', "24-2761", pid),
         pid = if_else(is.na(pid) & name == "Abubakar Ibrahim " & todate == '2024-09-10', "24-4137", pid),
         pid = if_else(is.na(pid) & name == "Hassana Wada " & todate == '2024-09-25', "24-2753", pid),
         pid = if_else(is.na(pid) & name == "Muhd muhd" & todate == '2024-09-27', "24-4147", pid),
         pid = if_else(is.na(pid) & name == "Abdullahi Muhd" & todate == '2024-07-22', "24-2763", pid),
         pid = if_else(is.na(pid) & name == "Hauwa Muhammad " & todate == '2024-09-30', "24-2765", pid),
         pid = if_else(is.na(pid) & name == "Ali Adamu" & todate == '2023-07-03', "23-9999", pid), # ITP, no PID assigned
         pid = if_else(is.na(pid) & name == "Muhammad lawanda " & todate == '2023-07-04', "23-9998", pid), # ITP, no PID assigned
         pid = if_else(is.na(pid) & name == "Laure Adamu " & todate == '2023-07-14', "23-9997", pid), # ITP, no PID assigned
         pid = if_else(is.na(pid) & name == "Aisha Umaru " & todate == '2024-01-31', "24-9999", pid), # ITP, no PID assigned
         pid = if_else(is.na(pid) & name == "Zainab Tijjani " & todate == '2024-10-21', "24-9991", pid), # ITP, no PID assigned
         pid = if_else(is.na(pid) & name == "Habiba Muhd" & todate == '2023-10-02', "23-9996", pid), # NO MATCH - unable to trace child of similar name 
         pid = if_else(is.na(pid) & name == "Muhammad Muhammad " & todate == '2023-11-20', "23-9995", pid), # NO MATCH - closest possible is 23-3101 - unable to trace child of similar name, location, and date; phone number logged with 3 other CGs, not admitted via ITP 
         pid = if_else(is.na(pid) & name == "Aisha Muhammad" & todate == '2024-03-25', "24-9997", pid), # NO MATCH - unable to trace child of similar name, location, and date; phone number not logged with any other observations, child of same name but different age enrolled in ITP
         pid = if_else(is.na(pid) & name == "Fatu Magaji " & todate == '2024-07-17', "24-9996", pid), # NO MATCH - unable to trace w/ name or phone
         pid = if_else(is.na(pid) & name == "Bello Umar" & todate == '2024-07-19', "24-9995", pid), # NO MATCH - unable to trace; phone links to 24-2126, but seems to be different child
         pid = if_else(is.na(pid) & name == "Hassan umaru " & todate == '2024-09-09', "24-9994", pid), # NO MATCH - unable to trace; phone does not link
         pid = if_else(is.na(pid) & name == "Mommy Ibrahim" & todate == '2024-10-17', "24-9993", pid), # NO MATCH - unable to trace; no phone
         pid = if_else(is.na(pid) & name == "Fatima Rabiu " & todate == '2024-09-02', "24-9992", pid), #  NO MATCH - unable to trace; phone does not link 
         pid = if_else(is.na(pid) & id == "dc384bb2-3cb8-4ad8-ad3b-9e2e3a8762ff", "24-9989", pid), # ITP, no PID assigned before death
         pid = if_else(is.na(pid) & id == "9bfd1025-6099-4b44-97f6-908842b38dc7", "24-4831", pid),
         pid = if_else(is.na(pid) & id == "0c5cc61e-2f05-42d4-8dd1-284cf2d2db62", "24-9988", pid) #  NO MATCH - unable to trace; phone does not link 
  ) 

### Edit in ITP data only:
# Maryam Muhd Sangaru - Excluded because age at time was 20 days, referred to SBCU - enrolled as 24-2757 upon reaching age of 1m 
# Muhd Yahaya (Sangaru) - 24-1953 - for some reason not recorded in ITP sheet

### Construct PIDs in OTP data, ITP data, and current:
#pid = if_else(is.na(pid) & name == "Ali Adamu" & todate == '2023-07-03', "23-9999", pid), # ITP, no PID assigned
#pid = if_else(is.na(pid) & name == "Muhammad lawanda " & todate == '2023-07-04', "23-9998", pid), # ITP, no PID assigned
#pid = if_else(is.na(pid) & name == "Laure Adamu " & todate == '2023-07-14', "23-9997", pid), # ITP, no PID assigned
#pid = if_else(is.na(pid) & name == "Aisha Umaru " & todate == '2024-01-31', "24-9999", pid), # ITP, no PID assigned
#pid = if_else(is.na(pid) & name == "Zainab Tijjani " & todate == '2024-10-21', "24-9991", pid),

### Construct PIDs in ITP data and current: 
# Saifullahi Yahaya - included in Brianna's data 24-9998
# Abdullahi Ismail - Deba - discharged home from ITP as not from catchment area - 24-9990 
# Hassan Usman (Kurjale) - No notes in ITP log - 24-0001 --> update on 15/11: this child's PID is 24-1613

### Construct PIDs in OTP data and current: 
# Set status to default
# pid = if_else(is.na(pid) & name == "Habiba Muhd" & todate == '2023-10-02', "23-9996", pid), # NO MATCH - unable to trace child of similar name 
# pid = if_else(is.na(pid) & name == "Muhammad Muhammad " & todate == '2023-11-20', "23-9995", pid), # NO MATCH - closest possible is 23-3101 - unable to trace child of similar name, location, and date; phone number logged with 3 other CGs, not admitted via ITP 
# pid = if_else(is.na(pid) & name == "Aisha Muhammad" & todate == '2024-03-25', "24-9997", pid), # NO MATCH - unable to trace child of similar name, location, and date; phone number not logged with any other observations, child of same name but different age enrolled in ITP
# pid = if_else(is.na(pid) & name == "Fatu Magaji " & todate == '2024-07-17', "24-9996", pid), # NO MATCH - unable to trace w/ name or phone
# pid = if_else(is.na(pid) & name == "Bello Umar" & todate == '2024-07-19', "24-9995", pid), # NO MATCH - unable to trace; phone links to 24-2126, but seems to be different child
# pid = if_else(is.na(pid) & name == "Hassan umaru " & todate == '2024-09-09', "24-9994", pid), # NO MATCH - unable to trace; phone does not link
# pid = if_else(is.na(pid) & name == "Mommy Ibrahim" & todate == '2024-10-17', "24-9993", pid), # NO MATCH - unable to trace; no phone
# pid = if_else(is.na(pid) & name == "Fatima Rabiu " & todate == '2024-09-02', "24-9992", pid), #  NO MATCH - unable to trace; phone does not link 

# Create a dataframe with new observations for CURRENT and join to current 
add_current <- data.frame(
  pid = c("23-9999", "23-9998", "23-9997", "24-9999", "24-9991", "24-9998", "24-9990", 
          "23-9996", "23-9995", "24-9997", "24-9996", "24-9995", "24-9994", "24-9993", "24-9992",
          "24-9989", "24-9988"),
  phone = c("701-729-1416", NA, "903-395-7213", "902-236-5364", NA, "803-769-9436", NA,
            NA, "806-380-3885", "708-720-7492", "706-611-7182", "903-754-3375", "802-670-5817", NA, "901-131-0883",
            "704-738-1330", "703-379-5483"),
  b_phoneconsent = c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA),
  langpref = c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA),
  phoneowner = c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA),
  site = c("sangaru", "kuri", "jalingo", "sangaru", "kuri", "kurjale", NA,
           "sangaru", "sangaru", "sangaru", "sangaru", "sangaru", "sangaru", "sangaru", "jalingo",
           "kuri", "sangaru"),
  status = c("dead", "dead", "excluded", "dead", "dead", "default", "excluded", 
             "default", "default", "default", "default", "default", "default", "default", "default", 
             "dead", "default"), 
  status_detail = c(NA, NA, "defect", NA, NA, "final", "other", 
                    "final", "final", "final", "final", "final", "final", "final", "final",
                    NA, "returnable"), # returnable will need to be updated next time this is run
  movement_detail = c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA),
  status_date = as.Date(c("2023-07-05", "2023-07-04", "2023-07-26", "2024-02-02", "2024-10-23", "2024-03-04", "2024-03-13", 
                          "2023-10-23", "2023-12-11", "2024-03-15", "2024-07-05", "2024-07-05", "2024-10-07", "2024-11-11", "2024-09-30",
                          "2024-12-05", "2024-12-09")), 
  nvdate = as.Date(c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA)),
  los = c(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
  dischqualanthro = c(FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE))

new_pids <- c("23-9999", "23-9998", "23-9997", "24-9999", "24-9991", "24-9998", "24-9990", 
              "23-9996", "23-9995", "24-9997", "24-9996", "24-9995", "24-9994", "24-9993", "24-9992", 
              "24-9989", "24-9988")

emage <- admit_raw %>% 
  filter(pid %in% new_pids) %>% 
  select(em_age, pid, todate)

add_current <- add_current %>%
  left_join(emage, by = 'pid') %>%
  mutate(
    em_age = case_when(
      pid == '24-9998' ~ 2,
      pid == '24-9990' ~ 10,
      TRUE ~ em_age
    ),
    todate = as.Date(coalesce(
      case_when(
        pid == '24-9998' ~ '2024-02-15',
        pid == '24-9990' ~ '2024-03-04',
      ), as.character(todate)), 
      format = "%Y-%m-%d")) %>% 
  mutate(age = as.integer(em_age + interval(todate, Sys.Date()) %/% months(1))
  ) %>%
  select(-em_age, -todate)

current <- bind_rows(current, add_current)

### Create a dataframe with new observations for EXCLUSIONS and join to exclusions 
add_exclusions <- data.frame(
  updt_id = c(NA, NA),
  pid = c("23-9997", "24-9990"),
  dt = as.Date(c("2023-07-26", "2024-03-13")), 
  exclusion_type = c("defect", "other"),
  notes = c("Per ITP, excluded as after receiving acre as a result of cerebral palsy. Excluded without assigning PID; PID 23-9997 assigned retroactively by Jenn during data cleaning.", 
            "Per ITP, discharged Home as patient is not from catchment area. No PID assigned; PID 24-9990 assigned retroactively by Jenn during data cleaning."),
  change_timestamp = c(NA, NA))

exclusions <- bind_rows(exclusions, add_exclusions)

### Create a dataframe with new observations for DEATHS and join to deaths
add_deaths <- data.frame(
  pid = c("23-9999", "23-9998", "24-9999", "24-9991", "24-9989"),
  dod = as.Date(c("2023-07-05", "2023-07-04", "2024-02-02", "2024-10-23", "2024-12-05")), 
  notes = c("Per ITP, patient died on transit to FTH (tertiary facility). Died in ITP care, before PID assigned; PID 23-9999 assigned retroactively by Jenn during data cleaning.", 
            "Per ITP, patient died on arrival to ITP, before PID was assigned. PID 23-9998 assigned retroactively by Jenn during data cleaning.",
            "Died in ITP before PID assigned; PID 24-9999 assigned retroactively by Jenn during data cleaning.",
            "Died in ITP before PID assigned; PID 24-9991 assigned retroactively by Jenn during data cleaning.",
            "Died in ITP before PID assigned; PID 24-9989 assigned retroactively by Jenn during data cleaning."),
  change_timestamp = c(NA, NA, NA, NA, NA))

deaths <- bind_rows(deaths, add_deaths)

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

# Create a dataframe with new observations for ADMIT RAW and join to admit_raw
add_admit_raw <- data.frame(
  pid = c("24-9998", "24-9990"),
  todate = as.Date(c("2024-02-15", "2024-03-04")),
  site = c("gh_deba", "gh_deba"),
  site_type = c("itp", "itp"),
  b_cpalate = c("false", "false"),
  b_hydrocephalus = c("false", "false"),
  b_prolap_hernia = c("false", "false"),
  b_downsynd = c("false", "false"),
  b_sicklecell = c("false", "false"),
  b_def_oth = c("false", "false"),
  b_fract_traum = c("false", "false"),
  def_pres = c("false", "false"),
  state = c("gombe", "other"),
  other_state = c(NA, "unknown"),
  name = c("Saifullahi Yahaya", "Abdullahi Ismail"),
  c_sex = c("male", "male"),
  age = c(2, 10),
  muac = c(7.2, 10),
  muac_status = c("sam", "sam"),
  maln_status = c("sam", "sam"),
  eref_u6sam = c("true", "false"),
  ref_overall = c("true", "true"),
  eff_eref_overall = c("true", NA),
  eff_ref_overall  = c("true", "true"),
  q_override_eref = c("false", NA),
  q_cgaccept_eref = c("true", NA),
  override_eref = c("false", NA),
  b_correctadmittype = c("true", "true"),
  precalc_admit_type_pp = c("New Admission", "New Admission"),
  admit_type = c("new", "new"),
  q_override_lref = c(NA, "false"),
  override_lref = c(NA, "false"),
  q_cgaccept_lref = c(NA, "true"),
  eff_lref_overall = c(NA, "true"),
  cat1_overall = c(NA, "true"),  
  id = c("fc41d7c0-1803-400b-81da-40d96f747497", "4e0d9d40-e3b5-45db-8584-4b271bbd7dd9"), 
  additionalnotes = c("Presented in ITP and discharged as stabilized. Not assigned PID b/c the ITP stocked out of admission forms. Pt never returned to OTP and was never assigned a PID. PID 24-9998 assigned retroactively by Jenn during data cleaning.",
                      "Presented in ITP. Treated, then excluded due to being from outsite the catchment area. Not assigned PID by ITP staff. PID 24-9990 assigned retroactively by Jenn during data cleaning."))

admit_raw <- admit_raw %>% 
  mutate(todate = as.Date(todate), 
         age = as.integer(age),
         muac = as.numeric(muac)) 

admit_raw <- bind_rows(admit_raw, add_admit_raw)

imci_emergency <- admit_raw %>% 
  filter(imci_emergency_otp == 'true')

imci_pids <- imci_emergency$pid

admit_raw <- admit_raw %>% 
  mutate(cleaning_note = ifelse(pid %in% new_pids, 'PID assigned retroactively during data cleaning.', NA),
         cleaning_note = ifelse(pid %in% imci_pids & imci_emergency_otp == 'true', "Presented as emergency case at OTP and referred to ITP with no PID assigned; linked with PID retroactively during data cleaning.", NA)
  )

# Create a dataframe with new observations for ADMIT PROCESSED and join to admit_processed
vector_of_names <- names(admit)

add_admit_processed <- admit_raw %>% 
  filter(pid %in% new_pids) %>% 
  mutate(md_starttime = as.Date(start_time),
         md_endtime = as.Date(end_time),
         autosite = glbsite,
         calcdate = as.Date(todate),
         staffmember = final_staffmember,
         staffmember_other = otherstaff,
         c_imci_emergency = imci_emergency_list,
         imci_emergency_other = spec_imci_em_other,
         b_referred_emergency = as.logical(imci_emergency_otp),
         b_presented_emergency = as.logical(imci_emergency_itp),
         b_fracture_trauma = as.logical(b_fract_traum),
         b_defect_present = as.logical(def_pres),
         b_prevenr = as.logical(b_prevenr),
         where_referred_emergency = imci_receivingitp,
         settlement_other	= other_settlement,
         prevenr_approx_start = as.Date(approx_start),
         prevenr_approx_end = as.Date(approx_end),
         c_prevstatus	= prev_status,
         sex = coalesce(c_sex, em_c_sex),
         b_agedocument = as.logical(age_valid),
         enr_approxage = as.integer(age_months_approx),
         enr_age = coalesce(age, em_age),
         enr_age = as.integer(enr_age),
         b_heightcheck = as.logical(age_height_check),
         b_reachcheck = as.logical(age_reach_check), 
         b_tooold = as.logical(b_too_old),
         domhl = direction_of_measure,
         finalhl = as.double(hl),
         roundedhl = as.double(hl_rounded),
         backuplength = as.double(length),
         ts_assessed_malnstatus = c_physician_assess,
         ts_assessed_needitp = as.logical(b_phys_req_itp),
         ms_wfh = wfh_maln_status,
         ms_muac = muac_status,
         ms_oedema = oedema_status,
         ms_overall = maln_status,
         b_hastwin = as.logical(b_twin), 
         muac = as.double(muac),
         b_twinattended	= as.logical(b_twinhere),
         tw_name = twin_name,
         tw_malnstatus = c_twin_ms,
         tw_pid = twin_pid,
         ref_g6u4kg = as.logical(eref_g6u4kg),
         ref_tsref = as.logical(eref_tsref),
         ref_u6sam = as.logical(eref_u6sam),
         ref_oedg3 = as.logical(eref_oedg3),
         ref_oedsam = as.logical(eref_oedsam),
         b_needsitp = as.logical(ref_overall),
         b_wasreferred = as.logical(eff_ref_overall),
         b_mamneedsitp = as.logical(mam_needsitp),
         cg_age = as.integer(cg_age),
         tradleader_name = tradleader,
         phoneowner	= final_phone_owner,
         b_phoneconsent	= as.logical(phone_consent),
         langpref	= lang_pref,
         gave_al_act = as.logical(fgive_al_act),
         ttype = final_ttype,
         dose = as.double(final_dose),
         dose_wtwin = as.double(final_dose_wtwin),
         rationweeks = as.integer(final_numweeksback),
         uuid = id,
         md_submissiondate = as.Date(submission_date),
         md_submitterid = submitter_id,
         md_submittername = submitter_name,
         md_attachmentspres = as.integer(attachments_present),
         md_attachmentsexp = as.integer(attachments_expected), 
         md_status = status, 
         md_reviewstate = review_state,
         md_deviceid = device_id,
         md_edits = as.integer(edits),
         cg_name = coalesce(cg_name, em_cg_name),
         homedesc = coalesce(homedesc, em_homedesc),
         phone = coalesce(phone, em_phone),
         b_fatheralive = as.logical(father_alive),
         b_motheralive = as.logical(mother_alive),
         b_isvaxed = as.logical(b_vaccinated),
         b_twinalive = as.logical(b_twinalive),
         ses_b_foodsecurity = as.logical(foodsecurity),
         ses_care_decisionmaker = care_decisionmaker,
         ses_care_decisionmaker_specify = care_decisionmaker_specify,
         ses_drinkingwater = drinking_water,
         ses_edtype_father = ed_type_father,
         ses_edtype_mother = ed_type_mother,
         ses_hh_adults = household_adults,
         ses_hh_slept = household_slept,
         ses_livingchildren = living_children,
         ses_toilet = toilet,
         ses_walltype = wall_type,
         week = NA, 
         enr_week = NA,
         b_movenextvisit = as.logical(final_movevisit),
         pt_photo = patient_picture,
         c_cgrelationship = cg_relationship,
         b_knowsbday = as.logical(b_knowsbday),
         birthdate = as.Date(birthdate),
         weight = as.double(weight),
         finalhl = as.double(hl), 
         roundedlhl = as.double(roundedhl),
         b_cgishoh = as.logical(b_cgishoh),
         manual_nvdate = as.Date(manual_nvdate),
         rec_birthvax = which_birthvax,
         rec_6wvax = which_6wvax,
         rec_10wvax = which_10wvax,
         rec_14wvax = which_10wvax,
         b_hadbirthvax = as.logical(b_hadbirthvax),
         b_had6wvax = as.logical(b_had6wvax),
         b_had10wvax = as.logical(b_had10wvax),
         b_had14wvax = as.logical(b_had14wvax),
         vd_birth = as.Date(vd_birth),
         vd_6w = as.Date(vd_6w),
         vd_10w = as.Date(vd_10w),
         vd_14w = as.Date(vd_14w),
         b_rota1diff = as.logical(b_rota1diff),
         b_ipv1diff = as.logical(b_ipv1diff),
         b_rota2diff = as.logical(b_rota2diff),
         b_rota3diff = as.logical(b_rota3diff),
         b_ipv2diff = as.logical(b_ipv2diff),
         vd_vita1 = as.Date(vd_vita1),
         vd_measles1 = as.Date(vd_measles1),
         vd_yf = as.Date(vd_yf),
         vd_menin = as.Date(vd_menin),
         vd_vita2 = as.Date(vd_vita2),
         vd_measles2 = as.Date(vd_measles2),
         supp_vd_rota1 = as.Date(supp_vd_rota1),
         supp_vd_ipv1 = as.Date(supp_vd_ipv1),
         supp_vd_rota2 = as.Date(supp_vd_rota2),
         supp_vd_rota3 = as.Date(supp_vd_rota3),
         supp_vd_ipv2 = as.Date(supp_vd_ipv2),
         dayssincevita = as.logical("")
  ) %>% 
  select(all_of(setdiff(vector_of_names, "wfhz")))

admit <- bind_rows(admit, add_admit_processed)

admit <- admit %>% 
  mutate(cleaning_note = ifelse(pid %in% new_pids, 'PID assigned retroactively during data cleaning.', NA),
         cleaning_note = ifelse(pid %in% imci_pids & b_referred_emergency == TRUE, "Presented as emergency case at OTP and referred to ITP with no PID assigned; linked with PID retroactively during data cleaning.", NA)
  )

### Load and clean ITP data ###
library(googlesheets4)
library(stringr)

itp_roster_2023 <- read_sheet("https://docs.google.com/spreadsheets/d/1Pb_bGGaHRIyzwHhIBzebFqQhd-6HQttWKDeTvRR58-o/edit?gid=0#gid=0") %>%
  as.data.frame() 

itp_roster_2023_2 <- itp_roster_2023 %>% 
  filter(!is.na(Facility)) %>% 
  mutate(itp = Facility,
         otp = OTP,
         age = as.character(`Age (months)`),
         sex = `Sex (m/f)`,
         initial_dx = Diagnosis,
         outcome = Outcome,
         outcome_date = coalesce(`Outcome date`, LOS),
         pid = `Reg. No`,
         los_days = as.character(`Week #`),
         admit_date = as.Date(`Date of admission`, format = "%Y-%m-%d"),
         outcome_date = as.Date(outcome_date, format = "%Y-%m-%d"),
         ref_w_pid = Yes,
         case_notes = paste(`Mgt checked in folder`, notes_2, notes_3, sep = ", "),
         mgt_checked_folder = `Checked date`,
         case_notes = case_notes %>%
           str_replace_all("NA, ", "") %>%     
           str_replace_all(", NA", "") %>%   
           str_replace_all(", , ", ", ") %>%   
           str_replace_all("^NA$", "") %>%
           str_replace_all("2023-11-08", "") %>%
           str_replace_all("2023-10-03", "") %>%
           str_replace_all("2023-09-26", "") %>%
           str_replace_all("2023-09-18", "")) %>% 
  mutate(pid = if_else(pid == '-' & Name == "Mohmmed Lawadu", "23-9998", pid),
         pid = if_else(pid == '-' & Name == "Ali Adamu", "23-9999", pid),
         pid = if_else(pid == '-' & Name == "Laure Adamu", "23-9997", pid)) %>% 
  select(-`Reg. No`, -`Week #`, -Name, -LOS, -`Outcome date`, -`Referred with Reg. No`,
         -Yes, -`Mgt checked in folder`, -notes_2, -notes_3, -`Checked date`,
         -`Date of admission`, -Facility, -OTP, -`Age (months)`, -`Sex (m/f)`,
         -Diagnosis, -Outcome)

itp_roster_2023_2$week_num <- unlist(itp_roster_2023_2$week_num)

itp_roster_2024 <- read_sheet("https://docs.google.com/spreadsheets/d/1kWdcIL7ajRxHfmtS7R3BNkUzUgEac-XCWlzM4xNlkuA/edit?pli=1&gid=0#gid=0") %>%
  as.data.frame() 

itp_roster_2024$Pid[itp_roster_2024$Pid == "24--4534"] <- "24-4534"

itp_roster_2024_2 <- itp_roster_2024 %>%
  mutate(sex = `Sex (m/f)`,
         itp = Facility,
         otp = OTP,
         initial_dx = `Initial Diagnosis`,
         outcome = Outcome,
         final_dx = `Final diagnosis`,
         age = as.character(`Age (months)`),
         muac = as.character(`MUAC (cm)`), 
         pid = Pid,
         admit_date = as.Date(`Date of Admission`, format = "%Y-%m-%d"),
         outcome_date = as.Date(`Outcome Date`, format = "%Y-%m-%d"),
         los_days = as.character(LOS),
         ref_w_pid = `Referred with PID`,
         case_notes = paste(Note, ...18, `Folder checked`, sep = ", "),
         case_notes = case_notes %>%
           str_replace_all("NA, ", "") %>%     
           str_replace_all(", NA", "") %>%   
           str_replace_all(", , ", ", ") %>%   
           str_replace_all("^NA$", "")) %>%
  mutate(pid = if_else(pid == '-' & Name == "Aisha Umar", "24-9999", pid),
         pid = if_else(pid == '-' & Name == "Saifullahi Yahaya", "24-9998", pid),
         pid = if_else(pid == '-' & Name == "Abdullahi Ismail", "24-9990", pid),
         pid = if_else(pid == '-' & Name == "Maryam Muhd", "24-2757", pid),
         pid = if_else(pid == '-' & Name == "zainab Tijjani", "24-9991", pid),
         pid = if_else(pid == '-' & Name == "Hassan Usman", "24-0001", pid),
         pid = if_else(pid == '-' & Name == "Muhd Yahaya", "24-1953", pid),
         pid = if_else(pid == '-' & Name == "Jamila Lawai", "24-9989", pid)) %>% 
  select(-Pid, -`Age (months)`, -`MUAC (cm)`, -`Date of Admission`, 
         -`Outcome Date`, -LOS, -`Folder checked`, -`Referred with PID`,
         -Note, -`?prob`, -...18, -Name, -Facility, -OTP, -`Sex (m/f)`,
         -`Initial Diagnosis`, -`Outcome`, -`Final diagnosis`)

itp_roster_2024_2$muac <- unlist(itp_roster_2024_2$muac)
itp_roster_2024_2$age <- unlist(itp_roster_2024_2$age)

itp_roster_2025 <- read_sheet("https://docs.google.com/spreadsheets/d/11LqjmNJeHNLirCaYSijpJms0Rp1NZbJDKZ0WxtSAqkk/edit?gid=0#gid=0") %>%
  as.data.frame() 

itp_roster_2025_2 <- itp_roster_2025 %>% 
  mutate(sex = `Sex (m/f)`,
         itp = Facility,
         otp = OTP,
         initial_dx = `Initial Diagnosis`,
         outcome = Outcome,
         final_dx = `Final diagnosis`,
         age = as.character(`Age (months)`),
         muac = as.character(`MUAC (cm)`), 
         pid = Pid,
         admit_date = as.Date(`Date of Admission`, format = "%Y-%m-%d"),
         outcome_date = as.Date(`Outcome Date`, format = "%Y-%m-%d"),
         los_days = as.character(LOS),
         ref_w_pid = `Referred with PID`,
         case_notes = paste(Note, ...18, `Folder checked`, sep = ", "),
         case_notes = case_notes %>%
           str_replace_all("NA, ", "") %>%     
           str_replace_all(", NA", "") %>%   
           str_replace_all(", , ", ", ") %>%   
           str_replace_all("^NA$", "")) %>%
  select(-Pid, -`Age (months)`, -`MUAC (cm)`, -`Date of Admission`, 
         -`Outcome Date`, -LOS, -`Folder checked`, -`Referred with PID`,
         -Note, -`?prob`, -...18, -Name, -Facility, -OTP, -`Sex (m/f)`,
         -`Initial Diagnosis`, -`Outcome`, -`Final diagnosis`) %>% 
  mutate(los_days = as.character(outcome_date - admit_date))

itp_roster <- bind_rows(itp_roster_2024_2, itp_roster_2023_2, itp_roster_2025_2) %>% 
  mutate(otp = ifelse(otp == 'Kurjelli', 'kurjale', otp),
         otp = ifelse(otp == 'PHC Kurjele', 'kurjale', otp),
         otp = ifelse(otp == 'Ashaka', 'jalingo', otp),
         otp = ifelse(otp == 'PHC Ashaka', 'jalingo', otp),
         otp = ifelse(otp == 'Kuri', 'kuri', otp),
         otp = ifelse(otp == 'PHC Kuri', 'kuri', otp),
         otp = ifelse(otp == 'Sangaru', 'sangaru', otp),
         otp = ifelse(otp == 'PHC Sangaru', 'sangaru', otp),
         itp = ifelse(itp == 'FTH', 'fth', itp),
         itp = ifelse(itp == 'SSHG', 'ssh', itp),
         itp = ifelse(itp == 'GH Deba', 'gh_deba', itp),
         itp = ifelse(itp == 'GH Bajoga', 'gh_bajoga', itp), 
         age = ifelse(age == '20days', '0.67', age),
         age = ifelse(age == 'NULL', NA, age),
         sex = ifelse(sex == 'M|', 'm', sex),
         sex = ifelse(sex == 'M', 'm', sex),
         sex = ifelse(sex == 'F', 'f', sex),
         muac = ifelse(muac == '-', NA, muac),
         muac = ifelse(muac == 'NULL', NA, muac),
         muac = as.numeric(muac),
         los_days = ifelse(los_days == 'NULL', NA, los_days),
         case_notes = ifelse(los_days == 'Treated as out patient', 'Treated as out patient', case_notes),
         los_days = ifelse(los_days == 'Treated as out patient', NA, los_days),
         los_days = as.numeric(los_days)) %>% 
  unique() 

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
  pw = 'deutsch6TH', 
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

### Load Relapse Study Data ###
ru_setup(
  svc = 'https://taimaka-internal.org:7443/v1/projects/12/forms/relapse.scv',
  un = 'jennifer@taimaka.org',
  pw = 'deutsch6TH', 
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
  pw = 'deutsch6TH', 
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
  pw = 'deutsch6TH', 
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

# ### Select 70% of PIDs for TRAINING SET on Nov 2nd ###
# library(googledrive)
# library(readr)
# 
# # Define the file ID
# training_set_id <- "1g8a_DTuCjcaFVz5RjM1y-vxjvwmREbdN"
# 
# # Download the file to a temporary location
# drive_download(as_id(training_set_id), path = "training_set.csv", overwrite = TRUE)
# 
# training_pids <- read_csv("training_set.csv")
# 
# selected_pids <- training_pids$x

### Cleaning of extreme values ###
admit_processed$ses_hh_adults[admit_processed$ses_hh_adults == 1511] <- NA
admit_raw_2$household_adults[admit_raw_2$household_adults == 1511] <- NA

admit_processed$finalhl[admit_processed$finalhl == 27.5] <- 70
admit_raw_2$hl[admit_raw_2$hl == 27.5] <- 70

admit_processed$finalhl[admit_processed$finalhl == 31] <- 72.2
admit_raw_2$hl[admit_raw_2$hl == 31] <- 72.2

admit_processed$finalhl[admit_processed$finalhl == 7.2] <- 82
admit_raw_2$hl[admit_raw_2$hl == 7.2] <- 82

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

weekly_processed <- weekly_processed %>% 
  filter(uuid != "73aa0d09-7144-49e1-88e8-8c57205db19d", # corresponds to 23-1597 on Dec. 19, 2023
         uuid != "9b449658-1d0c-481c-8276-47a607bceab5", # corresponds to 23-1850 on Dec. 29, 2023
         uuid != "f18b0301-b961-40e0-852f-51d74dbbc209", # corresponds to 23-0559 (actually admit for 23-0554) on July 20, 2023
         uuid != "ee475f11-b25e-4432-a7c0-9f020a5ec0b5", # corresponds to 24-0315 on April 19, 2024
         uuid != "bc97606e-ea4f-43af-bb6c-756517ab10bd", # corresponds to 24-0999 on April 15, 2024
         uuid != "bd5b39ea-65b1-480a-92f8-ae0276758303", # corresponds to 24-3024 on August 6, 2024
         uuid != "325db6da-007f-4029-82da-494b3ab03169", # corresponds to 23-0379 on Aug. 18, 2023
         uuid != "92f64f3a-dc03-4ca9-a978-4df70a93c44d", # corresponds to 23-0749 on Aug. 15, 2023
         uuid != "fb59b4e2-86f4-41ee-9664-cd69753a00ef", # corresponds to 23-1142 on Sep. 12, 2023
         uuid != "ce58d36d-74f7-42a7-bb71-33859f7be8b6", # corresponds to 24-3397 on Sep. 20, 2024
         uuid != "8cc30cb7-0dfc-408a-b4cf-dd2af460fb98", # corresponds to 23-2132 on Oct. 10, 2023 
         uuid != "394e5fbf-9065-4b09-bc6c-b765fd9b98fc", # corresponds to 24-3183 on Sep. 24, 2024, T1 (morning) (T2, in afternoon, ended in ITP referral)
         uuid != "3750d3a2-a77d-4eef-bc48-f4464cf58699", # corresponds to 24-4503 on Oct. 25, 2024 
         uuid != "8b3d7c2c-f788-4ba2-bfdd-e38716c3bb08", # corresponds to 23-1165 on Apr. 09, 2023
         uuid != "99bed840-23d6-4e8e-9096-0cc23e30880b", # corresponds to 23-1429 on Oct. 16, 2023
         uuid != "b8924426-df7d-47f2-adb2-684e093e69c0", # corresponds to 23-2909 on Jan. 02, 2024
         uuid != "a0f0d748-e151-4041-b8e6-7e2055bf6d84", # corresponds to 23-0231 on Jun. 06, 2023
         uuid != "d68537f3-b597-4669-9e7f-f78ef5240ceb", # corresponds to 23-0284 on Jun. 06, 2023
         uuid != "7bd966dd-cd7e-4b6d-b4ee-15a5f4b29b4a", # corresponds to 23-0359 on Jun. 06, 2023
         uuid != "ca49b128-d06c-4068-b957-42536be87b47", # corresponds to 23-1111 on Sep. 13, 2023
         uuid != "51c9b45d-056b-4982-87f2-07f1d3e90aad", # corresponds to 23-1737 on Oct. 10, 2023
         uuid != "a54f3197-e91f-4bdf-b04f-099e55a9c520", # corresponds to 23-1809 on Nov. 16, 2023
         uuid != "cd0e2916-abec-4c30-bbee-24d1225ebaec", # corresponds to 23-2917 on Nov. 21, 2023
         uuid != "5d24e7b1-a2fd-4b4c-92a6-708ca3d25546", # corresponds to 24-0757 on Mar. 07, 2024
         uuid != "d24520c1-cf3e-41fe-9eb4-adcb901253f9", # corresponds to 24-3539 on Nov. 08, 2024
         uuid != "c75495e2-1d09-4fa8-9432-78ed3c452718", # corresponds to 24-3728 on Oct. 11, 2024
         uuid != "458ec137-d9ee-4c54-b217-e9332ef4a054", # corresponds to 24-3729 on Oct. 11, 2024
         uuid != "3619fa38-d4c1-49b8-a2d6-30e6bf07ee6a", # corresponds to 23-1259 on Sep. 25, 2023
         uuid != "5cf1b4c2-48bc-499c-b111-02f442f63ccc", # corresponds to 23-0897 on Jul. 31, 2023, filtered this spike in MUAC and weight
         uuid != "74ea71a0-a6ab-4b45-bc9b-1acc334cbdae") # corresponds to 23-3261 on Jan. 24, 2024


weekly_raw_2 <- weekly_raw_2 %>% 
  filter(id != "73aa0d09-7144-49e1-88e8-8c57205db19d", # corresponds to 23-1597 on Dec. 19, 2023
         id != "9b449658-1d0c-481c-8276-47a607bceab5", # corresponds to 23-1850 on Dec. 29, 2023
         id != "f18b0301-b961-40e0-852f-51d74dbbc209", # corresponds to 23-0559 (actually admit for 23-0554) on July 20, 2023
         id != "ee475f11-b25e-4432-a7c0-9f020a5ec0b5", # corresponds to 24-0315 on April 19, 2024
         id != "bc97606e-ea4f-43af-bb6c-756517ab10bd", # corresponds to 24-0999 on April 15, 2024
         id != "bd5b39ea-65b1-480a-92f8-ae0276758303", # corresponds to 24-3024 on August 6, 2024
         id != "325db6da-007f-4029-82da-494b3ab03169", # corresponds to 23-0379 on Aug. 18, 2023
         id != "92f64f3a-dc03-4ca9-a978-4df70a93c44d", # corresponds to 23-0749 on Aug. 15, 2023
         id != "fb59b4e2-86f4-41ee-9664-cd69753a00ef", # corresponds to 23-1142 on Sep. 12, 2023
         id != "ce58d36d-74f7-42a7-bb71-33859f7be8b6", # corresponds to 24-3397 on Sep. 20, 2024
         id != "8cc30cb7-0dfc-408a-b4cf-dd2af460fb98", # corresponds to 23-2132 on Oct. 10, 2023 
         id != "394e5fbf-9065-4b09-bc6c-b765fd9b98fc", # corresponds to 24-3183 on Sep. 24, 2024, T1 (morning) (T2, in afternoon, ended in ITP referral)
         id != "3750d3a2-a77d-4eef-bc48-f4464cf58699", # corresponds to 24-4503 on Oct. 25, 2024 
         id != "8b3d7c2c-f788-4ba2-bfdd-e38716c3bb08", # corresponds to 23-1165 on Apr. 09, 2023
         id != "99bed840-23d6-4e8e-9096-0cc23e30880b", # corresponds to 23-1429 on Oct. 16, 2023
         id != "b8924426-df7d-47f2-adb2-684e093e69c0", # corresponds to 23-2909 on Jan. 02, 2024
         id != "a0f0d748-e151-4041-b8e6-7e2055bf6d84", # corresponds to 23-0231 on Jun. 06, 2023
         id != "d68537f3-b597-4669-9e7f-f78ef5240ceb", # corresponds to 23-0284 on Jun. 06, 2023
         id != "7bd966dd-cd7e-4b6d-b4ee-15a5f4b29b4a", # corresponds to 23-0359 on Jun. 06, 2023
         id != "ca49b128-d06c-4068-b957-42536be87b47", # corresponds to 23-1111 on Sep. 13, 2023
         id != "51c9b45d-056b-4982-87f2-07f1d3e90aad", # corresponds to 23-1737 on Oct. 10, 2023
         id != "a54f3197-e91f-4bdf-b04f-099e55a9c520", # corresponds to 23-1809 on Nov. 16, 2023
         id != "cd0e2916-abec-4c30-bbee-24d1225ebaec", # corresponds to 23-2917 on Nov. 21, 2023
         id != "5d24e7b1-a2fd-4b4c-92a6-708ca3d25546", # corresponds to 24-0757 on Mar. 07, 2024
         id != "d24520c1-cf3e-41fe-9eb4-adcb901253f9", # corresponds to 24-3539 on Nov. 08, 2024
         id != "c75495e2-1d09-4fa8-9432-78ed3c452718", # corresponds to 24-3728 on Oct. 11, 2024
         id != "458ec137-d9ee-4c54-b217-e9332ef4a054", # corresponds to 24-3729 on Oct. 11, 2024
         id != "3619fa38-d4c1-49b8-a2d6-30e6bf07ee6a", # corresponds to 23-1259 on Sep. 25, 2023
         id != "5cf1b4c2-48bc-499c-b111-02f442f63ccc", # corresponds to 23-0897 on Jul. 31, 2023, filtered this spike in MUAC and weight
         id != "74ea71a0-a6ab-4b45-bc9b-1acc334cbdae") # corresponds to 23-3261 on Jan. 24, 2024

# For errors, run:
# googlesheets4::gs4_deauth()

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

# ### Filter for 70% and save TRAINING set CSV files locally ###
# TRAIN_admit_processed <- admit_processed %>% 
#   filter(pid %in% selected_pids)
# train_admit_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_admit_processed_", Sys.Date(), ".csv")
# write.csv(TRAIN_admit_processed, file = train_admit_processed_file_name, row.names = FALSE)
# 
# FULL_admit_processed <- admit_processed %>% 
#   filter(pid %in% current_pids)
# full_admit_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_admit_processed_", Sys.Date(), ".csv")
# write.csv(FULL_admit_processed, file = full_admit_processed_file_name, row.names = FALSE)
# 
# TRAIN_weekly_processed <- weekly_processed %>% 
#   filter(pid %in% selected_pids)
# train_weekly_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_weekly_processed_", Sys.Date(), ".csv")
# write.csv(TRAIN_weekly_processed, file = train_weekly_processed_file_name, row.names = FALSE)
# 
# FULL_weekly_processed <- weekly_processed %>% 
#   filter(pid %in% current_pids)
# full_weekly_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_weekly_processed_", Sys.Date(), ".csv")
# write.csv(FULL_weekly_processed, file = full_weekly_processed_file_name, row.names = FALSE)
# 
# TRAIN_current_processed <- current_processed %>% 
#   filter(pid %in% selected_pids)
# train_current_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_current_processed_", Sys.Date(), ".csv")
# write.csv(TRAIN_current_processed, file = train_current_processed_file_name, row.names = FALSE)
# 
# FULL_current_processed <- current_processed %>% 
#   filter(pid %in% current_pids)
# full_current_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_current_processed_", Sys.Date(), ".csv")
# write.csv(FULL_current_processed, file = full_current_processed_file_name, row.names = FALSE)
# 
# TRAIN_admit_raw_2 <- admit_raw_2 %>% 
#   filter(pid %in% selected_pids)
# train_admit_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_admit_raw_", Sys.Date(), ".csv")
# write.csv(TRAIN_admit_raw_2, file = train_admit_raw_file_name, row.names = FALSE)
# 
# FULL_admit_raw_2 <- admit_raw_2 %>% 
#   filter(pid %in% current_pids)
# full_admit_raw_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_admit_raw_", Sys.Date(), ".csv")
# write.csv(FULL_admit_raw_2, file = full_admit_raw_processed_file_name, row.names = FALSE)
# 
# TRAIN_weekly_raw_2 <- weekly_raw_2 %>% 
#   filter(pid %in% selected_pids)
# train_weekly_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_weekly_raw_", Sys.Date(), ".csv")
# write.csv(TRAIN_weekly_raw_2, file = train_weekly_raw_file_name, row.names = FALSE)
# 
# FULL_weekly_raw_2 <- weekly_raw_2 %>% 
#   filter(pid %in% current_pids)
# full_weekly_raw_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_weekly_raw_", Sys.Date(), ".csv")
# write.csv(FULL_weekly_raw_2, file = full_weekly_raw_processed_file_name, row.names = FALSE)
# 
# TRAIN_itp_roster <- itp_roster %>% 
#   filter(pid %in% selected_pids)
# train_itp_roster_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_itp_roster_", Sys.Date(), ".csv")
# write.csv(TRAIN_itp_roster, file = train_itp_roster_file_name, row.names = FALSE)
# 
# FULL_itp_roster <- itp_roster %>% 
#   filter(pid %in% current_pids)
# full_itp_roster_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_itp_roster_", Sys.Date(), ".csv")
# write.csv(FULL_itp_roster, file = full_itp_roster_file_name, row.names = FALSE)
# 
# TRAIN_relapse_raw_2 <- relapse_raw_2 %>% 
#   filter(pid %in% selected_pids)
# train_relapse_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_relapse_raw_", Sys.Date(), ".csv")
# write.csv(TRAIN_relapse_raw_2, file = train_relapse_raw_file_name, row.names = FALSE)
# 
# FULL_relapse_raw_2 <- relapse_raw_2 %>% 
#   filter(pid %in% current_pids)
# full_relapse_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_relapse_raw_", Sys.Date(), ".csv")
# write.csv(FULL_relapse_raw_2, file = full_relapse_raw_file_name, row.names = FALSE)
# 
# TRAIN_mh_raw_2 <- mh_raw_2 %>% 
#   filter(pid %in% selected_pids)
# train_mh_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TRAIN_pba_mh_raw_", Sys.Date(), ".csv")
# write.csv(TRAIN_mh_raw_2, file = train_mh_raw_file_name, row.names = FALSE)
# 
# FULL_mh_raw_2 <- mh_raw_2 %>% 
#   filter(pid %in% current_pids)
# full_mh_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/FULL_pba_mh_raw_", Sys.Date(), ".csv")
# write.csv(FULL_mh_raw_2, file = full_mh_raw_file_name, row.names = FALSE)
# 
# settlement_loc_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/pba_settlement_loc_", Sys.Date(), ".csv")
# write.csv(settlement_loc2, file = settlement_loc_file_name, row.names = FALSE)
# 
# ### Filter for 30% and save TEST SET CSV files locally ###
# TEST_admit_processed <- admit_processed %>% 
#   filter(!pid %in% selected_pids)
# test_admit_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_admit_processed_", Sys.Date(), ".csv")
# write.csv(TEST_admit_processed, file = test_admit_processed_file_name, row.names = FALSE)
# 
# TEST_weekly_processed <- weekly_processed %>% 
#   filter(!pid %in% selected_pids)
# test_weekly_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_weekly_processed_", Sys.Date(), ".csv")
# write.csv(TEST_weekly_processed, file = test_weekly_processed_file_name, row.names = FALSE)
# 
# TEST_current_processed <- current_processed %>% 
#   filter(!pid %in% selected_pids)
# test_current_processed_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_current_processed_", Sys.Date(), ".csv")
# write.csv(TEST_current_processed, file = test_current_processed_file_name, row.names = FALSE)
# 
# TEST_admit_raw_2 <- admit_raw_2 %>% 
#   filter(!pid %in% selected_pids)
# test_admit_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_admit_raw_", Sys.Date(), ".csv")
# write.csv(TEST_admit_raw_2, file = test_admit_raw_file_name, row.names = FALSE)
# 
# TEST_weekly_raw_2 <- weekly_raw_2 %>% 
#   filter(!pid %in% selected_pids)
# test_weekly_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_weekly_raw_", Sys.Date(), ".csv")
# write.csv(TEST_weekly_raw_2, file = test_weekly_raw_file_name, row.names = FALSE)
# 
# TEST_itp_roster <- itp_roster %>% 
#   filter(!pid %in% selected_pids)
# test_itp_roster_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_itp_roster_", Sys.Date(), ".csv")
# write.csv(TEST_itp_roster, file = test_itp_roster_file_name, row.names = FALSE)
# 
# TEST_relapse_raw_2 <- relapse_raw_2 %>% 
#   filter(!pid %in% selected_pids)
# test_relapse_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_relapse_raw_", Sys.Date(), ".csv")
# write.csv(TEST_relapse_raw_2, file = test_relapse_raw_file_name, row.names = FALSE)
# 
# TEST_mh_raw_2 <- mh_raw_2 %>% 
#   filter(!pid %in% selected_pids)
# test_mh_raw_file_name <- paste0("C:/Users/ostro/taimaka_data/relapse/pba/TEST_pba_mh_raw_", Sys.Date(), ".csv")
# write.csv(TEST_mh_raw_2, file = test_mh_raw_file_name, row.names = FALSE)
# 
#
# # ### Upload TRAINING set files to Drive - change file name to match 1st upload ###
# library(googledrive)
# drive_auth()
#
# # Specify folder ID
# folder_id <- "1GD0yNK4g87mvQNRKP9vETtbwZu4Fi_J2"
#
# # Upload the CSV file to Google Drive
#
# drive_upload(
#   media = train_admit_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_admit_processed_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_weekly_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_weekly_processed_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_current_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_current_processed_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_admit_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_admit_raw_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_weekly_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_weekly_raw_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_itp_roster_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_itp_roster_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_relapse_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_relapse_raw_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = train_mh_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("train_pba_mh_raw_2024-11-02.csv"),
#   overwrite = TRUE
# )
#
# drive_upload(
#   media = settlement_loc_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("pba_settlement_loc_2024-11-02.csv"),
#   overwrite = TRUE
# )

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

# ### Upload FULL set files to Drive - change file name to match 1st upload ###
# library(googledrive)
# drive_auth()

# Specify folder ID
# folder_id <- "17DQT6R53YIo5oHJyeepZc_7Gn7xOn1Vh"

# Upload the CSV file to Google Drive

# drive_upload(
#   media = full_admit_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_admit_processed_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_weekly_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_weekly_processed_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_current_processed_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_current_processed_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_admit_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_admit_raw_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_weekly_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_weekly_raw_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_itp_roster_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_itp_roster_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_relapse_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("FULL_pba_relapse_raw_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = full_mh_raw_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("full_pba_mh_raw_2024-11-15.csv"),
#   overwrite = TRUE
# )
# 
# drive_upload(
#   media = settlement_loc_file_name,
#   path = as_id(folder_id),
#   type = "text/csv",
#   name = paste0("pba_settlement_loc_2024-11-15.csv"),
#   overwrite = TRUE
# )