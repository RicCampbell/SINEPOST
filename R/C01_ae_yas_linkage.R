## Script for creating lookup values and working on AE data

library(data.table)
library(ggplot2)
library(lubridate)

source("R/cleaning_fns_etl.r")

## Read in data

  ae_data <- readRDS("data/datasets/ae_processed_2021-07-12-110009.rds")
  yas_data <- load("data/datasets/epr_tables_datetime_changes-2021-07-14-100514.rda")
  
  
## Read in look up table created in A01
  
  study_id_encrypted_hesid_lookup <- readRDS("data/linkage/study_id_hesid_lookup_2021-07-12-105942.rds")
  
  
## Merge epr single table with epr point of care to get hospital information for all eprs (point of care is 1-to-1 to epr)
  
  stopifnot(epr_point_of_care_table[, .N, by = epr_id][N > 1, .N] == 0)
  
  epr_single_value_fields_table_poc <- merge(epr_single_value_fields_table,
                                             epr_point_of_care_table,
                                             by = "epr_id",
                                             all.x = TRUE)
  
  
## Merge in HESID to yas data single table then can used epr_id to link
  
  epr_single_value_fields_table_poc_HES_id <- merge(epr_single_value_fields_table_poc,
                                                    study_id_encrypted_hesid_lookup,
                                                    by.x = "patient_id",
                                                    by.y = "STUDY_ID",
                                                    all.x = TRUE)
  

## Merge in all AE arrival times for each person (HESID)

  yas_hes_linkage <- merge(epr_single_value_fields_table_poc_HES_id[transported_to_hospital == TRUE & receiving_hospital_department %chin% c("ED", "ED - Self Handover", "ED Self-handover/minors","ED Resus"),
                                                                    .(ENCRYPTED_HESID, incident_datetime, epr_id, patient_id)],
                           ae_data[, .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)],
                           by = "ENCRYPTED_HESID",
                           all.x = TRUE)


## Calculate the time between ambulance incident time, and arrival at A&E

  yas_hes_linkage[, incident_to_arrival_time := as.numeric(difftime(ARRIVALTIME, incident_datetime, unit = "hours"))]
  
  yas_hes_linkage[, incident_to_arrival_time_clipped := pmax(pmin(incident_to_arrival_time, 24.0), -24.0)]
  
  yas_hes_linkage[, incident_to_arrival_time_absolute := abs(incident_to_arrival_time)]


## Set order to find smallest absolute time between ambulance incident time and A&E arrival time - need an extra field to make sure sort is same everytime

  setorder(yas_hes_linkage, incident_to_arrival_time_absolute, na.last = TRUE)
  
  yas_hes_linkage[!is.na(incident_to_arrival_time_absolute), order := 1:.N, by = epr_id]


### Select the smallest absolute time that was within 24 hours, but also remove any cases that were within the first and last day of data

  smallest_time_diff_per_amb_inc_24_hour <- yas_hes_linkage[order == 1 &
                                                              incident_datetime >= floor_date(min(incident_datetime), unit = "day") + days(1) &
                                                              incident_datetime < floor_date(max(incident_datetime), unit = "day")][, order := NULL]
  
  
## Need to order the other way so as to keep AEKEY-epr_id 1-to-1, per_id added to sort to keep same each time
## Will this get rid of epr-(HES, incident_times) multiples, as HES-AEKEY-epr is now all 1-to-1?
  
  setorder(smallest_time_diff_per_amb_inc_24_hour, incident_to_arrival_time_absolute, epr_id, na.last = TRUE)
  
  smallest_time_diff_per_amb_inc_24_hour[!is.na(incident_to_arrival_time_absolute), order := 1:.N, by = AEKEY]
  
  smallest_time_diff_per_amb_inc_24_hour <- smallest_time_diff_per_amb_inc_24_hour[order == 1]


## Take records that are between 0 and 6 hours time duration, and only earliest in that time period *****
## Label last 5 (less than 6) hours of YAS data for this - field that indicates records are in this time-frame

## Add prefixes to field names in each table, and drop/rename common fields (HES_ID) after
  
  setnames(epr_single_value_fields_table_poc_HES_id, setdiff(colnames(epr_single_value_fields_table_poc_HES_id), "epr_id"),
           paste("epr", setdiff(colnames(epr_single_value_fields_table_poc_HES_id), "epr_id"), sep = "_"))
  
  setnames(ae_data, setdiff(colnames(ae_data), "AEKEY"),
           paste("ae", setdiff(colnames(ae_data), "AEKEY"), sep = "_"))
  
  

  yas_ae_data <- merge(merge(epr_single_value_fields_table_poc_HES_id,
                             smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time > 0 & incident_to_arrival_time < 6.0, .(epr_id, AEKEY)],
                             by = "epr_id",
                             all.x = TRUE),
                       ae_data,
                       by = "AEKEY",
                       all.x = TRUE)

## Check all HES ids match up, before renaming one and dropping the other
  
  stopifnot(yas_ae_data[epr_ENCRYPTED_HESID != ae_ENCRYPTED_HESID, .N] == 0)
  setnames(yas_ae_data, "ae_ENCRYPTED_HESID", "ENCRYPTED_HESID")
  yas_ae_data[, epr_ENCRYPTED_HESID := NULL]

  
## Do stopifnot checks
## Check that each epr_id is still only present once as only taken the first ae record per epr
  
  stopifnot(yas_ae_data[, .N, by = epr_id][N > 1, .N] == 0)
  
## Check that AEKEY only appears once, as should not be linked to two separate eprs, most recent (absolute) has been linked to
  
  stopifnot(yas_ae_data[!is.na(AEKEY), .N, by = AEKEY][N > 1, .N] == 0)
  
## Check that all AEKEYS found within 0-6 hours are present
  
  stopifnot(smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time > 0 & incident_to_arrival_time < 6.0, .N] == yas_ae_data[, .N])
  

  
  
# Re-attendances in YAS data ----------------------------------------------

## Set order so HED id and earliest incident datetime is first
  
  setorder(yas_ae_data, ENCRYPTED_HESID, epr_incident_datetime, na.last = TRUE)
  

## Find time between incidents for each person for all incidents
  
  yas_ae_data[!is.na(ENCRYPTED_HESID),
              duration_since_previous_incident := difftime(epr_incident_datetime, shift(epr_incident_datetime),  tz = "Europe/London", unit = "hours"),
              by = ENCRYPTED_HESID]
  
## Create field to label those incidents that happened within 24 hours of patients previous incident
  
  yas_ae_data[, previous_ems_attendence_within_24_hours := (duration_since_previous_incident < 24.0)]
  yas_ae_data[, duration_since_previous_incident := NULL]
  
  
## Set to NA all those incidents that occurred in the first day of the data as looking at whole 24 hour period
  
  yas_ae_data[epr_incident_datetime < floor_date(min(yas_ae_data$epr_incident_datetime)) + days(1), previous_ems_attendence_within_24_hours := NA]
  

# Save linked YAS-AE data -------------------------------------------------

  save_time <- getDateTimeForFilename()
  
  saveRDS(yas_ae_data, file = paste0("data/datasets/yas_ae_linked_", save_time, ".rds"))
  

