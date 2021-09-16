## Script for linking AE and YAS (single value table) data

library(data.table)
library(lubridate)

source("R/cleaning_fns_etl.r")

## Read in data

  ae_data <- readRDS("data/datasets/ae_processed_2021-08-02-152502.rds")
  yas_data <- load("data/datasets/cohort_processed_epr_table_2021-09-14-161940.rda")
  
  
## Read in look up table created in A01
  
  study_id_encrypted_hesid_lookup <- readRDS("data/linkage/study_id_hesid_lookup_2021-08-02-152440.rds")
  

## Check only one ENCRYPTED_HESID per AEKEY
  
  stopifnot(ae_data[, .N, by = .(ENCRYPTED_HESID, AEKEY)][, .N, by = AEKEY][N > 1, .N] == 0)
  
  
## Merge epr single table with epr point of care to get hospital information for all eprs (point of care is 1-to-1 to epr)
  
  stopifnot(epr_point_of_care_table[, .N, by = epr_id][N > 1, .N] == 0)
  
  epr_single_value_fields_table <- merge(epr_single_value_fields_table,
                                             epr_point_of_care_table,
                                             by = "epr_id",
                                             all.x = TRUE)


## Merge in all AE arrival times for each person (HESID)

  yas_hes_linkage <- merge(epr_single_value_fields_table[!is.na(ENCRYPTED_HESID) & transported_to_hospital == TRUE & receiving_hospital_department == "Emergency Department",
                                                                    .(ENCRYPTED_HESID, incident_datetime, epr_id, patient_id)],
                           ae_data[, .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)],
                           by = "ENCRYPTED_HESID",
                           all.x = TRUE)

  
## Calculate the time between ambulance incident time, and arrival at A&E

  yas_hes_linkage[, incident_to_arrival_time := as.numeric(difftime(ARRIVALTIME, incident_datetime, unit = "hours"))]
  
  
## Reduce to only AE records that happened with first 6 hours of ambulance incident time - done here so as ordering does not put minus times first
  
  yas_hes_linkage <- yas_hes_linkage[incident_to_arrival_time > 0 & incident_to_arrival_time < 6.0]

  
## Set order to find smallest time between ambulance incident time and A&E arrival time

  setorder(yas_hes_linkage, epr_id, incident_to_arrival_time, AEKEY, na.last = TRUE)
  
  yas_hes_linkage[!is.na(incident_to_arrival_time), order := 1:.N, by = epr_id]


### Select the smallest absolute time that was after incident time and within 6 hours - check ad-hoc script for distribution of +-24 hours which led to this decision

  smallest_time_diff_per_amb_inc_24_hour <- yas_hes_linkage[order == 1][, order := NULL]
  
  
## Need to order the other way so as to keep AEKEY-epr_id 1-to-1. epr_id included in sort to keep same each time

  setorder(smallest_time_diff_per_amb_inc_24_hour, incident_to_arrival_time, epr_id, na.last = TRUE)
  
  smallest_time_diff_per_amb_inc_24_hour[!is.na(incident_to_arrival_time), order := 1:.N, by = AEKEY]
  
  smallest_time_diff_per_amb_inc_24_hour <- smallest_time_diff_per_amb_inc_24_hour[order == 1]

  
## Check that it's all 1-to-1  
  
  stopifnot(smallest_time_diff_per_amb_inc_24_hour[, .N, by = .(epr_id, AEKEY)][, .N, by = epr_id][N > 1, .N] == 0)


## Add prefixes to field names in each table, and drop/rename common fields (HES_ID) after
  
  setnames(epr_single_value_fields_table, setdiff(colnames(epr_single_value_fields_table), "epr_id"),
           paste("epr", setdiff(colnames(epr_single_value_fields_table), "epr_id"), sep = "_"))
  
  setnames(ae_data, setdiff(colnames(ae_data), "AEKEY"),
           paste("ae", setdiff(colnames(ae_data), "AEKEY"), sep = "_"))
  
  

  yas_ae_data <- merge(merge(epr_single_value_fields_table,
                             smallest_time_diff_per_amb_inc_24_hour[, .(epr_id, AEKEY)],
                             by = "epr_id",
                             all.x = TRUE),
                       ae_data,
                       by = "AEKEY",
                       all.x = TRUE)

## Check all HES ids match up, before renaming one and dropping the other
  
  stopifnot(yas_ae_data[epr_ENCRYPTED_HESID != ae_ENCRYPTED_HESID, .N] == 0)
  setnames(yas_ae_data, "ae_ENCRYPTED_HESID", "ENCRYPTED_HESID")
  yas_ae_data[, epr_ENCRYPTED_HESID := NULL]

  
## Re-make time to ae field
  
  yas_ae_data[, ambulance_incident_to_ae_arrival := as.numeric(difftime(ae_ARRIVALTIME, epr_incident_datetime, unit = "hours"))]

    
## Check that each epr_id is still only present once as only taken the first AE record per epr
  
  stopifnot(yas_ae_data[, .N, by = epr_id][N > 1, .N] == 0)
  
## Check that AEKEY only appears once, as should not be linked to two separate eprs, most recent (absolute) has been linked to
  
  stopifnot(yas_ae_data[!is.na(AEKEY), .N, by = AEKEY][N > 1, .N] == 0)
  
## Check that all AEKEYS found within 0-6 hours are present
  
  stopifnot(setdiff(smallest_time_diff_per_amb_inc_24_hour[, AEKEY], yas_ae_data[, AEKEY]) == 0)
  
## Check all times are between 0 and 6 hours, or NA
  
  stopifnot(yas_ae_data[ambulance_incident_to_ae_arrival < 0 & ambulance_incident_to_ae_arrival > 6.0, .N] == 0)

## Check that all epr_ids were kept
  
  stopifnot(yas_ae_data[, .N] == epr_single_value_fields_table[, .N])


# Non-conveyed AE attendances ----------------------------------------------
## Look at if there was any AE attendance within 24 hours of ambulance time for those not said were conveyed
  
  
## Merge all epr and all AE records together (includes non-transported to hospital, not ED destination)
  
  all_yas_ae_linked <- merge(epr_single_value_fields_table[!is.na(epr_ENCRYPTED_HESID), .(epr_ENCRYPTED_HESID, epr_incident_datetime, epr_id)],
                             ae_data[, .(ae_ENCRYPTED_HESID, ae_ARRIVALTIME, AEKEY, ae_low_acuity_attendance)],
                             by.x = "epr_ENCRYPTED_HESID",
                             by.y = "ae_ENCRYPTED_HESID",
                             all.x = TRUE) 
  

## Find time between ambulance incident time and AE arrival time, and keep only those within 24 hours
  
  all_yas_ae_linked[, incident_time_to_arrival_time := as.numeric(difftime(ae_ARRIVALTIME, epr_incident_datetime, unit = "hours"))]
  
  attendance_24_hours <- all_yas_ae_linked[incident_time_to_arrival_time > 0 & incident_time_to_arrival_time <= 24.0]
  

## Count number of attendances within 24 hours for each epr
  
  attendance_24_hours[, number_ae_24_hour_attendances := sum(.N, na.rm = TRUE), by = epr_id]
  
  
## Where all attendances 'low-acuity'?
## 'all' works NA how we want; mix/false -> false, mix of true/NA -> NA, all true -> true, 
  
  attendance_24_hours[, all_low_acutity_24_hours := all(ae_low_acuity_attendance), by = epr_id]
  
  attendance_24_hours <- unique(attendance_24_hours[, .(epr_id, number_ae_24_hour_attendances, all_low_acutity_24_hours)])
  
  stopifnot(attendance_24_hours[, .N, by = epr_id][N > 1, .N] == 0)

  
## Merge back into yas_ae table
  
  yas_ae_data <- merge(yas_ae_data,
                       attendance_24_hours[, .(epr_id, number_ae_24_hour_attendances, all_low_acutity_24_hours)],
                       by = "epr_id",
                       all.x = TRUE)  
  
# Save linked YAS-AE data -------------------------------------------------

  save_time <- getDateTimeForFilename()
  
  saveRDS(yas_ae_data, file = paste0("data/datasets/yas_ae_linked_", save_time, ".rds"))
  

