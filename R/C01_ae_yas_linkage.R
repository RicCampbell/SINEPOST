## Script for linking AE and YAS (single value table) data

library(data.table)
library(lubridate)

source("R/cleaning_fns_etl.r")

## Read in data

  ae_data <- readRDS("data/datasets/ae_processed_2021-07-20-093110.rds")
  yas_data <- load("data/datasets/cohort_processed_epr_table_2021-07-20-170623.rda")
  
  
## Read in look up table created in A01
  
  study_id_encrypted_hesid_lookup <- readRDS("data/linkage/study_id_hesid_lookup_2021-07-12-105942.rds")
  

## Check only one  ENCRYPTED_HESID per AEKEY
  
  stopifnot(ae_data[, .N, by = .(ENCRYPTED_HESID, AEKEY)][, .N, by = AEKEY][N > 1, .N] == 0)
  
  
## Merge epr single table with epr point of care to get hospital information for all eprs (point of care is 1-to-1 to epr)
  
  stopifnot(epr_point_of_care_table[, .N, by = epr_id][N > 1, .N] == 0)
  
  epr_single_value_fields_table <- merge(epr_single_value_fields_table,
                                             epr_point_of_care_table,
                                             by = "epr_id",
                                             all.x = TRUE)


## Merge in all AE arrival times for each person (HESID)

  yas_hes_linkage <- merge(epr_single_value_fields_table[transported_to_hospital == TRUE & receiving_hospital_department == "Emergency Department",
                                                                    .(ENCRYPTED_HESID, incident_datetime, epr_id, patient_id)],
                           ae_data[, .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)],
                           by = "ENCRYPTED_HESID",
                           all.x = TRUE)

  
## Calculate the time between ambulance incident time, and arrival at A&E

  yas_hes_linkage[, incident_to_arrival_time := as.numeric(difftime(ARRIVALTIME, incident_datetime, unit = "hours"))]
  
  yas_hes_linkage[, incident_to_arrival_time_clipped := pmax(pmin(incident_to_arrival_time, 24.0), -24.0)]
  
  yas_hes_linkage[, incident_to_arrival_time_absolute := abs(incident_to_arrival_time)]


## Set order to find smallest absolute time between ambulance incident time and A&E arrival time

  setorder(yas_hes_linkage, epr_id, incident_to_arrival_time_absolute, AEKEY, na.last = TRUE)
  
  yas_hes_linkage[!is.na(incident_to_arrival_time_absolute), order := 1:.N, by = epr_id]


### Select the smallest absolute time that was within 24 hours, but also remove any cases that were within the first and last day of data

  smallest_time_diff_per_amb_inc_24_hour <- yas_hes_linkage[order == 1 &
                                                              incident_datetime >= floor_date(min(incident_datetime), unit = "day") + days(1) &
                                                              incident_datetime < floor_date(max(incident_datetime), unit = "day")][, order := NULL]
  
  
## Need to order the other way so as to keep AEKEY-epr_id 1-to-1. epr_id included in sort to keep same each time

  setorder(smallest_time_diff_per_amb_inc_24_hour, incident_to_arrival_time_absolute, epr_id, na.last = TRUE)
  
  smallest_time_diff_per_amb_inc_24_hour[!is.na(incident_to_arrival_time_absolute), order := 1:.N, by = AEKEY]
  
  smallest_time_diff_per_amb_inc_24_hour <- smallest_time_diff_per_amb_inc_24_hour[order == 1]

  
## Check that it's all 1-to-1  
  
  stopifnot(smallest_time_diff_per_amb_inc_24_hour[, .N, by = .(epr_id, AEKEY)][, .N, by = epr_id][N > 1, .N] == 0)

  
## Take records that are between 0 and 6 hours time duration, and only earliest in that time period
## Label last 5 (less than 6) hours of YAS data for this - field that indicates records are in this time-frame

## Add prefixes to field names in each table, and drop/rename common fields (HES_ID) after
  
  setnames(epr_single_value_fields_table, setdiff(colnames(epr_single_value_fields_table), "epr_id"),
           paste("epr", setdiff(colnames(epr_single_value_fields_table), "epr_id"), sep = "_"))
  
  setnames(ae_data, setdiff(colnames(ae_data), "AEKEY"),
           paste("ae", setdiff(colnames(ae_data), "AEKEY"), sep = "_"))
  
  

  yas_ae_data <- merge(merge(epr_single_value_fields_table,
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

  
## Check that each epr_id is still only present once as only taken the first AE record per epr
  
  stopifnot(yas_ae_data[, .N, by = epr_id][N > 1, .N] == 0)
  
## Check that AEKEY only appears once, as should not be linked to two separate eprs, most recent (absolute) has been linked to
  
  stopifnot(yas_ae_data[!is.na(AEKEY), .N, by = AEKEY][N > 1, .N] == 0)
  
## Check that all AEKEYS found within 0-6 hours are present
  
  stopifnot(setdiff(smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time > 0 & incident_to_arrival_time < 6.0, AEKEY], yas_ae_data[, AEKEY]) == 0)

## Check that all epr_ids were kept
  
  stopifnot(yas_ae_data[, .N] == epr_single_value_fields_table[, .N])

  
## **** Extra
## Look at if was any AE attendance within 24 hours of ambulance time for those not said were conveyed
  
  all_yas_ae_linked <- merge(epr_single_value_fields_table[, .(epr_ENCRYPTED_HESID, epr_incident_datetime, epr_id)],
                             ae_data[, .(ae_ENCRYPTED_HESID, ae_ARRIVALTIME, AEKEY)],
                             by.x = "epr_ENCRYPTED_HESID",
                             by.y = "ae_ENCRYPTED_HESID",
                             all.x = TRUE) 
  
  all_yas_ae_linked[, incident_time_to_arrival_time := as.numeric(difftime(ae_ARRIVALTIME, epr_incident_datetime, unit = "hours"))]
  
  attendance_24_hours <- all_yas_ae_linked[incident_time_to_arrival_time > 0 & incident_time_to_arrival_time <= 24.0]

## eprs that have a AE within 24 hours that are not in linked dataset    
  setdiff(attendance_24_hours[, epr_id], yas_ae_data[is.na(AEKEY), epr_id])

## AE records that occur within 24 hours of an ambulancec incident that are not in the linked dataset    
  test <- setdiff(attendance_24_hours[, AEKEY], yas_ae_data[is.na(AEKEY), AEKEY])

  attendance_24_hours[AEKEY %chin% test]
    

# Save linked YAS-AE data -------------------------------------------------

  save_time <- getDateTimeForFilename()
  
  saveRDS(yas_ae_data, file = paste0("data/datasets/yas_ae_linked_", save_time, ".rds"))
  

