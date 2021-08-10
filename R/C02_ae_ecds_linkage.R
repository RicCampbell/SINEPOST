## Script for linking in ECDS data to the AE(/YAS) data

library(data.table)

source("R/cleaning_fns_etl.r")


## Read in the processed ECDS and AE data (could change to AE-yas data)

  ecds_data <- readRDS("data/datasets/ecds_processed_2021-08-05 144452.rds")
  yas_ae_data <- readRDS("data/datasets/yas_ae_linked_2021-08-03-095334.rds")
  
  
## Take original counts for checks later
  
  ecds_row_count <- ecds_data[, .N]
  ecds_hes_id_count <- uniqueN(ecds_data[!is.na(ENCRYPTED_HESID), ENCRYPTED_HESID])
  
  
## Check that are is only one HED-id per record
  
  stopifnot(ecds_data[, .N, by = .(ENCRYPTED_HESID, RECORD_IDENTIFIER)][, .N, by = RECORD_IDENTIFIER][N > 1, .N] == 0)
  
  
## Multiple lines per record-id
  
  multiple_record_id_count <- ecds_data[, .N, by = RECORD_IDENTIFIER][N > 1, .N]
  
  
## Remove study_id (has been 'replaced' with HES-id), and unique, reduces to one record per record-id
  
  ecds_data[, STUDY_ID := NULL]
  
  ecds_data <- unique(ecds_data)
  
  stopifnot(ecds_data[, .N, by = RECORD_IDENTIFIER][N > 1, .N] == 0)
  stopifnot(ecds_data[, .N] + multiple_record_id_count == ecds_row_count)
  
  
## Check to see how many multiple arrival_time-HES_id pairs there are (88 double records, 84 people)
  
  multiple_record_count <- ecds_data[!is.na(ENCRYPTED_HESID), .N, by = .(ENCRYPTED_HESID, ARRIVAL_TIME)][N > 1, sum(N)]
  #ecds_data[!is.na(ENCRYPTED_HESID), .N, by = .(ENCRYPTED_HESID, ARRIVAL_TIME)][N > 1, .N, by = ENCRYPTED_HESID][, .N]
  
  
## Get all records that are multiple time-id records
  
  ecds_data[!is.na(ENCRYPTED_HESID), total_count := sum(.N), by = .(ENCRYPTED_HESID, ARRIVAL_TIME)]
  
  multiple_records_ids <- ecds_data[total_count >= 2, RECORD_IDENTIFIER]
  
  stopifnot(length(multiple_records_ids) == multiple_record_count)

  multiple_record_table <- ecds_data[RECORD_IDENTIFIER %chin% multiple_records_ids]

    
## ACUITY is most important field, so select any records where one has a value and the other does not (most of 2 records per hes_id-time)
  
  stopifnot(multiple_record_table[, max(total_count)] == 2)
  
  single_acuity_value <- multiple_record_table[!is.na(ACUITY), non_na_acuity_count := sum(.N),
                                               by = .(ENCRYPTED_HESID, ARRIVAL_TIME)][non_na_acuity_count == 1, .(RECORD_IDENTIFIER, ENCRYPTED_HESID)]
  
  
## Remove these people from multiple record table
  
  multiple_record_table <- multiple_record_table[!(ENCRYPTED_HESID %chin% single_acuity_value$ENCRYPTED_HESID)]
  

## Look at the number of blank fields
  
  multiple_record_table[, null_fields := rowSums(is.na(multiple_record_table))]
  
  
## Set order so for each person we take the most complete record, and then one with diagnosis, treatment, and then earliest times
  
  setorder(multiple_record_table, ENCRYPTED_HESID, null_fields, DIAGNOSIS_CODE_1, INVESTIGATION_CODE_1, TREATMENT_CODE_1,
           ARRIVAL_TIME, ASSESSMENT_TIME, SEEN_TIME, CONCLUSION_TIME, DEPARTURE_TIME, RECORD_IDENTIFIER, na.last = TRUE)
  
  multiple_record_table[, order := 1:.N, by = ENCRYPTED_HESID]
  
  single_id_datetime <- rbind(multiple_record_table[order == 1, .(RECORD_IDENTIFIER, ENCRYPTED_HESID)],
                              single_acuity_value)
  
  
## Do checks have right number of records, and one for each person originaly flagged as having multiple records
  
  stopifnot(uniqueN(single_id_datetime[, ENCRYPTED_HESID]) == single_id_datetime[, .N])
  
  stopifnot(setdiff(ecds_data[total_count >= 2, ENCRYPTED_HESID], single_id_datetime[, ENCRYPTED_HESID]) == 0)
  stopifnot(setdiff(single_id_datetime[, ENCRYPTED_HESID], ecds_data[total_count >= 2, ENCRYPTED_HESID]) == 0)
  
  
## Keep only one-to-one hes_id-time records and the ones that we have identified as wanted
  
  ecds_data <- ecds_data[total_count == 1 | RECORD_IDENTIFIER %chin% single_id_datetime$RECORD_IDENTIFIER]

  
## Check that we now have only one record for hes_id-arrival_time, and that we have kept a record for all hes_id
  
  stopifnot(ecds_data[!is.na(ENCRYPTED_HESID), .N, by = .(ENCRYPTED_HESID, ARRIVAL_TIME)][N > 1, .N] == 0)
  
  stopifnot(uniqueN(ecds_data[, ENCRYPTED_HESID]) == ecds_hes_id_count)
  
  
## Remove all created fields
  
  ecds_data[, total_count := NULL]
  
  
## Add prefix to col names so can tell come from ECDS dataset
  
  setnames(ecds_data, setdiff(colnames(ecds_data), "ENCRYPTED_HESID"),
           paste("ecds", setdiff(colnames(ecds_data), "ENCRYPTED_HESID"), sep = "_"))
  
  
## Merge ECDS data into yas-ae data based on sharing a person-arrival time pair
  
  yas_ae_ecds_data <- merge(yas_ae_data,
                            ecds_data,
                            by.x = c("ENCRYPTED_HESID", "ae_ARRIVALTIME"),
                            by.y = c("ENCRYPTED_HESID", "ecds_ARRIVAL_TIME"),
                            all.x = TRUE)
  
  setnames(yas_ae_ecds_data, "ae_ARRIVALTIME", "ARRIVAL_TIME")
  
  stopifnot(yas_ae_ecds_data[, .N] == yas_ae_data[, .N])
  stopifnot(yas_ae_ecds_data[!is.na(ecds_RECORD_IDENTIFIER), .N, by = ecds_RECORD_IDENTIFIER][N > 1, .N] == 0)

  
## Save fully linked data
  
  save_time <- getDateTimeForFilename()
  
  saveRDS(yas_ae_ecds_data, file = paste0("data/datasets/yas_ae_ecds_linked_", save_time, ".rds"))
    
  