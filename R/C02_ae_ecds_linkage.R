## Script for linking in ECDS data to the AE(/YAS) data

library(data.table)
source("R/cleaning_fns_etl.r")


## Read in the processed ECDS and AE data (could change to AE-yas data)

  ecds_data <- readRDS("data/datasets/ecds_processed_2021-08-05 144452.rds")
  yas_ae_data <- readRDS("data/datasets/yas_ae_linked_2021-08-03-095334.rds")
  
  
## Take original counts for checks later
  
  ecds_row_count <- ecds_data[, .N]
  ecds_hes_id_count <- uniqueN(ecds_data[!is.na(ENCRYPTED_HESID), ENCRYPTED_HESID])
  
  
## Check that are is only one HES-id per record
  
  stopifnot(ecds_data[, .N, by = .(ENCRYPTED_HESID, RECORD_IDENTIFIER)][, .N, by = RECORD_IDENTIFIER][N > 1, .N] == 0)
  
  
## Multiple lines per record-id
  
  multiple_record_id_count <- ecds_data[, .N, by = RECORD_IDENTIFIER][N > 1, .N]
  
  
## Remove study_id (has been 'replaced' with HES-id), and unique, reduces to one record per record-id
  
  ecds_data[, STUDY_ID := NULL]
  
  ecds_data <- unique(ecds_data)
  
  stopifnot(ecds_data[, .N, by = RECORD_IDENTIFIER][N > 1, .N] == 0)
  stopifnot(ecds_data[, .N] + multiple_record_id_count == ecds_row_count)
  

## Remove all records without HES id as they won't be linked to anything anyway
  
  ecds_data <- ecds_data[!is.na(ENCRYPTED_HESID)]
  
  
## Create field of number of blank fields
  
  ecds_data[, null_fields := rowSums(is.na(ecds_data))]
  
  
## Create field that indicates that ACUITY is null or not
  
  ecds_data[, null_acuity := is.na(ACUITY)]
  
  
## Set order so that we get records with acuity first, and then least number of blank records
  
  setorder(ecds_data, ENCRYPTED_HESID, ARRIVAL_TIME, null_acuity, null_fields, DIAGNOSIS_CODE_1, INVESTIGATION_CODE_1, TREATMENT_CODE_1,
           ASSESSMENT_TIME, SEEN_TIME, CONCLUSION_TIME, DEPARTURE_TIME, RECORD_IDENTIFIER, na.last = TRUE)
  
  
## Create order field
  
  ecds_data[, order := 1:.N, by = .(ENCRYPTED_HESID, ARRIVAL_TIME)]
  

## Keep only first record from ordering to get to one-to-one hes-id - arrival time pairs
  
  ecds_data_single_id_arrival <- ecds_data[order == 1]


## Check have only one record per hes-id - arrival datetime now, and the same amount of unique hes-ids as at start (have removed NAs)  
  
  stopifnot(ecds_data_single_id_arrival[, .N, by = .(ENCRYPTED_HESID, ARRIVAL_TIME)][N > 1, .N] == 0)
  
  stopifnot(uniqueN(ecds_data_single_id_arrival$ENCRYPTED_HESID) == ecds_hes_id_count)

  
## Remove all created fields
  
  ecds_data_single_id_arrival[, c("null_fields", "null_acuity", "order") := NULL]
  
  
## Add prefix to col names so can tell come from ECDS dataset
  
  setnames(ecds_data_single_id_arrival, setdiff(colnames(ecds_data_single_id_arrival), "ENCRYPTED_HESID"),
           paste("ecds", setdiff(colnames(ecds_data_single_id_arrival), "ENCRYPTED_HESID"), sep = "_"))
  
  
## Merge ECDS data into yas-ae data based on sharing a person-arrival time pair
  
  yas_ae_ecds_data <- merge(yas_ae_data,
                            ecds_data_single_id_arrival,
                            by.x = c("ENCRYPTED_HESID", "ae_ARRIVALTIME"),
                            by.y = c("ENCRYPTED_HESID", "ecds_ARRIVAL_TIME"),
                            all.x = TRUE)
  
  setnames(yas_ae_ecds_data, "ae_ARRIVALTIME", "ARRIVAL_TIME")
  
  stopifnot(yas_ae_ecds_data[, .N] == yas_ae_data[, .N])
  stopifnot(yas_ae_ecds_data[!is.na(ecds_RECORD_IDENTIFIER), .N, by = ecds_RECORD_IDENTIFIER][N > 1, .N] == 0)

  
## Save fully linked data
  
  save_time <- getDateTimeForFilename()
  
  saveRDS(yas_ae_ecds_data, file = paste0("data/datasets/yas_ae_ecds_linked_", save_time, ".rds"))
    
  