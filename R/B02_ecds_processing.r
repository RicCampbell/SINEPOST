## Script for preparing ECDS data to be linked to YAS data, and to standardise cols that require it

library(data.table)
source("R/cleaning_fns_etl.r")


# Read in HES data --------------------------------------------------------

  ecds <- fread("D:/data/nhs_digital/ecds/FILE0119464_NIC284866_HES_ECDS_201999.txt",
                colClasses = "character")
  
  setnames(ecds, make.names(toupper(colnames(ecds)), unique = TRUE))
  ecds_col_names <- colnames(ecds)
  ecds[, (ecds_col_names) := lapply(.SD, fn_removeBlanks), .SDcols = ecds_col_names]

  
# Remove all empty cols (98 in total)

  empty_cols <- c("CHIEF_COMPLAINT_IS_INJURY_RELATED", "CONCLUSION_TIME_SINCE_ARRIVAL", "DECISION_TO_ADMIT_TIME_SINCE_ARRIVAL", "DEPARTURE_TIME_SINCE_ARRIVAL", 
                paste0("DIAGNOSIS_FIRST_", 1:12), paste0("DIAGNOSIS_IS_AEC_RELATED_", 1:12), paste0("DIAGNOSIS_IS_ALLERGY_RELATED_", 1:12),
                paste0("DIAGNOSIS_IS_APPLICABLE_TO_FEMALES_", 1:12), paste0("DIAGNOSIS_IS_APPLICABLE_TO_MALES_", 1:12),
                paste0("DIAGNOSIS_IS_INJURY_RELATED_", 1:12), paste0("DIAGNOSIS_IS_NOTIFIABLE_DISEASE_", 1:12), "FRACTIONAL_AGE_AT_ARRIVAL",
                "INITIAL_ASSESSMENT_TIME_SINCE_ARRIVAL", "LSOA_2011", "LSOA_PROVIDER_DISTANCE", "LSOA_PROVIDER_DISTANCE_ORIGIN", "LSOA_SITE_OF_TREATMENT_DISTANCE",
                "LSOA_SITE_OF_TREATMENT_DISTANCE_ORIGIN", "MSOA_11", "RURAL_URBAN_INDICATOR", "SEEN_FOR_TREATMENT_TIME_SINCE_ARRIVAL")

  stopifnot(all(sapply(ecds[, ..empty_cols], function(field) all(is.na(field)))))
  ecds[, (empty_cols) := NULL]
  
# ecds[, .N] # 254181
# sapply(ecds, function(col) sum(is.na(col)))

  
## Probably don't want a SSoT as coming from same-ish source, and don't have enough demographic information to creat and be certain of
## All records have STUDY_ID

## Could remove more fields that appear in both datasets or aren't required  

# Retain unique records only, and create record ID

  ecds_data <- unique(ecds)

  
# Convert date fields to dates

  ecds_date_cols <- c("ARRIVAL_DATE", "ASSESSMENT_DATE", "CONCLUSION_DATE", "DECIDED_TO_ADMIT_DATE", "DEPARTURE_DATE", "INJURY_DATE", "SEEN_DATE")
  ecds_data[, (ecds_date_cols) := lapply(.SD, fn_covertStrToDate), .SDcols = ecds_date_cols]

  
# Covert time to datetimes
## INVESTIGATION 1:12, and TREATMENT 1:12 also have DATE and TIME fields, do first one
## If more are required, these will probably need to be melted, so change time/date then!

  ecds_data[, ':=' (ARRIVAL_TIME = lubridate::fast_strptime(paste(ARRIVAL_DATE, substr(ARRIVAL_TIME, 1, 8)),
                                                          format = "%Y-%m-%d %H:%M:%S",
                                                          tz = "Europe/London",
                                                          lt = FALSE),
                  ASSESSMENT_TIME = lubridate::fast_strptime(paste(ASSESSMENT_DATE, substr(ASSESSMENT_TIME, 1, 8)),
                                                          format = "%Y-%m-%d %H:%M:%S",
                                                          tz = "Europe/London",
                                                          lt = FALSE),
                  CONCLUSION_TIME = lubridate::fast_strptime(paste(CONCLUSION_DATE, substr(CONCLUSION_TIME, 1, 8)),
                                                          format = "%Y-%m-%d %H:%M:%S",
                                                          tz = "Europe/London",
                                                          lt = FALSE),
                  DECIDED_TO_ADMIT_TIME = lubridate::fast_strptime(paste(DECIDED_TO_ADMIT_DATE, substr(DECIDED_TO_ADMIT_TIME, 1, 8)),
                                                          format = "%Y-%m-%d %H:%M:%S",
                                                          tz = "Europe/London",
                                                          lt = FALSE),
                  DEPARTURE_TIME = lubridate::fast_strptime(paste(DEPARTURE_DATE, substr(DEPARTURE_TIME, 1, 8)),
                                                          format = "%Y-%m-%d %H:%M:%S",
                                                          tz = "Europe/London",
                                                          lt = FALSE),
                  INJURY_TIME = lubridate::fast_strptime(paste(INJURY_DATE, substr(INJURY_TIME, 1, 8)),
                                                                   format = "%Y-%m-%d %H:%M:%S",
                                                                   tz = "Europe/London",
                                                                   lt = FALSE),
                  INVESTIGATION_TIME_1 = lubridate::fast_strptime(paste(INVESTIGATION_DATE_1, substr(INVESTIGATION_TIME_1, 1, 8)),
                                                         format = "%Y-%m-%d %H:%M:%S",
                                                         tz = "Europe/London",
                                                         lt = FALSE),
                  TREATMENT_TIME_1 = lubridate::fast_strptime(paste(TREATMENT_DATE_1, substr(TREATMENT_TIME_1, 1, 8)),
                                                                  format = "%Y-%m-%d %H:%M:%S",
                                                                  tz = "Europe/London",
                                                                  lt = FALSE),
                  SEEN_TIME = lubridate::fast_strptime(paste(SEEN_DATE, substr(SEEN_TIME, 1, 8)),
                                                            format = "%Y-%m-%d %H:%M:%S",
                                                            tz = "Europe/London",
                                                            lt = FALSE))]
  

## Convert fields to integers
  
  ecds_data[, ':=' (AGE_AT_ARRIVAL = as.integer(AGE_AT_ARRIVAL),
                    AGE_AT_CDS_ACTIVITY_DATE = as.integer(AGE_AT_CDS_ACTIVITY_DATE ),
                    AGE_RANGE = as.integer(AGE_RANGE))]
  

  
  ## Merge in HES_ID to ECDS data
  
  ecds_data_hes_id <- merge(ecds_data,
                            encrypted_hes_to_study_id_lookup,
                            by = "STUDY_ID",
                            all.x = TRUE)
  
  
  ## How many ECDS records do not have a HES_ID linked to them, and how many study ids does this apply to (report %'s)
  
  ecds_data_hes_id[is.na(ENCRYPTED_HESID), .N]
  ecds_data_hes_id[is.na(ENCRYPTED_HESID), .N, by = STUDY_ID][, .N]
  
  # check <- ecds_data[, .N, by = .(STUDY_ID, RECORD_IDENTIFIER)][, .N, by = RECORD_IDENTIFIER][N > 1, RECORD_IDENTIFIER]
  # ecds_data[RECORD_IDENTIFIER %chin% check, .N, by = STUDY_ID][, .N]
  
  
  
# Save ECDS data ----------------------------------------------------------
  
  save_time <- gsub(":", "", Sys.time(), fixed = TRUE)

  saveRDS(ecds_data, file = paste0("data/datasets/ecds_processed_", save_time, ".rds"))
  
