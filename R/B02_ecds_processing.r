## Script for preparing ECDS data to be linked to AE/YAS data, and to standardise cols that require it

library(data.table)
library(readxl)
source("R/cleaning_fns_etl.r")
source("R/standardise_functions.r")


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


# Retain unique records only

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
  
  
  
## Read in full SNOMED table
  
  snomed_codes <- fread("D:/reference_data/full_trud_snomed_concepts_2021-08-20-124429.csv",
                        colClasses = "character")
  
  
## Make list of all fields that are SNOMED codes
  
  snomed_fields <- c("ACUITY", "ARRIVAL_MODE", "ACCOMMODATION_STATUS", "ACCESSIBLE_INFORMATION_PROFESSIONAL_REQUIRED", "ATTENDANCE_SOURCE",
                     "CHIEF_COMPLAINT", "PLACE_OF_INJURY", "INJURY_INTENT", "INJURY_ACTIVITY_STATUS", "INJURY_ACTIVITY_TYPE", "INJURY_MECHANISM",
                     paste0("COMORBIDITIES_", 1:10), paste0("DIAGNOSIS_CODE_", 1:12), paste0("DIAGNOSIS_QUALIFIER_", 1:12), paste0("INVESTIGATION_CODE_", 1:12),
                     paste0("TREATMENT_CODE_", 1:12), paste0("REFERRED_TO_SERVICE_", 1:4), "DISCHARGE_STATUS", "DISCHARGE_DESTINATION", "DISCHARGE_FOLLOW_UP", 
                     "DISCHARGE_INFORMATION_GIVEN")
  
  snomed_fields_label <- paste0(snomed_fields, "_label")
  
  ecds_data[, (snomed_fields_label) := lapply(.SD, getSnomedLabel, ecds_data, snomed_codes), .SDcols = snomed_fields]
 
   
## Create field for readable SITE label - read in trust data first
  
  trust_data <- readRDS("D:/reference_data/site_data.rds")
  
  ecds_data[, site_label := trust_data$name[match(SITE, trust_data$org_code)]]
  ecds_data[!(SITE %chin% trust_data$org_code), site_label := NA]
  
  
## Create field for readable attendance category (taken from NHS Data Model and Dictionary website)
  
  attendance_cat_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                                  sheet = "ecds_attendance_cat",
                                                  col_names = TRUE,
                                                  col_types = "text",
                                                  trim_ws = TRUE))
  
  ecds_data <- merge(ecds_data,
                     attendance_cat_mapping,
                     by.x = "ATTENDANCE_CATEGORY",
                     by.y = "attendance_cat",
                     all.x = TRUE)
  
  
## Create field for readable department type (taken from NHS Data Model and Dictionary website)
  
  dep_type_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                            sheet = "ecds_dep_type",
                                            col_names = TRUE,
                                            col_types = "text",
                                            trim_ws = TRUE))
  
  ecds_data <- merge(ecds_data,
                     dep_type_mapping,
                     by.x = "DEPARTMENT_TYPE",
                     by.y = "department_type",
                     all.x = TRUE)
  
  
## Remove all fields that have been made readable, acuity left as needed later
  
  ecds_data[, c("ATTENDANCE_CATEGORY", "DEPARTMENT_TYPE") := NULL]
  
  
## Read in study_id-HES_is look up table created in A01
  
  study_id_encrypted_hesid_lookup <- readRDS("data/linkage/study_id_hesid_lookup_2021-08-02-152440.rds")  
  
  
## Merge in HES_ID to ECDS data
  
  ecds_data_hes_id <- merge(ecds_data,
                            study_id_encrypted_hesid_lookup,
                            by = "STUDY_ID",
                            all.x = TRUE)
  
  
## How many ECDS records do not have a HES_ID linked to them, and how many study ids does this apply to (report %'s)
  
  ecds_data_hes_id[is.na(ENCRYPTED_HESID), .N]
  ecds_data_hes_id[is.na(ENCRYPTED_HESID), .N, by = STUDY_ID][, .N]
  
  # check <- ecds_data[, .N, by = .(STUDY_ID, RECORD_IDENTIFIER)][, .N, by = RECORD_IDENTIFIER][N > 1, RECORD_IDENTIFIER]
  # ecds_data[RECORD_IDENTIFIER %chin% check, .N, by = STUDY_ID][, .N]
  
  
  
# Save ECDS data ----------------------------------------------------------
  
  save_time <- gsub(":", "", Sys.time(), fixed = TRUE)

  saveRDS(ecds_data_hes_id, file = paste0("data/datasets/ecds_processed_", save_time, ".rds"))
  
