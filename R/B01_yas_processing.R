library(data.table)
library(lubridate)
library(readxl)
source("R/cleaning_fns_etl.r")


## Read in YAS tables

  epr_data_object_names <- load("data/datasets/epr_tables_2021-09-13-145424.rda")
  

# Remove eprs so that we have only a single epr per (incident_datetime-HESid) pair --------
  

## Read in patient_id-HES id look up, and merge into epr_single table

  study_id_encrypted_hesid_lookup <- readRDS("data/linkage/study_id_hesid_lookup_2021-08-02-152440.rds")
  
  epr_single_value_fields_table <- merge(epr_single_value_fields_table,
                                         study_id_encrypted_hesid_lookup,
                                         by.x = "patient_id",
                                         by.y = "STUDY_ID",
                                         all.x = TRUE)
  

## Find HES ids that appear with the same incident datetime more than once

  multi_incident_time_hes_ids <- epr_single_value_fields_table[!is.na(ENCRYPTED_HESID),
                                                                      .N, by = .(incident_datetime, ENCRYPTED_HESID)][N > 1, ENCRYPTED_HESID]

  
## Get the eprs not related to these, and therefore ones to retain

  included_epr_ids <- epr_single_value_fields_table[!ENCRYPTED_HESID %chin% multi_incident_time_hes_ids, epr_id]


## Remove records from all tables, but save them separately

  removed_epr_data_object_names <- sapply(epr_data_object_names, function(dt_name, eprs_to_retain) {
    excluded_table_name <- paste(dt_name, "excluded", sep = "_")
    dt <- get(dt_name)
    assign(dt_name, dt[epr_id %chin% eprs_to_retain], envir = parent.frame(n = 3))
    assign(excluded_table_name, dt[!(epr_id %chin% eprs_to_retain)], envir = parent.frame(n = 3))
    return(excluded_table_name)
  }, eprs_to_retain = included_epr_ids)

  
## Check that all epr_ids wanted have been retain and not more
  
  stopifnot(setdiff(epr_single_value_fields_table[, epr_id], included_epr_ids) == 0)
  stopifnot(setdiff(included_epr_ids, epr_single_value_fields_table[, epr_id]) == 0)

  
#  Reduce point-of-care fields so they are 1-to-1 with epr_id -------------

## *** Could pick receiving hospital better! but not the most important right now *****

## Splitting the look up into two groups 1) non_transport_reason, receiving_hospital, receiving_hospital_department = transported_to_hospital
## 2) final_impression_code, hand_over_atmist, hand_over_sbar - non-null


## Get all epr_ids that have appear twice across point of care table

  multi_epr_point_of_care_ids <- epr_point_of_care_table[, .N, by = epr_id][N > 1, epr_id]


## Show that poc_colour and poc_type are empty so do not need to be included at moment

  stopifnot(getNamesOfEmptyFields(epr_point_of_care_table[epr_id %chin% multi_epr_point_of_care_ids]) == c("poc_colour", "poc_type"))


## Create vectors of first grouping of fields, with epr_id

  poc_hospital_fields <- c("non_transport_reason", "receiving_hospital", "receiving_hospital_department", "transported_to_hospital", "epr_id")


## Get single row of each epr where there is 2+ rows per epr_id, where transported to hospital is not null

  multi_epr_poc_non_null_table <- unique(epr_point_of_care_table[epr_id %chin% multi_epr_point_of_care_ids, ..poc_hospital_fields][!is.na(transported_to_hospital)])


## Show that each epr that had multiple rows for point of care, all have a non-null transported to hospital

  stopifnot(setdiff(multi_epr_point_of_care_ids, multi_epr_poc_non_null_table[, epr_id]) == 0)

## and only one unique non-null row

  stopifnot(multi_epr_poc_non_null_table[, .N, by = epr_id][N > 1, .N] == 0)


## Replace fields with non-null transported to hospital record based on epr_id

  epr_point_of_care_table[multi_epr_poc_non_null_table, ':=' (non_transport_reason = i.non_transport_reason,
                                                              receiving_hospital = i.receiving_hospital,
                                                              receiving_hospital_department = i.receiving_hospital_department,
                                                              transported_to_hospital = i.transported_to_hospital),
                          on = "epr_id"]


## Check that these fields now have a 1-to-1 relationship with epr_id

  stopifnot(unique(epr_point_of_care_table[, ..poc_hospital_fields])[, .N, by = epr_id][N > 1, .N] == 0)


## Create vectors of second grouping of fields, with epr_id

  poc_handover_fields <- c("final_impression_code", "hand_over_atmist", "hand_over_sbar", "epr_id")
  
  multi_handover_table <- epr_point_of_care_table[epr_id %chin% multi_epr_point_of_care_ids, ..poc_handover_fields]


## Checks that there is only one non-null value for epr_id

  stopifnot(multi_handover_table[!is.na(final_impression_code), .N, by = .(epr_id, final_impression_code)][, .N, by = epr_id][N > 1, .N] == 0)
  stopifnot(multi_handover_table[!is.na(hand_over_atmist), .N, by = .(epr_id, hand_over_atmist)][, .N, by = epr_id][N > 1, .N] == 0)
  stopifnot(multi_handover_table[!is.na(hand_over_sbar), .N, by = .(epr_id, hand_over_sbar)][, .N, by = epr_id][N > 1, .N] == 0)

  
## For each epr_id take the non-null value for each of those fields
  
  epr_point_of_care_table[epr_id %chin% multi_epr_point_of_care_ids, ':=' (final_impression_code = final_impression_code[!is.na(final_impression_code)],
                                                                           hand_over_atmist = hand_over_atmist[!is.na(hand_over_atmist)],
                                                                           hand_over_sbar = hand_over_sbar[!is.na(hand_over_sbar)]), by = epr_id]
  

## Check if have 1-to-1 relationship of these cols for each epr_id now, and for all point of care cols
# Unique as there are still two lines for each epr that were the problem, but these have been made the same through processing above

  stopifnot(unique(epr_point_of_care_table[, ..poc_handover_fields])[, .N, by = epr_id][N > 1, .N] == 0)
  stopifnot(unique(epr_point_of_care_table)[, .N, by = epr_id][N > 1, .N] == 0)

  
## Unique whole table to get final 1-to-1 table

  poc_count <- epr_point_of_care_table[, .N]
  
  epr_point_of_care_table <- unique(epr_point_of_care_table)
  stopifnot(epr_point_of_care_table[, .N, by = epr_id][N > 1, .N] == 0)
  
  stopifnot(poc_count - length(unique(multi_epr_point_of_care_ids)) == epr_point_of_care_table[, .N])

  
# epr standardisation -----------------------------------------------------

  
  epr_single_value_fields_table[age < 0L | age > 120, age := NA]
  
  epr_airways_intervention_table[airway_intervention_size < 0L | airway_intervention_size > 10L, airway_intervention_size := NA]
  
  epr_phys_observations_table[blood_sugar_reading < 0L | blood_sugar_reading > 40L, blood_sugar_reading := NA]
  
  epr_phys_observations_table[bp_diastolic < 0L | bp_diastolic > 200L, bp_diastolic := NA]
  
  epr_phys_observations_table[bp_diastolic > bp_systolic, bp_diastolic := NA]
  
  epr_phys_observations_table[bp_systolic < 0L | bp_systolic > 300L, bp_systolic := NA]
  
  epr_phys_observations_table[co_reading_end < 0L | co_reading_end > 10L, co_reading_end := NA]
  
  epr_phys_observations_table[manual_pulse_rate < 5L | manual_pulse_rate > 220L, manual_pulse_rate := NA]
  
  epr_phys_observations_table[oxygen_saturations < 11L | oxygen_saturations > 100L, oxygen_saturations := NA]
  
  epr_phys_observations_table[peak_flow_measurement < 0L | peak_flow_measurement > 800L, peak_flow_measurement := NA]
  
  epr_phys_observations_table[pupil_reaction_left_size < 0L | pupil_reaction_left_size > 9L, pupil_reaction_left_size := NA]
  
  epr_phys_observations_table[pupil_reaction_right_size < 0L | pupil_reaction_right_size > 9L, pupil_reaction_right_size := NA]
  
  epr_phys_observations_table[temperature < 25L | temperature > 50L, temperature := NA]
  


# Re-coding ---------------------------------------------------------------

  ## Read in re-coding files
  
  receiving_hospital_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                                      sheet = "receiving_hospital",
                                                      col_names = TRUE,
                                                      col_types = "text",
                                                      trim_ws = TRUE))
  
  ethnicity_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                                      sheet = "ethnicity_mapping",
                                                      col_names = TRUE,
                                                      col_types = "text",
                                                      trim_ws = TRUE))
  
  receiving_hospital_department_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                                                 sheet = "receiving_hospital_department",
                                                                 col_names = TRUE,
                                                                 col_types = "text",
                                                                 trim_ws = TRUE))
  
  final_impression_code_mapping <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_and_meta_data.xlsx",
                                                         sheet = "final_impression_code",
                                                         col_names = TRUE,
                                                         col_types = c("numeric","text"),
                                                         trim_ws = TRUE))
  
  drug_mapping <- data.table(read_excel("D:/reference_data/catagorical_variables_values_download_2021-07-16.xlsx",
                                        sheet = "epr_drug_fields_table",
                                        col_names = TRUE,
                                        col_types = "text",
                                        trim_ws = TRUE))
  

## Check final impression code has a mapping for 1-99
  
  stopifnot(setdiff(1:99, final_impression_code_mapping$final_impression_code) == 0)

  
## Re-code receiving hospital field to remove duplicate names
  
  epr_point_of_care_table[, receiving_hospital := receiving_hospital_mapping$receiving_hospital_mapped[match(receiving_hospital, receiving_hospital_mapping$receiving_hospital)]]
  epr_point_of_care_table[!(receiving_hospital %chin% receiving_hospital_mapping$receiving_hospital_mapped), receiving_hospital := NA]
  
  
## Create new field for second coding of receiving hospital (can/can't use), and add in postcode of hospital

  epr_point_of_care_table[, receiving_hospital_sinepost := receiving_hospital_mapping$receiving_hospital_sinepost[match(receiving_hospital, receiving_hospital_mapping$receiving_hospital)]]
  epr_point_of_care_table[, receiving_hospital_postcode := receiving_hospital_mapping$postcode[match(receiving_hospital, receiving_hospital_mapping$receiving_hospital)]]
    

## Re-code receiving hospital department field
  
  epr_point_of_care_table[, receiving_hospital_department := 
                            receiving_hospital_department_mapping$receiving_hospital_department_sinepost[match(receiving_hospital_department, receiving_hospital_department_mapping$receiving_hospital_department)]]
  epr_point_of_care_table[!(receiving_hospital_department %chin% receiving_hospital_department_mapping$receiving_hospital_department_sinepost), receiving_hospital_department := NA]

    
## Re-code ethnicity field
  
  epr_single_value_fields_table[, yas_ethnicity := ethnicity]
  epr_single_value_fields_table[, ethnicity_group := ethnicity_mapping$ethnicity_group[match(ethnicity, ethnicity_mapping$epr_ethnicity)]]
  epr_single_value_fields_table[, ethnicity_desc := ethnicity_mapping$ethnicity_desc[match(ethnicity, ethnicity_mapping$epr_ethnicity)]]
  epr_single_value_fields_table[, ethnicity := NULL]  
  
  
## Re-code final impression code field
  
  epr_point_of_care_table[, final_impression_code := 
                            final_impression_code_mapping$final_impression_code_mapped[match(final_impression_code, final_impression_code_mapping$final_impression_code)]]
  
  epr_point_of_care_table[!(final_impression_code %in% final_impression_code_mapping$final_impression_code_mapped), final_impression_code := NA]
  

## Re-code drugs field
  
  epr_drug_fields_table[, drug_mapped := drug_mapping$end_value[match(drug_name, drug_mapping$field_value)]]
  
  epr_drug_fields_table[!(drug_mapped %in% drug_mapping$end_value), drug_mapped := NA]
  epr_drug_fields_table[, drug_name := NULL]
  
  
# Add in deprivation index outcome --------------------------------------------------------

## Read in postcode-IMD lookup
  
  load("D:/reference_data/pc_to_oa11_classes.rda")
  
## Tidy YAS postcode, all uppercase and standard format (might as well do GP one as well)
  
  epr_single_value_fields_table[,  ':=' (postcode = fn_cleanPostcode(postcode),
                                         gp_postcode = fn_cleanPostcode(gp_postcode ))]
  epr_single_value_fields_table[,  ':=' (postcode = fn_removeBlanks(postcode),
                                         gp_postcode = fn_removeBlanks(gp_postcode))]
  
## There are 10 postcodes with more than 8 characters, set this to NA
  
  epr_single_value_fields_table[nchar(postcode) > 8, postcode := NA]
  
## Ensure YAS postcodes always have one (and only one) space between outward and inward portions, and of variable length (like downloaded ref data)
  
  stopifnot(epr_single_value_fields_table[, max(nchar(postcode), na.rm = TRUE)] == 8 & epr_single_value_fields_table[, min(nchar(postcode), na.rm = TRUE)] == 6 &
              epr_single_value_fields_table[substr(postcode, nchar(postcode) - 3, nchar(postcode) - 3) == " ", .N] == epr_single_value_fields_table[!is.na(postcode), .N])
  
  
## Merge in IMD to epr data
  
  epr_single_value_fields_table <- merge(epr_single_value_fields_table,
                                         pc_to_oa11_classes,
                                         by.x = "postcode",
                                         by.y = "pcds",
                                         all.x = TRUE)
  
  epr_single_value_fields_table[, postcode_invalid := !is.na(postcode) & is.na(usertype)]
  epr_single_value_fields_table[postcode_invalid == FALSE & is.na(usertype), postcode_invalid := NA]
  
  
# Re-attendances in YAS data ----------------------------------------------
  
  ## Set order so HED id and earliest incident datetime is first
  
  setorder(epr_single_value_fields_table, ENCRYPTED_HESID, incident_datetime, na.last = TRUE)
  
  
  ## Find time between incidents for each person for all incidents
  
  epr_single_value_fields_table[!is.na(ENCRYPTED_HESID),
                                duration_since_previous_incident := difftime(incident_datetime, shift(incident_datetime),  tz = "Europe/London", unit = "hours"),
                                by = ENCRYPTED_HESID]
  
  ## Create field to label those incidents that happened within 24 hours of patients previous incident
  
  epr_single_value_fields_table[, previous_ems_attendence_within_24_hours := (duration_since_previous_incident < 24.0)]
  epr_single_value_fields_table[, duration_since_previous_incident := NULL]
  
  
  ## Set to NA all those incidents that occurred in the first day of the data as looking at whole 24 hour period
  
  epr_single_value_fields_table[incident_datetime < floor_date(min(epr_single_value_fields_table$incident_datetime), unit = "day") + days(1) & 
                                  previous_ems_attendence_within_24_hours == FALSE, previous_ems_attendence_within_24_hours := NA]  
  
  
## Save epr table, and excluded epr tables with new name
  
  save_time <- getDateTimeForFilename()
  
  save(list = epr_data_object_names, file = paste0("data/datasets/cohort_processed_epr_table_", save_time, ".rda"))
  save(list = removed_epr_data_object_names, file = paste0("data/datasets/cohort_excluded_processed_epr_table_", save_time, ".rda"))
  
  