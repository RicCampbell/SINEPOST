## Script for creating analysis dataset
## Adds fields from other table to 'epr_single..' table
## Creates and adds derived outcome fields

library(data.table)
source("R/cleaning_fns_etl.r")


## Load in standardised YAS tables, linked ae-ecds-yas data, and distance data

### NEED TO CHANGE TO READ IN YAS_AE_ECDS DATA - check this all the way through and that don't lose any fields at any point

  epr_data_object_names <- load("data/datasets/cohort_processed_epr_table_2021-09-14-161940.rda")
  yas_ae_ecds_data <- readRDS("data/datasets/yas_ae_ecds_linked_2021-09-14-162412.rds")
  
  incident_hospital_geo_data <- readRDS("data/datasets/incident_hospital_routes_ 2021-09-10-100552 .rds")

  
## Take number or records/epr_ids to check merges later
  
  epr_id_count <- yas_ae_ecds_data[, .N]
  
  
# Merge in distance and time from incident to hospital site ---------------

  yas_ae_ecds_data <- merge(yas_ae_ecds_data,
                            incident_hospital_geo_data,
                            by = c("epr_postcode", "epr_receiving_hospital_postcode"),
                            all.x = TRUE)  
  
  
# Add in desired fields from other YAS tables, and add prefix for each table --------


# Find max news scores for each epr_id ------------------------------------

  max_news_score_data <- epr_news_score_table[, .SD[which.max(news_score)], by = epr_id]
  
  setnames(max_news_score_data, setdiff(colnames(max_news_score_data), "epr_id"),
           paste("epr", setdiff(colnames(max_news_score_data), "epr_id"), sep = "_"))
           
  stopifnot(uniqueN(epr_news_score_table[, epr_id]) == max_news_score_data[, .N])
  

# Physical observations ---------------------------------------------------

  ## Remove capital from obvs type so field names are still all lower case later
  
  epr_phys_observations_table[, observation_type := tolower(observation_type)]
  
  
## ** Not using MentalCapacity obvs ** - checked
  
## Want to take the Primary observation, and the 'first' Subsequent observation

## Check there is only one Primary set of observations for epr_ids  
  
  stopifnot(epr_phys_observations_table[observation_type == "primary", .N, by = epr_id][N > 1, .N] == 0)
  
  
## Make table of just Primary obvs
  
  all_primary_obvs <- epr_phys_observations_table[observation_type == "primary"]
  
  stopifnot(all_primary_obvs[, .N, by = epr_id][N > 1, .N] == 0)

  
## Does any epr have more than one record for each Subsequent-datetime pair - YES, max of 3
  
  epr_phys_observations_table[, .N, by = .(epr_id, observations_recorded_time)][N > 1, .N, by = N]
  

## Are these Subsequent-datetime pairs duplicates?  (no, they are not, proven at end of section)

  yas_duplicated_eprid_obstime <- epr_phys_observations_table[observation_type == "subsequent",
                                                              N_records := .N, by = .(epr_id, observations_recorded_time)][N_records > 1, epr_id]

  duplicated_fields <- colnames(epr_phys_observations_table)[!(colnames(epr_phys_observations_table) %in% c("epr_id"))]
  
  epr_phys_observations_table[epr_id %chin% yas_duplicated_eprid_obstime,
                              duplicates := duplicated(epr_phys_observations_table[epr_id %chin% yas_duplicated_eprid_obstime], by = duplicated_fields)]
  
  epr_phys_observations_table[epr_id %chin% yas_duplicated_eprid_obstime, .(distinct_records = sum(duplicates == FALSE)),
                                        by = .(epr_id, observations_recorded_time)]
  
## As [distinct_records != 1, .N] == 0 is NOT TRUE, then they are not duplicates
  
  epr_phys_observations_table[, duplicates := NULL]


## Build table of Subsequent physical obvs that we want ----------------------------
  
## Find number of non_null fields for each record
  
  epr_subsequent_phys_obvs <- epr_phys_observations_table[observation_type == "subsequent"]
  
  epr_subsequent_phys_obvs[, null_fields := rowSums(is.na(epr_subsequent_phys_obvs))]
  
  
## Set order by id, time, and then number of non null fields
  
  setorder(epr_subsequent_phys_obvs, epr_id, observations_recorded_time, null_fields, na.last = TRUE)
  
  epr_subsequent_phys_obvs[, order := 1:.N, by = epr_id]

    
## Create id list for subsequent obvs that had multiple records for datetime 
  
  multiple_first_datetime_subsequent_obvs_ids <- epr_subsequent_phys_obvs[, multiple_datetime_obvs := 1:.N, by = .(epr_id, observations_recorded_time)][multiple_datetime_obvs == 2, epr_id]
  
  
## Take only subsequent obvs of interest
  
  epr_subsequent_phys_obvs <- epr_subsequent_phys_obvs[order == 1]
  

## Remove extra cols and bind together
    
  epr_subsequent_phys_obvs[, c("null_fields", "order","multiple_datetime_obvs", "N_records") := NULL]

  
## Bind all physical obvs that we now have together  
      
  all_obvs_want <- rbind(all_primary_obvs, 
                         epr_subsequent_phys_obvs)
  
    
## Cast to get wide version
    
  phys_obs_cols <- colnames(all_obvs_want)[!(colnames(all_obvs_want) %in% c("epr_id", "observation_type"))]
  
  physcial_obvs_wide <- dcast(all_obvs_want, epr_id ~ observation_type, value.var = phys_obs_cols)
  
    
## Create a field to flag if subsequent obvs had multiple records for datetime
    
  physcial_obvs_wide[, multi_timedate_obvs := (epr_id %chin% multiple_first_datetime_subsequent_obvs_ids)]
    
    
## Create a field to flag if a secondary obvs is prior to primary
    
  physcial_obvs_wide[, subsequent_before_primary := (observations_recorded_time_primary > observations_recorded_time_subsequent)]
    

  
# epr_pon_notification_type_table -----------------------------------------

## Cast to get one line per epr-id
    
  epr_pon_notification_type_table_wide <- dcast(epr_pon_notification_type_table, epr_id ~ pon_notification_type, value.var = "pon_notification_type")
  
  stopifnot(epr_pon_notification_type_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  
  setnames(epr_pon_notification_type_table_wide, setdiff(colnames(epr_pon_notification_type_table_wide), "epr_id"),
           paste("pon", setdiff(colnames(epr_pon_notification_type_table_wide), "epr_id"), sep = "_"))
    
    
# epr_airways_intervention_table ------------------------------------------
## Check each _epr_id only has each airway type at most once first, then cast
  
  stopifnot(epr_airways_intervention_table[, .N, by = .(epr_id, airway_type)][N > 1, .N] == 0)
  
  airway_cols <- colnames(epr_airways_intervention_table)[!(colnames(epr_airways_intervention_table) %in% c("epr_id", "airway_type"))]
  
  epr_airways_intervention_table_wide <- dcast(epr_airways_intervention_table, epr_id ~ airway_type, value.var = airway_cols)
  
  stopifnot(epr_airways_intervention_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  

# epr_cardiac_respiratory_arrest_table ------------------------------------
## Only one line at most per epr_id
  
  stopifnot(epr_cardiac_respiratory_arrest_table[, .N, by = epr_id][N > 1, .N] == 0)
  

# epr_immobilisation_table ------------------------------------------------

## Only want binary of if each type was used or not, if no time was still used
  
  stopifnot(epr_immobilisation_table[, .N, by = .(epr_id, iex_type)][N > 1, .N] == 0)
  
  epr_immobilisation_table[, iex_time := NULL]
  
  epr_immobilisation_table_wide <- dcast(epr_immobilisation_table, epr_id ~ iex_type, value.var = "iex_type")
  
  setnames(epr_immobilisation_table_wide, setdiff(colnames(epr_immobilisation_table_wide), "epr_id"),
           paste("immob", setdiff(colnames(epr_immobilisation_table_wide), "epr_id"), sep = "_"))

  
# epr_advice_given_table --------------------------------------------------

## Check each epr_id only has each ipa_type at most once first, then cast
  
  stopifnot(epr_advice_given_table[,.N, by = .(epr_id, ipa_type)][N > 1, .N] == 0)
  
  advice_cols <- colnames(epr_advice_given_table)[!(colnames(epr_advice_given_table) %in% c("epr_id", "ipa_type"))]

  epr_advice_given_table_wide <- dcast(epr_advice_given_table, epr_id ~ ipa_type, value.var = advice_cols)
  
  stopifnot(epr_advice_given_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  
  setnames(epr_advice_given_table_wide, setdiff(colnames(epr_advice_given_table_wide), "epr_id"),
           paste("advice", setdiff(colnames(epr_advice_given_table_wide), "epr_id"), sep = "_"))
  
  
# epr_mobility_table ------------------------------------------------------

## Not interesting in mobility type, so can drop this and cast (checked with Jamie)
  
  epr_mobility_table[, mobility_type := NULL]
  
## There can be multiple entries for item used, but only interested in binary if used or not
  
  epr_mobility_table <- unique(epr_mobility_table)
  
  epr_mobility_table_wide <- dcast(epr_mobility_table, epr_id ~ mobility_item_used, value.var = "mobility_item_used")
  
  stopifnot(epr_mobility_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  
  setnames(epr_mobility_table_wide, setdiff(colnames(epr_mobility_table_wide), "epr_id"),
           paste("mobililty", setdiff(colnames(epr_mobility_table_wide), "epr_id"), sep = "_"))
  
  
# epr_fast_stroke_table ---------------------------------------------------
## Only one line at most per epr_id
  
  stopifnot(epr_fast_stroke_table[, .N, by = epr_id][N > 1, .N] == 0)
  

# epr_point_of_referral_table ---------------------------------------------

## Check each epr_id only has each referral_type_crew at most once first, then cast
  
  stopifnot(epr_point_of_referral_table[, .N, by = .(epr_id, referral_type_crew)][N > 1, .N] == 0)
  
  epr_point_of_referral_table_wide <- dcast(epr_point_of_referral_table, epr_id ~ referral_type_crew, value.var = "por_referral_accepted")
  
  stopifnot(epr_point_of_referral_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  
  setnames(epr_point_of_referral_table_wide, setdiff(colnames(epr_point_of_referral_table_wide), "epr_id"),
           paste("por", setdiff(colnames(epr_point_of_referral_table_wide), "epr_id"), sep = "_"))
  
  
# epr_psyc_observations_table ---------------------------------------------
  
## Check each erp_id only has each psy_type at most once first, then cast
  
  stopifnot(epr_psyc_observations_table[, .N, by = .(epr_id, psy_type)][N > 1, .N] == 0)
  
  epr_psyc_observations_table_wide <- dcast(epr_psyc_observations_table, epr_id ~ psy_type, value.var = "psy_result")
  
  stopifnot(epr_psyc_observations_table_wide[, .N, by = epr_id][N > 1, .N] == 0)

  setnames(epr_psyc_observations_table_wide, setdiff(colnames(epr_psyc_observations_table_wide), "epr_id"),
           paste("psyc", setdiff(colnames(epr_psyc_observations_table_wide), "epr_id"), sep = "_"))
  

# epr_ecg_incl_freetext_table ---------------------------------------------

## Only interested in primary ecg records, there is one epr_id with 2x primary obvs
  
  epr_ecg_incl_freetext_table_primary <- epr_ecg_incl_freetext_table[ecg_recording_order == "Primary"]
  
## create field with number of null fields for each row
  
  epr_ecg_incl_freetext_table_primary[, null_fields := rowSums(is.na(epr_ecg_incl_freetext_table_primary))]
  
  setorder(epr_ecg_incl_freetext_table_primary, epr_id, null_fields)
  
  epr_ecg_incl_freetext_table_primary[, order := 1:.N, by = epr_id]
  
  epr_ecg_incl_freetext_table_primary <- epr_ecg_incl_freetext_table_primary[order == 1][, c("order", "null_fields") := NULL]
  
  stopifnot(epr_ecg_incl_freetext_table_primary[, .N, by = epr_id][N > 1, .N] == 0)
  
  
# epr_cannulation_table  --------------------------------------------------

## Only interested if an icn_type has been attempted or not for each incident (binary), not site or if successful (checked with Jamie)  
  
  epr_cannulation_table[, c("icn_site", "icn_success") := NULL]
  
  epr_cannulation_table <- unique(epr_cannulation_table)

  epr_cannulation_table_wide <- dcast(epr_cannulation_table, epr_id ~ icn_type, value.var = "icn_type")  
  
  stopifnot(epr_cannulation_table_wide[, .N, by = epr_id][N > 1, .N] == 0)
  
  setnames(epr_cannulation_table_wide, setdiff(colnames(epr_cannulation_table_wide), "epr_id"),
           paste("cannulation", setdiff(colnames(epr_cannulation_table_wide), "epr_id"), sep = "_"))
  
  
# epr_drug_fields_table ---------------------------------------------------

## Only interested if a drug was given or not, so reduce to binary by unique
  
  epr_drug_fields_table <- unique(epr_drug_fields_table)
  
  epr_drug_fields_table_wide <- dcast(epr_drug_fields_table, epr_id ~ drug_mapped, value.var = "drug_mapped")
  
  setnames(epr_drug_fields_table_wide, setdiff(colnames(epr_drug_fields_table_wide), "epr_id"),
           paste("drug", setdiff(colnames(epr_drug_fields_table_wide), "epr_id"), sep = "_"))
  
  
# epr_urinary_observations_table ------------------------------------------
  
  ## Not urinary table, but obu stands for 'unable to record', these are not needed for final dataset
  
  
# Then will want to merge all tables on epr_id ----------------------------
  
  epr_id_merge_tables <- list(yas_ae_ecds_data,
                              max_news_score_data,
                              physcial_obvs_wide,
                              epr_pon_notification_type_table_wide,
                              epr_airways_intervention_table_wide,
                              epr_cardiac_respiratory_arrest_table,
                              epr_immobilisation_table_wide,
                              epr_advice_given_table_wide,
                              epr_mobility_table_wide,
                              epr_fast_stroke_table,
                              epr_point_of_referral_table_wide,
                              epr_psyc_observations_table_wide,
                              epr_ecg_incl_freetext_table_primary,
                              epr_cannulation_table_wide,
                              epr_drug_fields_table_wide)
  
  
  analysis_dataset <- Reduce(function(x, y) merge(x = x, y = y, by = "epr_id", all.x = TRUE), epr_id_merge_tables)
    
  
## Remove some fields 
  
  analysis_dataset[, c("epr_postcode", "epr_lat", "epr_long", "epr_lsoa11", "epr_ru11ind", "epr_oac11", "ecds_BIRTH_YEAR",
                       "ecds_CONCLUSION_TIME", "ecds_ASSESSMENT_TIME", "ecds_DEPARTURE_TIME", "ecds_SEEN_TIME") := NULL]
  
  
## Turn datetime into dates only
  
  analysis_dataset[, epr_incident_datetime := as.Date(epr_incident_datetime)]
  analysis_dataset[, observations_recorded_time_primary := as.Date(observations_recorded_time_primary)]
  analysis_dataset[, observations_recorded_time_subsequent := as.Date(observations_recorded_time_subsequent)]
  
  analysis_dataset[, ARRIVAL_TIME := as.Date(ARRIVAL_TIME)]
  
  analysis_dataset[, ecds_INJURY_TIME := as.Date(ecds_INJURY_TIME)]
  analysis_dataset[, ecds_INVESTIGATION_TIME_1 := substr(ecds_INVESTIGATION_TIME_1, 12, 19)]
  analysis_dataset[, ecds_TREATMENT_TIME_1 := substr(ecds_TREATMENT_TIME_1, 12, 19)]
  
  
  # Save data ---------------------------------------------------------------
  ## Save as both csv and as an rsa object as Jamie is in R as well
  
  save_time <- getDateTimeForFilename()
  
  write.csv(analysis_dataset, file = paste0("data/data_for_analysis/cohort_", save_time, "_yas_ae_ecds_phys_obvs.csv"))
  saveRDS(analysis_dataset, file = paste0("data/data_for_analysis/cohort_", save_time, "_yas_ae_ecds_phys_obvs.rds"))
  
  