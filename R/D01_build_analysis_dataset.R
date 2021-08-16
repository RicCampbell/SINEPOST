## Script for creating analysis dataset
## Adds fields from other table to 'epr_single..' table
## Creates and adds derived outcome fields

library(data.table)
source("R/cleaning_fns_etl.r")


## Load in standardised YAS tables, and AE linked YAS data

### NEED TO CHANGE TO READ IN YAS_AE_ECDS DATA - check this all the way through and that don't lose any fields at any point

  epr_data_object_names <- load("data/datasets/cohort_processed_epr_table_2021-08-03-094902.rda")
  yas_ae_ecds_data <- readRDS("data/datasets/yas_ae_ecds_linked_2021-08-05-144835.rds")
  
  incident_hospital_geo_data <- readRDS("data/datasets/incident_hospital_geo_data_2021-08-16-???????.rds")

  
# Add in desired fields from other YAS tables, and add prefix for each table --------

## Take number or records/epr_ids to check merges later
  
  epr_id_count <- yas_ae_ecds_data[, .N]
  
  
## Find max news scores for each epr_id
  
  max_news_score_data <- epr_news_score_table[, .SD[which.max(news_score)], by = epr_id]
  
  setnames(max_news_score_data, setdiff(colnames(max_news_score_data), "epr_id"),
           paste("epr", setdiff(colnames(max_news_score_data), "epr_id"), sep = "_"))
           
  stopifnot(uniqueN(epr_news_score_table[, epr_id]) == max_news_score_data[, .N])
  

# Physical observations ---------------------------------------------------

  ## Remove capital from obvs type so field names are still all lower case later
  
  epr_phys_observations_table[, observation_type := tolower(observation_type)]
  
  
## ** Not using MentalCapacity obvs - remember to put in processing doc ** - checked
  
## Want to take the Primary observation, and the 'first' Subsequent observation

## Check there is only one Primary set of observations for epr_ids  
  
  stopifnot(epr_phys_observations_table[observation_type == "primary", .N, by = epr_id][N > 1, .N] == 0)
  
  
## Make table of just Primary obvs
  
  all_primary_obvs <- epr_phys_observations_table[observation_type == "primary"]
  
  stopifnot(all_primary_obvs[, .N, by = epr_id][N > 1, .N] == 0)

  
## Does any epr have more than one record for each Subsequent-datetime pair - YES, max of 3
  
  epr_phys_observations_table[, .N, by = .(epr_id, observations_recorded_time)][N > 1, .N, by = N]
  

## Are these Subsequent-datetime pairs duplicates?
  ## Get table of the Subsequent-datetime pairs that are the same   
  
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

## Label all subsequent-time pairs in order, first for when there isn't multiple timedate pairs, and then when there is
    
    setorder(epr_phys_observations_table, epr_id, observation_type, observations_recorded_time, na.last = TRUE)
    
    epr_phys_observations_table[observation_type == "subsequent", subsequent_order := 1:.N, by = .(epr_id)]
    epr_phys_observations_table[observation_type == "subsequent", subsequent_time_count := .N, by = .(epr_id, observations_recorded_time)]
    
  ## Take subsequent phys obvs where first subsequent is unique timedate
    
    unique_first_subsequent_obvs <- epr_phys_observations_table[subsequent_order == 1 & subsequent_time_count == 1]
    
    stopifnot(unique_first_subsequent_obvs[, .N, by = epr_id][N > 1, .N] == 0)
    
  ## Take subsequent phys obvs where first subsequent is NOT unique timedate (== 1 means only have one line per epr_id so have to get all rows for these ids)
    
    multiple_first_subsequent_obvs_ids <- epr_phys_observations_table[subsequent_order == 1 & subsequent_time_count > 1, epr_id]
    
    multiple_first_subsequent_obvs <- epr_phys_observations_table[(epr_id %chin% multiple_first_subsequent_obvs_ids) & observation_type == "subsequent"]
  
    
  ## Find most non-null set of obvs  
    
    multiple_first_subsequent_obvs[, null_fields := rowSums(is.na(multiple_first_subsequent_obvs))]
    
    setorder(multiple_first_subsequent_obvs, epr_id, observations_recorded_time, null_fields, na.last = TRUE)
    
    multiple_first_subsequent_obvs[, order := 1:.N, by = epr_id]  ##Still need tie breaker but yeah
    
    #multiple_first_subsequent_obvs[, .N, by = .(epr_id, observations_recorded_time, null_fields)][N > 1, uniqueN(epr_id)]
    
    unique_multiple_first_subsequent_obvs <- multiple_first_subsequent_obvs[order == 1]
    
    
  ## Remove extra cols and bind together
    
    unique_first_subsequent_obvs[, c("subsequent_order", "subsequent_time_count", "N_records") := NULL]
    unique_multiple_first_subsequent_obvs[, c("subsequent_order", "subsequent_time_count", "N_records", "null_fields", "order") := NULL]
    
    
  ## Create id list for subsequent obvs that had multiple records for datetime 
    
    unique_multiple_first_subsequent_obvs_ids <- unique_multiple_first_subsequent_obvs[, epr_id]
    
    
  ## Bind all physical obvs that we now how together  
      
    all_obvs_want <- rbind(all_primary_obvs, 
                           rbind(unique_first_subsequent_obvs, unique_multiple_first_subsequent_obvs))

    
  ## Cast to get wide version
    
    phys_obs_cols <- colnames(all_obvs_want)[!(colnames(all_obvs_want) %in% c("epr_id", "observation_type"))]
    
    physcial_obvs_wide <- dcast(all_obvs_want, epr_id ~ observation_type, value.var = phys_obs_cols)

    
  ## Create a field to flag if subsequent obvs had multiple records for datetime
    
    physcial_obvs_wide[, multi_timedate_obvs := (epr_id %chin% unique_multiple_first_subsequent_obvs_ids)]
    
    
  ## Create a field to flag if a secondary obvs is prior to primary
    
    physcial_obvs_wide[, subsequent_before_primary := (observations_recorded_time_primary > observations_recorded_time_subsequent)]
    
  

## Then will want to merge all tables on epr_id
    
    epr_id_merge_tables <- list(yas_ae_ecds_data,
                                max_news_score_data,
                                physcial_obvs_wide)


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
  
  
  