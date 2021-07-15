library(data.table)
library(readxl)
library(googlesheets4)
source("R/cleaning_fns_etl.r")

## Read in data

yas_data <- fread("D:/data/yas/sinepost.csv",
                  colClasses = "character",
                  na.strings = c("", "n/a"))

setnames(yas_data, make.names(tolower(colnames(yas_data)), unique = TRUE))


## Read in standardised field name list/start meta-data

yas_data_mapping <- data.table(read_excel("D:/reference_data/yas_meta_data_sinepost.xlsx",
                                          sheet = "yas_meta_data_sinepost",
                                          col_names = TRUE,
                                          col_types = "text",
                                          trim_ws = TRUE))

## Check all ages are in years

stopifnot(yas_data[pcragetype != "Y", .N] == 0)


## Get completeness of all cols
  
  col_names <- colnames(yas_data)
  
  completness_of_cols <- transpose(yas_data[, lapply(.SD, function(col_name) round((sum(!is.na(col_name))/.N)*100L, digits = 2)), .SDcols = col_names], keep.names = "field_name")
  
  setnames(completness_of_cols, c("field_name", "completness_percentage"))
  

## Completeness of all cols by epr_id    
  
  completness_of_cols_by_epr <- yas_data[, lapply(.SD, function(col_name) sum(!is.na(col_name))), by = unique_epr_id, .SDcols = col_names]
  
  percentage_completness_by_epr_id <- completness_of_cols_by_epr[, lapply(.SD, function(x) round((sum(x != 0L)/.N) *100L, digits = 2)), .SDcols = col_names]
  
  transposed_percentage_completness_by_epr_id <- transpose(percentage_completness_by_epr_id, keep.names = "field_name")
  
  setnames(transposed_percentage_completness_by_epr_id, c("field_name", "completness_percentage_by_epr"))
  
  
# Get min/max of all cols -------------------------------------------------

  ## Have to change to correct type as everything is a character
  
  int_cols <- yas_data_mapping[data_type == "int", source_field]
  decimal_cols <- yas_data_mapping[data_type == "float", source_field]
  date_cols <- yas_data_mapping[data_type == "datetime" | data_type == "date", source_field]
  
  min_max_cols <- c(int_cols, decimal_cols, date_cols)
  
  yas_data[, (int_cols) := lapply(.SD, as.integer), .SDcols = int_cols]
  yas_data[, (decimal_cols) := lapply(.SD, as.double), .SDcols = decimal_cols]
  yas_data[, (date_cols) := lapply(.SD, function(col_name) as.Date(col_name, "%Y-%m-%d")), .SDcols = date_cols]
  
  min_values <- yas_data[, lapply(.SD, function(col_name) min(col_name, na.rm = TRUE)), .SDcols = min_max_cols]
  max_values <- yas_data[, lapply(.SD, function(col_name) max(col_name, na.rm = TRUE)), .SDcols = min_max_cols]
  
  transpose_min_values <- transpose(min_values, keep.names = "field_name")
  transpose_max_values <- transpose(max_values, keep.names = "field_name")
  
  setnames(transpose_min_values, c("field_name", "min_value"))
  setnames(transpose_max_values, c("field_name", "max_value"))
  
  
# Number of distinct values per col ---------------------------------------

  distinct_values <- yas_data[, lapply(.SD, function(col_name) uniqueN(col_name, na.rm = TRUE)), .SDcols = col_names]
  transpose_distinct_values <- transpose(distinct_values, keep.names = "field_name")
  setnames(transpose_distinct_values, c("field_name", "number_distinct_values"))

  

# Merge all findings together ---------------------------------------------

  
  
  yas_meta_data_report <- merge(yas_data_mapping,
                                completness_of_cols,
                                by.x = "source_field",
                                by.y = "field_name",
                                all.x = TRUE)
  
  yas_meta_data_report <- merge(yas_meta_data_report,
                                transposed_percentage_completness_by_epr_id,
                                by.x = "source_field",
                                by.y = "field_name",
                                all.x = TRUE)
  
  yas_meta_data_report <- merge(yas_meta_data_report,
                                transpose_min_values,
                                by.x = "source_field",
                                by.y = "field_name",
                                all.x = TRUE)
  
  yas_meta_data_report <- merge(yas_meta_data_report,
                                transpose_max_values,
                                by.x = "source_field",
                                by.y = "field_name",
                                all.x = TRUE)
  
  yas_meta_data_report <- merge(yas_meta_data_report,
                                transpose_distinct_values,
                                by.x = "source_field",
                                by.y = "field_name",
                                all.x = TRUE)
  
  
  sheet_write(yas_meta_data_report,
              ss = "https://docs.google.com/spreadsheets/d/1tQIPob11DT9pzdH8K-yzrekwykbb-EW_X9BcX8QOxuE/edit#gid=1174136342",
              sheet = "field_mapping_and_values")
  
  fwrite(yas_meta_data_report, file = "D:/reference_data/temp/yas_meta_data_report.csv")
  
  
## Working by field group
  
  field_groupings <- yas_data_mapping[!is.na(field_group), .(source_field, field_group)]
  
  cannulation_fields <- c(field_groupings[field_group == "cannulation", source_field], "unique_epr_id")
  immobilisation_fields <- c(field_groupings[field_group == "immobilisation", source_field], "unique_epr_id")
  advice_given_fields <- c(field_groupings[field_group == "advice_given", source_field], "unique_epr_id")
  cardiac_respiratory_arrest_fields <- c(field_groupings[field_group == "cardiac_respiratory_arrest", source_field], "unique_epr_id")
  airways_intervention_fields <- c(field_groupings[field_group == "airways_intervention", source_field], "unique_epr_id")
  mobility_fields <- c(field_groupings[field_group == "mobility", source_field], "unique_epr_id")
  ecg_incl_freetext_fields <- c(field_groupings[field_group == "ecg_incl_freetext", source_field], "unique_epr_id")
  fast_stroke_fields <- c(field_groupings[field_group == "fast_stroke", source_field], "unique_epr_id")
  phys_observations_fields <- c(field_groupings[field_group == "phys_observations", source_field], "unique_epr_id")
  urinary_observations_fields <- c(field_groupings[field_group == "urinary_observations", source_field], "unique_epr_id")
  point_of_care_fields <- c(field_groupings[field_group == "point_of_care", source_field], "unique_epr_id")
  point_of_referral_fields <- c(field_groupings[field_group == "point_of_referral", source_field], "unique_epr_id")
  psyc_observations_fields <- c(field_groupings[field_group == "psyc_observations", source_field], "unique_epr_id")
  
  
  number_eprs <- completness_of_cols_by_epr[, .N]
    
## All fields complete
  
  cannulation_sum <- completness_of_cols_by_epr[, ..cannulation_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  immobilisation_sum <- completness_of_cols_by_epr[, ..immobilisation_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  advice_given_sum <- completness_of_cols_by_epr[, ..advice_given_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  cardiac_respiratory_arrest_sum <- completness_of_cols_by_epr[, ..cardiac_respiratory_arrest_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  airways_intervention_sum <- completness_of_cols_by_epr[, ..airways_intervention_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  mobility_sum <- completness_of_cols_by_epr[, ..mobility_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  ecg_incl_freetext_sum <- completness_of_cols_by_epr[, ..ecg_incl_freetext_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  fast_stroke_sum <- completness_of_cols_by_epr[, ..fast_stroke_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  phys_observations_sum <- completness_of_cols_by_epr[, ..phys_observations_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  urinary_observations_sum <- completness_of_cols_by_epr[, ..urinary_observations_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  point_of_care_sum <- completness_of_cols_by_epr[, ..point_of_care_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  point_of_referral_sum <- completness_of_cols_by_epr[, ..point_of_referral_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  psyc_observations_sum <- completness_of_cols_by_epr[, ..psyc_observations_fields][, whole_set := all(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  
  
## Any fields complete
  
  cannulation_sum_any <- completness_of_cols_by_epr[, ..cannulation_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  immobilisation_sum_any <- completness_of_cols_by_epr[, ..immobilisation_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  advice_given_sum_any <- completness_of_cols_by_epr[, ..advice_given_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  cardiac_respiratory_arrest_sum_any <- completness_of_cols_by_epr[, ..cardiac_respiratory_arrest_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  airways_intervention_sum_any <- completness_of_cols_by_epr[, ..airways_intervention_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  mobility_sum_any <- completness_of_cols_by_epr[, ..mobility_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  ecg_incl_freetext_sum_any <- completness_of_cols_by_epr[, ..ecg_incl_freetext_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  fast_stroke_sum_any <- completness_of_cols_by_epr[, ..fast_stroke_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  phys_observations_sum_any <- completness_of_cols_by_epr[, ..phys_observations_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  urinary_observations_sum_any <- completness_of_cols_by_epr[, ..urinary_observations_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  point_of_care_sum_any <- completness_of_cols_by_epr[, ..point_of_care_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  point_of_referral_sum_any <- completness_of_cols_by_epr[, ..point_of_referral_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  psyc_observations_sum_any <- completness_of_cols_by_epr[, ..psyc_observations_fields][, whole_set := any(.SD), by = unique_epr_id][whole_set == TRUE, .N]/number_eprs*100
  
  all_values_available_by_group <- data.table(field_group = c("cannulation", "immobilisation", "advice_given", "cardiac_respiratory_arrest",
                                                              "airways_intervention", "mobility", "ecg_incl_freetext", "fast_stroke",
                                                              "phys_observations", "urinary_observations", "point_of_care",
                                                              "point_of_referral", "psyc_observations"),
                                              percent_epr_with_all_value_each_col = c(cannulation_sum, immobilisation_sum, advice_given_sum,
                                                                                       cardiac_respiratory_arrest_sum, airways_intervention_sum,
                                                                                       mobility_sum, ecg_incl_freetext_sum, fast_stroke_sum,
                                                                                       phys_observations_sum, urinary_observations_sum, point_of_care_sum,
                                                                                       point_of_referral_sum, psyc_observations_sum),
                                              percent_epr_with_any_value_each_col = c(cannulation_sum_any, immobilisation_sum_any, advice_given_sum_any,
                                                                                       cardiac_respiratory_arrest_sum_any, airways_intervention_sum_any,
                                                                                       mobility_sum_any, ecg_incl_freetext_sum_any, fast_stroke_sum_any,
                                                                                       phys_observations_sum_any, urinary_observations_sum_any, point_of_care_sum_any,
                                                                                       point_of_referral_sum_any, psyc_observations_sum_any))


  sheet_write(all_values_available_by_group,
              ss = "https://docs.google.com/spreadsheets/d/1tQIPob11DT9pzdH8K-yzrekwykbb-EW_X9BcX8QOxuE/edit#gid=1174136342",
              sheet = "completness_by_grouping")
  
  fwrite(all_values_available_by_group, file = "D:/reference_data/temp/yas_by_group_meta_data_report.csv")
  

  
  
  
## Reduce YAS data to just cols that are interested in

  wanted_cols <- yas_data_mapping[wanted_col == TRUE, source_field]
  
  yas_data <- yas_data[, ..wanted_cols]


## Check that there are no duplicates in destination field

  stopifnot(all(duplicated(yas_data_mapping[!is.na(destination_field), destination_field]) == FALSE))


## Re-code all the fields so standardised

  setnames(yas_data, yas_data_mapping[wanted_col == TRUE, source_field], yas_data_mapping[wanted_col == TRUE, destination_field]) 




# Read in epr data that has been split by tables --------------------------


  epr_save_id <- "2021-04-22 114404"

  epr_table_names <- load(paste0("data/datasets/epr_tables-", epr_save_id, ".rda"))
  
  ## test set
  epr_table_names <- load(paste0("data/datasets/epr_tables-2021-03-31 163742.rda"))
  
  epr_single_value_fields_table[, .N]
  epr_icn_site_table[, .N]
  epr_icn_type_table[, .N]              
  epr_iex_time_table[, .N]             
  epr_iex_type_table[, .N]             
  epr_ipa_type_table[, .N]              
  epr_ipa_written_table[, .N]
  epr_mobility_type_table[, .N]          
  epr_news_score_table[, .N]       
  epr_obu_observation_utr_measure_table[, .N]
  epr_obu_observation_utr_type_table[, .N]
  epr_obu_utr_reason_table[, .N]
  epr_pon_notification_type_table[, .N]
  epr_por_referral_accepted_table[, .N]
  epr_psy_result_table[, .N]
  epr_psy_type_table[, .N]           
  epr_referral_type_crew_table[, .N]
  epr_airways_intervention_table[, .N]
  epr_phys_observations_table[, .N]
  epr_cardiac_respiratory_arrest_table[, .N]
  epr_ecg_incl_freetext_table[, .N]
  epr_point_of_care_table[, .N]  
  epr_mobility_table[, .N]    
  epr_fast_stroke_table[, .N]
  
  
## Want to unique each table
  
  
  
  epr_single_value_fields_table <- unique(epr_single_value_fields_table)
  epr_icn_site_table <- unique(epr_icn_site_table)
  epr_icn_type_table <- unique(epr_icn_type_table)              
  epr_iex_time_table <- unique(epr_iex_time_table)           
  epr_iex_type_table <- unique(epr_iex_type_table)         
  epr_ipa_type_table <- unique(epr_ipa_type_table)             
  epr_ipa_written_table <- unique(epr_ipa_written_table)
  epr_mobility_type_table <- unique(epr_mobility_type_table)         
  epr_news_score_table <- unique(epr_news_score_table)      
  epr_obu_observation_utr_measure_table <- unique(epr_obu_observation_utr_measure_table)
  epr_obu_observation_utr_type_table <- unique(epr_obu_observation_utr_type_table)
  epr_obu_utr_reason_table <- unique(epr_obu_utr_reason_table)
  epr_pon_notification_type_table <- unique(epr_pon_notification_type_table)
  epr_por_referral_accepted_table <- unique(epr_por_referral_accepted_table)
  epr_psy_result_table <- unique(epr_psy_result_table)
  epr_psy_type_table <- unique(epr_psy_type_table)         
  epr_referral_type_crew_table <- unique(epr_referral_type_crew_table)
  epr_airways_intervention_table <- unique(epr_airways_intervention_table)
  epr_phys_observations_table <- unique(epr_phys_observations_table)
  epr_cardiac_respiratory_arrest_table <- unique(epr_cardiac_respiratory_arrest_table)
  epr_ecg_incl_freetext_table <- unique(epr_ecg_incl_freetext_table)
  epr_point_of_care_table <- unique(epr_point_of_care_table)  
  epr_mobility_table <- unique(epr_mobility_table)    
  epr_fast_stroke_table <- unique(epr_fast_stroke_table)

  

  
  
## Want percentage of total completeness
  
  
  
  
## Want percentage of completeness for each epr_id, so does each epr_id have [x] value or not
  
  col_names <- colnames(epr_point_of_care_table)
  
  
  #distinct_values_by_field_epr_id_wide <- yas_data[, lapply(.SD, uniqueN), by = epr_id, .SDcols = col_names][, lapply(.SD, max), .SDcols = col_names]
  test <- epr_point_of_care_table[, lapply(.SD, function(col_name) sum(!is.na(col_name))), by = epr_id, .SDcols = col_names]
  
  check <- test[, sapply(.SD, function(x) sum(x != 1)/.N), .SDcols = col_names]
  
  epr_point_of_care_table[, any(!is.na(receiving_hospital_department)), by = epr_id]
  
  
  
## Min and Max of each field