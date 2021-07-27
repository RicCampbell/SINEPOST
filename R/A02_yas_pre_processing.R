## Script that takes YAS ePR data, changes field names, reduces to only cols of interest, and splits into grouped tables
## Grouped tables are also reduced to unique rows and rows that have only epr_id present are removed.
## Tables are saved as one .rda file

library(data.table)
library(readxl)
library(openxlsx)
source("R/cleaning_fns_etl.r")


# Read in data ------------------------------------------------------------

  yas_data <- data.table(readRDS("D:/data/yas/sinepost.rds"))
  epr_drug_fields_table <- data.table(readRDS("D:/data/yas/missing-drugs-final.rds"))

  setnames(yas_data, make.names(tolower(colnames(yas_data)), unique = TRUE))
  setnames(epr_drug_fields_table, make.names(tolower(colnames(epr_drug_fields_table)), unique = TRUE))

  
## Read in standardised field name list/start meta-data
  
  yas_data_mapping <- data.table(read_excel("D:/reference_data/yas_meta_data_sinepost.xlsx",
                                            sheet = "yas_meta_data_sinepost",
                                            col_names = TRUE,
                                            col_types = "text",
                                            trim_ws = TRUE))
  

# Data check --------------------------------------------------------------

## Check all ages are in years, will remove this col soon so check once here then don't have to deal with
  
  stopifnot(yas_data[pcragetype != "Y", .N] == 0)

    
## Check all rows interested in have a destination field and that there are no duplicates
  
  stopifnot(yas_data_mapping[wanted_col == TRUE & is.na(destination_field), .N] == 0)
  stopifnot(all(duplicated(yas_data_mapping[!is.na(destination_field), destination_field]) == FALSE))
  

# Reduce data to desired fields and standardise -------------------------------------------

## Remove drug fields from meta data table as will be kept separate anyway, and merge would be many-to-many, and casting would create more cols
  
  yas_data_mapping_minus_drugs <- yas_data_mapping[field_group != "drugs" | is.na(field_group)]
  
  
## Reduce YAS data to just cols that are interested in (no destination fields for unwanted cols)
  
  wanted_cols <- yas_data_mapping_minus_drugs[wanted_col == TRUE, source_field]
  
  yas_data <- yas_data[, ..wanted_cols]
  
  
## Re-code all the fields so standardised
  
  setnames(yas_data, 
           yas_data_mapping_minus_drugs[wanted_col == TRUE, source_field],
           yas_data_mapping_minus_drugs[wanted_col == TRUE, destination_field])
  
  setnames(epr_drug_fields_table,
           yas_data_mapping[field_group == "drugs" | source_field == "unique_epr_id", source_field],
           yas_data_mapping[field_group == "drugs" | source_field == "unique_epr_id", destination_field])
  

  ## For testing - a save of this trimmed data has been saved
    # yas_data <- yas_data[1:100000]


# Clean data --------------------------------------------------------------

## Replace all empty strings with NA for character cols, also replace "n/a" with NA
  
  yas_data_character_cols_names <- yas_data_mapping_minus_drugs[wanted_col == TRUE & field_type == "character", destination_field]
  
  yas_data[, (yas_data_character_cols_names) := lapply(.SD, fn_removeBlanks), .SDcols = yas_data_character_cols_names]
  yas_data[, (yas_data_character_cols_names) := lapply(.SD, fn_removeValues, "n/a"), .SDcols = yas_data_character_cols_names]
  
  
## Remove any rows that have no data other than epr_id
  
  all_cols_not_epr_id <- colnames(yas_data)[!(colnames(yas_data) %chin% "epr_id")]
  
  yas_data <- yas_data[rowSums(is.na(yas_data[, ..all_cols_not_epr_id])) != ncol(yas_data)]
  

## Change columns that have wrong data type, also update the reference table for use later
  
  ## Guardian named to logical (0 = FALSE, 1 = TRUE)
  yas_data[, guardian_named := as.logical(guardian_named)]
  yas_data_mapping[destination_field == "guardian_named", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Nok named to logical (0 = FALSE, 1 = TRUE)
  yas_data[, nok_named := as.logical(nok_named)]
  yas_data_mapping[destination_field == "nok_named", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Parent named to logical (0 = FALSE, 1 = TRUE)
  yas_data[, parent_named := as.logical(parent_named)]
  yas_data_mapping[destination_field == "parent_named", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Social worker named to logical (0 = FALSE, 1 = TRUE)
  yas_data[, sw_named := as.logical(sw_named)]
  yas_data_mapping[destination_field == "sw_named", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Hypercapnic to logical (via numerical, 0 = FALSE, 1 = TRUE)
  yas_data[, hypercapnic_resp_failure := as.logical(as.integer(hypercapnic_resp_failure))]
  yas_data_mapping[destination_field == "hypercapnic_resp_failure", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Supplemental oxygen to logical
  yas_data[, obs_supplimental_oxygen := as.logical(obs_supplimental_oxygen)]
  yas_data_mapping[destination_field == "obs_supplimental_oxygen", ':=' (field_type = "logical", field_class = "logical")]
  
  
  ## Final impression code to numerical (from character)
  yas_data[, final_impression_code := as.numeric(final_impression_code)]
  yas_data_mapping[destination_field == "final_impression_code", ':=' (field_type = "double", field_class = "numeric")]
  
  
  ## Change receiving hospital to all upper case, remove apostrophes
  
  yas_data[, receiving_hospital := gsub("'", "", toupper(receiving_hospital))]
  
  ## Change drug name to upper case
  
  epr_drug_fields_table[, drug_name := toupper(drug_name)]
  
  
# Create ePR tables seperated into groups ---------------------------------

# Create epr_id 'one-to-one' table ----------------------------------------

  distinct_values_by_field_epr_id_wide <- yas_data[, lapply(.SD, uniqueN), by = epr_id, .SDcols = all_cols_not_epr_id][, lapply(.SD, max), .SDcols = all_cols_not_epr_id]
  
  distinct_values_by_field_epr_id <- melt(distinct_values_by_field_epr_id_wide,
                                          measure.vars = colnames(distinct_values_by_field_epr_id_wide),
                                          variable.name = "field",
                                          variable.factor = FALSE,
                                          value.name = "max_distinct_vals")
  
  distinct_values_by_field_epr_id <- merge(distinct_values_by_field_epr_id,
                                           yas_data_mapping[, .(destination_field, field_group)],
                                           by.x = "field",
                                           by.y = "destination_field",
                                           all.x = TRUE)
  
  
## Group all cols that are 'one-to-one' and are not part of a grouping and add in epr_id
  
  epr_id_single_value_cols <- distinct_values_by_field_epr_id[is.na(field_group) & max_distinct_vals == 1, field]
  
  epr_id_single_value_cols <- c("epr_id", epr_id_single_value_cols)
  
  
## Create table with all values but just these cols and only distinct values
  
  epr_single_value_fields_table <- unique(yas_data[, ..epr_id_single_value_cols])
  

## Check number of rows is the same as unique epr ids in all yas data
  
  stopifnot(epr_single_value_fields_table[, .N] == yas_data[, uniqueN(epr_id)])
  
  
# Create epr id one-to-multiple tables ------------------------------------

## Get list of cols, not in groups, that are not one-to-one relationships to epr_id
  
  epr_multi_value_ungrouped_fields <- distinct_values_by_field_epr_id[is.na(field_group) & max_distinct_vals >1, field]
  
  epr_multi_value_ungrouped_field_tables <- sapply(epr_multi_value_ungrouped_fields, function(field, data) {
    fields <- c("epr_id", field)
    table_name <- paste("epr", field, "table", sep = "_")
    assign(table_name, unique(data[!is.na(get(field)), ..fields]), envir = parent.frame(n = 3))
    return(table_name)
  }, data = yas_data)
  
  
## Get list of groups that are not a one-to-one relationship to epr_id
  
  epr_multi_value_field_groups <- distinct_values_by_field_epr_id[!is.na(field_group), unique(field_group)]
  
  epr_multi_value_grouped_field_tables <- sapply(epr_multi_value_field_groups, function(field_group_name, data, groupings) {
    fields <- c("epr_id", groupings[field_group == field_group_name, field])
    table_name <- paste("epr", field_group_name, "table", sep = "_")
    dt <- data[, ..fields][, .non_missing_obs. := Reduce(`+`, lapply(.SD, function(x) !is.na(x)))][.non_missing_obs. != 1][, .non_missing_obs. := NULL]
    assign(table_name, unique(dt), envir = parent.frame(n = 3))
    return(table_name)
  }, data = yas_data, groupings = distinct_values_by_field_epr_id)
  

# Change dates to proper format (currently look right, but are not when do date maths), back to string then to datetime
# Using how date originally came gives error, "do not know how to convert 'time1' to class POSIXct when using difftime, even though is of class POSIXct

  epr_immobilisation_table[, iex_time := format(as.POSIXct(iex_time, "YYYY-MM-DD HH:MM:SS", tz = "Europe/London"))]
  epr_single_value_fields_table[, incident_datetime := format(as.POSIXct(incident_datetime, "YYYY-MM-DD HH:MM:SS", tz = "Europe/London"))]
  epr_cardiac_respiratory_arrest_table[, cardiac_arrest_time := format(as.POSIXct(cardiac_arrest_time, "YYYY-MM-DD HH:MM:SS", tz = "Europe/London"))]
  epr_cardiac_respiratory_arrest_table[, resuscitation_ceased_time := format(as.POSIXct(resuscitation_ceased_time, "YYYY-MM-DD HH:MM:SS", tz = "Europe/London"))]
  epr_phys_observations_table[, observations_recorded_time := format(as.POSIXct(observations_recorded_time, "YYYY-MM-DD HH:MM:SS", tz = "Europe/London"))]


  epr_immobilisation_table[, iex_time := lubridate::fast_strptime(iex_time, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
  epr_single_value_fields_table[, incident_datetime := lubridate::fast_strptime(incident_datetime, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
  epr_cardiac_respiratory_arrest_table[, cardiac_arrest_time := lubridate::fast_strptime(cardiac_arrest_time, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
  epr_cardiac_respiratory_arrest_table[, resuscitation_ceased_time := lubridate::fast_strptime(resuscitation_ceased_time, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
  epr_phys_observations_table[, observations_recorded_time := lubridate::fast_strptime(observations_recorded_time, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]

  
# Save data ---------------------------------------------------------------

## Save all tables, that have been unique'd and all rows containing only epr_id have been removed
  
  save_time <- getDateTimeForFilename()
  
  save(list = c("epr_single_value_fields_table",
                "epr_drug_fields_table",
                epr_multi_value_ungrouped_field_tables,
                epr_multi_value_grouped_field_tables),
       file = paste0("data/datasets/epr_tables_", save_time, ".rda"))
  
  
## Write over meta data spreadsheet as some col data types have been changed
  
  write.xlsx(yas_data_mapping,
             file = "D:/reference_data/yas_meta_data_sinepost.xlsx",
             sheetName = "yas_meta_data_sinepost")
  
  