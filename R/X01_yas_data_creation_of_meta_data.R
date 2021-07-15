### YAS data processing, and creation of meta data sheets

library(data.table)
library(readxl)
library(googlesheets4)
library(lubridate)

## Read in data

  epr_save_id <- "2021-05-11 131114"
  epr_table_names <- load(paste0("data/datasets/epr_tables-", epr_save_id, ".rda"))
  
  yas_data_mapping <- data.table(read_excel("D:/reference_data/yas_meta_data_sinepost.xlsx",
                                            sheet = "yas_meta_data_sinepost",
                                            col_names = TRUE,
                                            col_types = "text",
                                            trim_ws = TRUE))


## CPR start time to time (from character) - tricky as would have to figure out date for certain
  
  # epr_cardiac_respiratory_arrest_table[, test := as.POSIXct(cpr_started_time, format = "%H:%M:%OS", tz = "GMT")]


## Completeness, and ranges within each table
  
  all_numerical_variable_names <- yas_data_mapping[(field_class == "numeric" | field_class == "integer") & wanted_col == TRUE 
                                                   & destination_field != "epr_id" & destination_field != "patient_id"
                                                   , destination_field]
  
  date_or_time_variable_names <- yas_data_mapping[field_class == "POSIXct|POSIXt" & wanted_col == TRUE 
                                                  & destination_field != "epr_id" & destination_field != "patient_id"
                                                  , destination_field]
  
  numerical_and_dates_variable_names <- c(all_numerical_variable_names, date_or_time_variable_names)

  
  all_tables <- sapply(epr_table_names, function(epr_table_name)  {
    
    dt <- get(epr_table_name)
    table_cols <- colnames(dt)
    
    sapply(table_cols, function(col_name, epr_table_name)  {
      
      dt <- get(epr_table_name)
      
      
      number_of_non_nas <- dt[!is.na(get(col_name)), .N]
      col_completeness <- round((number_of_non_nas/dt[, .N])*100, digits = 2)
      
      number_of_non_nas_per_id <- dt[!is.na(get(col_name)), .N, by = epr_id][, .N]
      col_completeness_per_id <- round((number_of_non_nas_per_id/uniqueN(dt[, epr_id]))*100, digits = 2)
      
      max_number_of_distinct_values <- dt[!is.na(get(col_name)), .N, by = epr_id][, max(N)]
      
      if(col_name %chin% numerical_and_dates_variable_names)  {
        
        min_value <- dt[, min(get(col_name), na.rm = TRUE)]
        max_value <- dt[, max(get(col_name), na.rm = TRUE)]
        fith_percentile <- quantile(dt[, get(col_name)], prob = 0.05, na.rm = TRUE)
        ninty_fith_percentile <- quantile(dt[, get(col_name)], prob = 0.95, na.rm = TRUE)
        output_dt <- data.table(field_name = col_name,
                                field_group = epr_table_name,
                                completeness_in_table = col_completeness,
                                completeness_in_table_per_id = col_completeness_per_id,
                                max_number_of_distinct_values = max_number_of_distinct_values,
                                min_value = min_value,
                                max_value = max_value,
                                fith_percentile = fith_percentile,
                                ninty_fith_percentile = ninty_fith_percentile,
                                low_accepted_limit = "required",
                                high_accepted_limit = "required")
        
        sheet_append(output_dt,
                     ss = "https://docs.google.com/spreadsheets/d/1XsyQ0WX8-JquwCn8DGz01-mdIwprK-K4TsPjOJNK_0o/edit#gid=0",
                     sheet = "completeness_and_ranges")
        
        print(output_dt)
        
      } else if (col_name != "epr_id" & !(col_name %chin% numerical_and_dates_variable_names)) {
        
        output_dt <- data.table(field_name = col_name,
                                field_group = epr_table_name,
                                completeness_in_table = col_completeness,
                                completeness_in_table_per_id = col_completeness_per_id,
                                max_number_of_distinct_values = max_number_of_distinct_values)
        
        print(output_dt)
        
        sheet_append(output_dt,
                     ss = "https://docs.google.com/spreadsheets/d/1XsyQ0WX8-JquwCn8DGz01-mdIwprK-K4TsPjOJNK_0o/edit#gid=0",
                     sheet = "completeness_and_ranges")
        
      } 
    }, epr_table_name = epr_table_name)
  })



## Create a sheet for each categorical (character) col with all values
  
## Get all categorical col names
  
  all_categorical_variable_names <- yas_data_mapping[field_type == "character" & wanted_col == TRUE 
                                                     & destination_field != "epr_id" & destination_field != "patient_id"
                                                     , destination_field]

  
## Create a sheet in one google spreadsheet for each categorical variable (that has under 120 values)

  output_col_names <- data.table("field_name", "field_value", "count", "end_value")
  
  lapply(epr_table_names, function(epr_table_name)  {
    
    dt <- get(epr_table_name)
    table_cols <- colnames(dt)
    character_cols <- table_cols[table_cols %chin% all_categorical_variable_names]
    
    if(length(character_cols) > 0)  {
      
      sheet_add(ss = "https://docs.google.com/spreadsheets/d/1OboCOP7gPhpQlFY_Uf6w8WBdy2qytCTrcwy65XsESr4/edit#gid=0",
                sheet = epr_table_name)
      
      sheet_append(output_col_names,
                   ss = "https://docs.google.com/spreadsheets/d/1OboCOP7gPhpQlFY_Uf6w8WBdy2qytCTrcwy65XsESr4/edit#gid=0",
                   sheet = epr_table_name)
      
      sapply(character_cols, function(col_name, epr_table_name)  {
        
        dt <- get(epr_table_name)[, ..character_cols]
        list_of_values <- unique(dt[, ..col_name], na.rm = TRUE)
        
        if(list_of_values[, .N] < 120L) {
          
          output_table <- data.table(table(dt[, ..col_name]))
          
          output_table[, ':=' (field_name = col_name,
                               end_value = "")]
          
          setnames(output_table, colnames(output_table), c("field_value", "count", "field_name", "end_value"))
          setcolorder(output_table, c("field_name", "field_value", "count", "end_value"))
          setorder(output_table, -"count")
          
          sheet_append(output_table,
                       ss = "https://docs.google.com/spreadsheets/d/1OboCOP7gPhpQlFY_Uf6w8WBdy2qytCTrcwy65XsESr4/edit#gid=0",
                       sheet = epr_table_name)
        }
        
      }, epr_table_name = epr_table_name)
    }
  })  

  
  sheet_append(data.table(table(epr_drug_fields_table$drug_name)),
               ss = "https://docs.google.com/spreadsheets/d/1OboCOP7gPhpQlFY_Uf6w8WBdy2qytCTrcwy65XsESr4/edit#gid=0",
               sheet = "epr_drug_fields_table")
  
## Table of maximum number of records by epr_id for each table  
  
  max_records_per_id_each_table <- data.table(table_name = epr_table_names,
                                              max_epr_for_table = sapply(epr_table_names, function(table_name)  {
                                                dt <- get(table_name)
                                                dt[, .N, by = epr_id][, max(N)]
                                              }))
  
  sheet_write(max_records_per_id_each_table,
              ss = "https://docs.google.com/spreadsheets/d/1XsyQ0WX8-JquwCn8DGz01-mdIwprK-K4TsPjOJNK_0o/edit#gid=0",
              sheet = max_rows_per_epr_id_each_table)
                                              
  
  test <- data.table(max_records_per_id_each_table)
  
## Create missing field of was there an ambulance within the last 24 hours or on in the next 24 hours





## Number of physical obvs per epr_id;