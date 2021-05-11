## Script for looking at the data from YAS for the SINEPOST project

library(data.table)
library(DataExplorer)
library(readxl)
library(rmarkdown)
library(lubridate)
library(ggplot2)

## Read in data (using saved rds as original source data takes time to load due to high compression)

  yas_data <- fread("D:/data/yas/sinepost.csv",
                    colClasses = "character",
                    na.strings = "")
  
  
## Change col names to lower case
  
  setnames(yas_data, colnames(yas_data), make.names(tolower(colnames(yas_data))))

    
## Separate full duplicate
  
  yas_data[, duplicated_record := duplicated(yas_data)]
  
  yas_data_duplicates <- yas_data[duplicated_record == TRUE]
  yas_no_duplicates <- yas_data[duplicated_record == FALSE]

  
  number_of_full_dups <- yas_data[, .N] - yas_no_duplicates[ ,.N]
  percentage_full_dups <- (number_of_full_dups/yas_data[, .N])*100L
  

  rm(yas_data) 
  
  
## Change col types as needed
  
  yas_no_duplicates[, incdate := as.Date(incdate, format = "%Y-%m-%d")]
  yas_no_duplicates[, pcrage := as.integer(pcrage)]
  
  
## Floor dates for plotting
  
  yas_no_duplicates[, incident_month := floor_date(incdate, unit = "month")]
  yas_no_duplicates[, incident_week := floor_date(incdate, unit = "week")]
  
  
## Create age groups
  
  yas_no_duplicates[, age_group := cut(pcrage, c(seq(0, 120, 10), Inf), include.lowest = TRUE)]

  
## Information sharing agreement does not have list of variables that should be produced  
  
  
## Read in meta data for YAS (from PREIST for now)
  
  epr_yas_meta <- data.table(read_excel("D:/reference_data/YAS meta data.xlsx",
                                        sheet = "ePR data",
                                        col_names = TRUE,
                                        col_types = "text",
                                        trim_ws = TRUE))
  
  cad_yas_meta <- data.table(read_excel("D:/reference_data/YAS meta data.xlsx",
                                        sheet = "CAD data",
                                        col_names = TRUE,
                                        col_types = "text",
                                        trim_ws = TRUE))
  
## Get YAS field names
  
  epr_meta_fields <- epr_yas_meta$'variable name (as supplied)'
  cad_meta_fields <- cad_yas_meta$'variable name (as supplied)'

  
## Look at which fields we have seen before and which we haven't
  
  yas_colnames <- colnames(yas_no_duplicates)
  
  epr_known_fields <- yas_colnames[yas_colnames %chin% epr_meta_fields]
  cad_known_fields <- yas_colnames[yas_colnames %chin% cad_meta_fields]
  
  unknown_fields <- yas_colnames[!(yas_colnames %chin% epr_meta_fields | yas_colnames %chin% cad_meta_fields)]
  epr_fields_do_not_have <- epr_meta_fields[!epr_meta_fields %chin% yas_colnames]
  

## Take length of unknown cols so can split into two reports so doesn't run out of memory  
  
    number_unknown <- length(unknown_fields)
  
  
## Use Data Explorer Package on dataset
  
  DataExplorer::create_report(yas_no_duplicates[, ..epr_known_fields],
                              output_format = html_document(),
                              output_file = "YAS data report - known fields.html",
                              output_dir = "RMarkdown")
  
  
  DataExplorer::create_report(yas_no_duplicates[, ..unknown_fields][, 1:floor(number_unknown/2)],
                              output_format = html_document(),
                              output_file = "YAS data report - first half unknown fields.html",
                              output_dir = "RMarkdown")
  
  DataExplorer::create_report(yas_no_duplicates[, ..unknown_fields][, ceiling(number_unknown/2):number_unknown],
                              output_format = html_document(),
                              output_file = "YAS data report - second half unknown fields.html",
                              output_dir = "RMarkdown")
  
  
## Create a few graphs of records over time

  records_per_month_plot <- ggplot(yas_no_duplicates[, .N, by = incident_month],
                                   aes(x = incident_month, y = N)) +
                                     geom_point() + 
                                     geom_smooth() + 
                                     ggtitle("Ambulance records per month")
  
  records_per_week_plot <- ggplot(yas_no_duplicates[, .N, by = incident_week],
                                   aes(x = incident_week, y = N)) +
                            geom_point() + 
                            geom_smooth() + 
                            ggtitle("Ambulance records per month")
  
  age_plot <- ggplot(yas_no_duplicates[, .N, by = age_group],
                     aes(x = age_group, y = N)) +
                       geom_point() + 
                       geom_smooth() + 
                       ggtitle("Ambulance records per age group")
  
  age_month_plot <- ggplot(yas_no_duplicates[, .N, by = .(incident_month, age_group)],
                           aes(x = incident_month, y = N, color = age_group)) +
                             geom_point() + 
                             geom_smooth() + 
                             ggtitle("Ambulance records by age group per month")
  
  
## Create desc stats function
  
  desc_stats_func <- function(data_table, col_name) {
    
    desc_stat <- data.table(variable = gsub("_", " ", col_name))
    
    desc_stat[, count := length(data_table[, get(col_name)])]
    desc_stat[, missing_values := data_table[is.na(get(col_name)), .N]]
    desc_stat[, missing_percent := round((missing_values/count)*100, digits = 2)]
    
    if(typeof(data_table[, get(col_name)]) == "integer" | typeof(data_table[, get(col_name)]) == "double")  {
      
      desc_stat[, range := data_table[, max(get(col_name), na.rm = TRUE)] - data_table[, min(get(col_name), na.rm = TRUE)]]
      desc_stat[, mean := data_table[, round(mean(get(col_name), na.rm = TRUE), digits = 2)]]
      desc_stat[, median := data_table[, median(get(col_name), na.rm = TRUE)]]
      
    }
    
    if(col_name == "age_years") {
      
      desc_stat[, under_sixteen := data_table[get(col_name) < 16, .N]]
    }
    
    return(desc_stat)
    
  }
 
## Change all variable to numeric types that want desc figures
  
  numeric_fields <- c("obsbloodsugar", "obsbpdiastolic", "obsbpsystolic", "obsetco2", "obspeakflow", "obspulsemanual", "obsresprate",
                      "obssp02air", "obstempdeg")
  
  yas_no_duplicates[, (numeric_fields) := lapply(.SD, as.double), .SDcols =  numeric_fields]
   
  
  blood_sugar_desc <- desc_stats_func(yas_no_duplicates, "obsbloodsugar")
  dia_blood_pressure_desc <- desc_stats_func(yas_no_duplicates, "obsbpdiastolic")
  sys_blood_pressure_desc <- desc_stats_func(yas_no_duplicates, "obsbpsystolic")
  co2_desc <- desc_stats_func(yas_no_duplicates, "obsetco2")
  peak_flow_desc <- desc_stats_func(yas_no_duplicates, "obspeakflow")
  pulse_desc <- desc_stats_func(yas_no_duplicates, "obspulsemanual")
  resp_rate_desc <- desc_stats_func(yas_no_duplicates, "obsresprate")
  o2_desc <- desc_stats_func(yas_no_duplicates, "obssp02air")
  temperature_desc <- desc_stats_func(yas_no_duplicates, "obstempdeg")

  
  desc_stats <- rbindlist(list(blood_sugar_desc, dia_blood_pressure_desc, sys_blood_pressure_desc, co2_desc, peak_flow_desc, pulse_desc,
                          resp_rate_desc, o2_desc, temperature_desc))
  
  
  
  
  
  
  
  
  
  
  
    