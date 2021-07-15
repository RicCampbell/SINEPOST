## Script for looking at the data from NHS Digital for the SINEPOST project

library(data.table)
library(DataExplorer)
library(readxl)
library(rmarkdown)
library(lubridate)
library(ggplot2)
source("R/cleaning_fns_etl.R")


## Read in all data from NHS Digital

  ecds <- fread("D:/data/nhs_digital/ecds/FILE0119464_NIC284866_HES_ECDS_201999.txt",
                colClasses = "character",
                na.strings = "")
  
  ae <- fread("D:/data/nhs_digital/ae/FILE0119465_NIC284866_HES_AE_201999.txt",
              colClasses = "character",
              na.strings = "")
  

## Change all col names to upper case and take col names
  
  ecds_colnames <- colnames(setnames(ecds, colnames(ecds), toupper(colnames(ecds))))
  ae_colnames <- colnames(setnames(ae, colnames(ae), toupper(colnames(ae))))
  
  
## Separate full duplicate
  
  ecds[, duplicated_record := duplicated(ecds)]
  ae[, duplicated_record := duplicated(ae)]
  
  ecds_data_duplicates <- ecds[duplicated_record == TRUE]
  ecds_no_duplicates <- ecds[duplicated_record == FALSE]
  ae_data_duplicates <- ae[duplicated_record == TRUE]
  ae_no_duplicates <- ae[duplicated_record == FALSE]
  
  
  ecds_number_of_full_dups <- ecds[, .N] - ecds_no_duplicates[ ,.N]
  ecds_percentage_full_dups <- (ecds_number_of_full_dups/ecds[, .N])*100L
  ae_number_of_full_dups <- ae[, .N] - ae_no_duplicates[ ,.N]
  ae_percentage_full_dups <- (ae_number_of_full_dups/ae[, .N])*100L
  
  rm(ecds, ae)
  
  
## Change col types as needed
  
  ecds_no_duplicates[, ARRIVAL_DATE := fn_covertStrToDate(ARRIVAL_DATE)]
  ae_no_duplicates[, ARRIVALDATE := fn_covertStrToDate(ARRIVALDATE)]
  
  ecds_no_duplicates[, AGE_AT_ARRIVAL := as.integer(AGE_AT_ARRIVAL)]
  ae_no_duplicates[, ACTIVAGE := as.integer(ACTIVAGE)]
  
  
## Floor dates for plotting
  
  ecds_no_duplicates[, incident_month := floor_date(ARRIVAL_DATE, unit = "month")]
  ecds_no_duplicates[, incident_week := floor_date(ARRIVAL_DATE, unit = "week")]
  ae_no_duplicates[, incident_month := floor_date(ARRIVALDATE, unit = "month")]
  ae_no_duplicates[, incident_week := floor_date(ARRIVALDATE, unit = "week")]
  

## Create age groups
  
  ecds_no_duplicates[, age_group := cut(AGE_AT_ARRIVAL, c(seq(0, 120, 10), Inf), include.lowest = TRUE)]  
  ae_no_duplicates[, age_group := cut(ACTIVAGE, c(seq(0, 120, 10), Inf), include.lowest = TRUE)]


## Use Data Explorer Package on dataset      
  
  DataExplorer::create_report(ecds_no_duplicates,
                              output_format = html_document(),
                              output_file = "ECDS data explorer report.html",
                              output_dir = "RMarkdown",
                              config = configure_report(add_plot_prcomp = FALSE))
  
  DataExplorer::create_report(ae_no_duplicates,
                              output_format = html_document(),
                              output_file = "AE data explorer report.html",
                              output_dir = "RMarkdown",
                              config = configure_report(add_plot_prcomp = FALSE))
  
  
## Create a few graphs of records over time

## ECDS
  
  ecds_records_per_month_plot <- ggplot(ecds_no_duplicates[, .N, by = incident_month],
                                   aes(x = incident_month, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("ECDS episodes per month")
  
  ecds_records_per_week_plot <- ggplot(ecds_no_duplicates[, .N, by = incident_week],
                                  aes(x = incident_week, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("ECDS episodes per week")
  
  ecds_age_plot <- ggplot(ecds_no_duplicates[, .N, by = age_group],
                     aes(x = age_group, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("ECDS episodes per age group")
  
  ecds_age_month_plot <- ggplot(ecds_no_duplicates[, .N, by = .(incident_month, age_group)],
                           aes(x = incident_month, y = N, color = age_group)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("ECDS episodes by age group per month")
  
## AE
  
  ae_records_per_month_plot <- ggplot(ae_no_duplicates[, .N, by = incident_month],
                                   aes(x = incident_month, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("AE episodes per month")
  
  ae_records_per_week_plot <- ggplot(ae_no_duplicates[, .N, by = incident_week],
                                  aes(x = incident_week, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("AE episodes per week")
  
  ae_age_plot <- ggplot(ae_no_duplicates[, .N, by = age_group],
                     aes(x = age_group, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("AE episodes per age group")
  
  ae_age_month_plot <- ggplot(ae_no_duplicates[, .N, by = .(incident_month, age_group)],
                           aes(x = incident_month, y = N, color = age_group)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("AE episodes by age group per month")
  
  