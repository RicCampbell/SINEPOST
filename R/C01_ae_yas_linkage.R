## Script for creating lookup values and working on AE data

library(data.table)
library(ggplot2)
library(lubridate)

source("R/cleaning_fns_etl.r")

## Read in data

ae_data <- readRDS("data/datasets/ae_processed_2021-07-10-001655.rds")
yas_data <- load("data/datasets/epr_tables-2021-07-05 201827.rda")





## Merge in all AE arrival times for each person (HESID)

yas_hes_linkage <- merge(yas_data_hes_id[transported_to_hospital == TRUE & receiving_hospital_department %chin% c("ED", "ED - Self Handover", "ED Self-handover/minors","ED Resus"),
                                         .(ENCRYPTED_HESID, incident_datetime, epr_id)],
                         ae_data[, .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)],
                         by = "ENCRYPTED_HESID",
                         all.x = TRUE)


## Calculate the time between ambulance incident time, and arrival at A&E


yas_hes_linkage[, incident_to_arrival_time := as.numeric(difftime(ARRIVALTIME, incident_datetime, unit = "hours"))]

yas_hes_linkage[, incident_to_arrival_time_clipped := pmax(pmin(incident_to_arrival_time, 24.0), -24.0)]

yas_hes_linkage[, incident_to_arrival_time_absolute := abs(incident_to_arrival_time)]



## Set order to find smallest absolute time between ambulance incident time and A&E arrival time - need an extra field to make sure sort is same everytime

setorder(yas_hes_linkage, incident_to_arrival_time_absolute, na.last = TRUE)

yas_hes_linkage[!is.na(incident_to_arrival_time_absolute), order := 1:.N, by = epr_id]


### Select the smallest absolute time that was within 24 hours, but also remove any cases that were within the first and last day of data

smallest_time_diff_per_amb_inc_24_hour <- yas_hes_linkage[order == 1 &
                                                            incident_datetime >= floor_date(min(incident_datetime)) + days(1) &
                                                            incident_datetime < floor_date(max(incident_datetime))]


### Look at distribution of these times - table would probably be better!

ggplot(smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time_absolute <= 5], aes(x = incident_to_arrival_time_clipped)) +
  geom_histogram(binwidth = 1) +
  ggtitle("Distribution of time between ambulance incident and A&E arrival")


head(smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time_absolute <= 5, .N,
                                            by = (diff_time = round(as.numeric(incident_to_arrival_time), digits = 1))][order(diff_time)],100)


## Take records that are between 0 and 6 hours time duration. and only earliest in that time period *****
## Label last 5 (less than 6) hours of YAS data for this - field that indicates records are in this time-frame

## TODO: add prefixes to field names in each table, and drop/rename common fields (HES_ID) after

yas_ae_data <- merge(merge(epr_single_value_fields_table,
                           smallest_time_diff_per_amb_inc_24_hour[incident_to_arrival_time > 0 & incident_to_arrival_time < 6.0, .(epr_id, AEKEY)],
                           by = "epr_id",
                           all.x = TRUE),
                     ae_data,
                     by = "AEKEY",
                     all.x = TRUE)


## Do stopifnot checks



## Re-attendances in YAS data
## Add comments!!

setorder(yas_ae_data, ENCRYPTED_HESID, incident_datetime, na.last = TRUE)

yas_ae_data[!is.na(ENCRYPTED_HESID),
            duration_since_previous_incident := difftime(incident_datetime, shift(incident_datetime),  tz = "Europe/London", unit = "hours"),
            by = ENCRYPTED_HESID]

yas_ae_data[, previous_ems_attendence_within_24_hours := (duration_since_previous_incident < 24.0)]
yas_ae_data[, duration_since_previous_incident := NULL]
yas_ae_data[incident_datetime < floor_date(min(yas_ae_data$incident_datetime)) + days(1), previous_ems_attendence_within_24_hours := NA]



