library(data.table)
library(googlesheets4)
library(usethis)
library(readxl)
source("R/cleaning_fns_etl.r")


# Download and read in ONS file for postcode to LSOA11, RUC11, OAC --------

# ONSPD Aug 2020

  temp <- tempfile(fileext = ".zip")
  download.file("https://www.arcgis.com/sharing/rest/content/items/a644dd04d18f4592b7d36705f93270d8/data", temp, mode = "wb")
  
  temp_location <- "D:/reference_data/temp/onspd_2020-08"
  files <- unzip(temp, exdir = temp_location)
  pc_to_oa11_classes <- fread(paste0(temp_location, "/Data/ONSPD_AUG_2020_UK.csv"))

  
# retain only fields of interest
  
  pc_to_oa11_classes_fields_to_retain <- c("pcds", "usertype", "lsoa11", "ru11ind", "oac11", "imd", "lat", "long")
  pc_to_oa11_classes[, colnames(pc_to_oa11_classes)[!(colnames(pc_to_oa11_classes) %in% pc_to_oa11_classes_fields_to_retain)] := NULL]
  
  pc_to_oa11_classes_char_cols <- colnames(pc_to_oa11_classes)[sapply(pc_to_oa11_classes, typeof) == "character"]
  pc_to_oa11_classes[, (pc_to_oa11_classes_char_cols) := lapply(.SD, fn_removeBlanks), .SDcols = pc_to_oa11_classes_char_cols]

  
# Retain Output Area Classifications for England/Wales/Scotland/NI only
  
  pc_to_oa11_classes[!(substr(lsoa11, 1, 1) %in% c("E", "W", "S", "9")), ru11ind := NA]

# Retain Rural-Urban classifications for England/Wales only - other home countries use other measures
  
  pc_to_oa11_classes[!(substr(lsoa11, 1, 1) %in% c("E", "W")), ru11ind := NA]

# Retain IoD/IMD for England only - other home countries use other measures
  
  pc_to_oa11_classes[substr(lsoa11, 1, 1) != "E", imd := NA]

# It is necessary to calculate quantiles using Type 4 to match the UK Gov published deciles.
  
  pc_to_oa11_classes[, iod19_decile := cut(imd,
                                           breaks = quantile(pc_to_oa11_classes[!is.na(imd), .N, by = .(lsoa11, imd)][, imd], probs = 0:10/10, type = 4),
                                           labels = FALSE,
                                           include.lowest = TRUE)]
  
  pc_to_oa11_classes[, imd := NULL]

## Remove temp file and link to unzipped file

  unlink(c(temp, files))

# Check only one entry per postcode
  stopifnot(pc_to_oa11_classes[, .N, by = pcds][N > 1, .N] == 0)
  stopifnot(pc_to_oa11_classes[is.na(pcds), .N] == 0)

# Check postcode format is variable length, one space between outward and inward parts)
  stopifnot(pc_to_oa11_classes[, max(nchar(pcds), na.rm = TRUE)] == 8 & pc_to_oa11_classes[, min(nchar(pcds), na.rm = TRUE)] == 6 &
              pc_to_oa11_classes[substr(pcds, nchar(pcds) - 3, nchar(pcds) - 3) == " ", .N] == nrow(pc_to_oa11_classes))
  
  save(pc_to_oa11_classes, file = "D:/reference_data/pc_to_oa11_classes.rda")

  rm(pc_to_oa11_classes)


# Download Trust reference data -------------------------------------------

## Trust data
  
  temp <- tempfile(fileext = ".zip")
  download.file("https://files.digital.nhs.uk/assets/ods/current/etr.zip", temp, mode = "wb")
  
  temp_location <- "D:/reference_data/temp/etr_2021_05"
  files <- unzip(temp, exdir = temp_location)
  trust_data <- fread(paste0(temp_location, "/etr.csv"))

  setnames(trust_data, c("org_code", "name", "national_group", "high_level_health_geog", "address_1", "address_2", "address_3", "address_4", "address_5",
                         "postcode", "open_date", "close_date", "null_1", "null_2", "null_3", "null_4", "null_5", "telephone_number", "null_6", "null_7", "null_8",
                         "amended_record", "null_9", "gor_code", "null_10", "null_11", "null_12"))
  
  
## Check postcode is correct format, one and only one space
  
  stopifnot(trust_data[, max(nchar(postcode), na.rm = TRUE)] == 8 & trust_data[, min(nchar(postcode), na.rm = TRUE)] == 6 &
              trust_data[substr(postcode, nchar(postcode) - 3, nchar(postcode) - 3) == " ", .N] == trust_data[!is.na(postcode), .N])
  
  saveRDS(trust_data, file = "D:/reference_data/trust_data.rds")
  
  
## Remove temp file and link to unzipped file and tidy up environment
  
  unlink(c(temp, files))
  rm(trust_data)
  
  
## Site data
  
  temp <- tempfile(fileext = ".zip")
  download.file("https://files.digital.nhs.uk/assets/ods/current/ets.zip", temp, mode = "wb")
  
  temp_location <- "D:/reference_data/temp/ets_2021_05"
  files <- unzip(temp, exdir = temp_location)
  site_data <- fread(paste0(temp_location, "/ets.csv"))
  
  setnames(site_data, c("org_code", "name", "national_group", "high_level_health_geog", "address_1", "address_2", "address_3", "address_4", "address_5",
           "postcode", "open_date", "close_date", "null_1", "org_sub_type", "parent_org_code", "null_2", "null_3", "telephone_number", "null_4",
           "null_5", "null_6", "amended_record", "null_7", "gor_code", "null_8", "null_9", "null_10"))
  
  ## Check postcode is correct format, one and only one space
  
  stopifnot(site_data[, max(nchar(postcode), na.rm = TRUE)] == 8 & site_data[, min(nchar(postcode), na.rm = TRUE)] == 6 &
              site_data[substr(postcode, nchar(postcode) - 3, nchar(postcode) - 3) == " ", .N] == site_data[!is.na(postcode), .N])
  
  saveRDS(site_data, file = "D:/reference_data/site_data.rds")
  

## Remove temp file and link to unzipped file and tidy up environment
  
  unlink(c(temp, files))
  rm(site_data)

  

# Download SNOMED CT from TRUD (V31.3.0, 2021-01-27, ~930MB) -----------

  trud_address <- "https://isd.digital.nhs.uk/trud3/api/v1/keys/4149673aafc1a67390597b8f681b4d6a3660968b/files/SNOMEDCT2/32.2.0/UK_SCT2CL/uk_sct2cl_32.2.0_20210707000001Z.zip"
  
  
  temp <- tempfile(fileext = ".zip")
  download.file(trud_address, temp, mode = "wb")
  files <- unzip(temp, exdir = "D:/reference_data/temp/snomed")
  
  snomed_uk_desc <- fread("D:/reference_data/temp/snomed/SnomedCT_UKClinicalRF2_PRODUCTION_20210707T000001Z/Full/Terminology/sct2_Description_UKCLFull-en_GB1000000_20210707.txt",
                          colClasses = "character")
  snomed_int_desc <- fread("D:/reference_data/temp/snomed/SnomedCT_InternationalRF2_PRODUCTION_20210131T120000Z/Full/Terminology/sct2_Description_Full-en_INT_20210131.txt",
                           colClasses = "character")
  
  setnames(snomed_uk_desc, make.names(tolower(colnames(snomed_uk_desc)), unique = TRUE))
  setnames(snomed_int_desc, make.names(tolower(colnames(snomed_int_desc)), unique = TRUE))
  
  snomed_desc_full <- rbind(snomed_uk_desc, snomed_int_desc)
  
  save_time <- getDateTimeForFilename()
  
  write.csv(snomed_desc_full, paste0("D:/reference_data/full_trud_snomed_concepts_", save_time, ".csv"))
  
  
## Remove temp file and link to unzipped file
  
  unlink(c(temp, files))
  

# Download TRUD data for ECDS ---------------------------------------------

  trud_address <- "https://isd.digital.nhs.uk/trud3/api/v1/keys/4149673aafc1a67390597b8f681b4d6a3660968b/files/UKHRS/32.2.0/UKHRS_UKCL/UKHRS_UKCL_32.2.0_20210707000001Z.zip"
  
  
  temp <- tempfile(fileext = ".zip")
  download.file(trud_address, temp, mode = "wb")
  files <- unzip(temp, exdir = "D:/reference_data/temp/snomed_ecds")

## Acuity
  
  ecds_acuity_snomed <- data.table(read_excel("D:/reference_data/temp/snomed_ecds/SnomedCT_UKClinical_HumanReadableRefsets_20210707T000001Z/Emergencycareacuity-999003061000000107-20210707.xlsx",
                                              sheet = "999003061000000107",
                                              range = cell_rows(2:7),
                                              col_names = TRUE,
                                              col_types = "text",
                                              trim_ws = TRUE))
  
  setnames(ecds_acuity_snomed, make.names(tolower(colnames(ecds_acuity_snomed)), unique = TRUE))
  
  write.csv(ecds_acuity_snomed, paste0("D:/reference_data/ecds_acuity_snomed.csv"))
 
## Arrival Mode
  
  ecds_arrival_mode <- data.table(read_excel("D:/reference_data/temp/snomed_ecds/SnomedCT_UKClinical_HumanReadableRefsets_20210707T000001Z/Emergencycarearrivalmode-999002981000000107-20210707.xlsx",
                                             sheet = "999002981000000107",
                                             range = cell_rows(2:11),
                                             col_names = TRUE,
                                             col_types = "text",
                                             trim_ws = TRUE))
  
  setnames(ecds_arrival_mode, make.names(tolower(colnames(ecds_arrival_mode)), unique = TRUE))
  
  write.csv(ecds_arrival_mode, paste0("D:/reference_data/ecds_arrival_mode.csv"))
  
  
## Attendance Source
  
  ecds_arrival_source <- data.table(read_excel("D:/reference_data/temp/snomed_ecds/SnomedCT_UKClinical_HumanReadableRefsets_20210707T000001Z/Emergencycareattendancesource-999002991000000109-20210707.xlsx",
                                             sheet = "999002991000000109",
                                             range = cell_rows(2:35),
                                             col_names = TRUE,
                                             col_types = "text",
                                             trim_ws = TRUE))
  
  setnames(ecds_arrival_source, make.names(tolower(colnames(ecds_arrival_source)), unique = TRUE))
  
  write.csv(ecds_arrival_source, paste0("D:/reference_data/ecds_arrival_source.csv"))
  
  
  unlink(c(temp, files))
  rm(ecds_acuity_snomed, ecds_arrival_mode, ecds_arrival_source)
