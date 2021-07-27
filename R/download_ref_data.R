library(data.table)
library(googlesheets4)
library(usethis)
library(readxl)



# Download and read in ONS file for postcode to LSOA11, RUC11, OAC --------

# ONSPD Aug 2020

  temp <- tempfile(fileext = ".zip")
  download.file("https://www.arcgis.com/sharing/rest/content/items/a644dd04d18f4592b7d36705f93270d8/data", temp, mode = "wb")
  
  temp_location <- "D:/reference_data/temp/onspd_2020-08"
  files <- unzip(temp, exdir = temp_location)
  pc_to_oa11_classes <- fread(paste0(temp_location, "/Data/ONSPD_AUG_2020_UK.csv"))

# retain only fields of interest
pc_to_oa11_classes_fields_to_retain <- c("pcds", "usertype", "lsoa11", "ru11ind", "oac11", "imd")
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

save(pc_to_oa11_classes, file = "data/reference_data/pc_to_oa11_classes.rda")