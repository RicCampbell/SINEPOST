## Script that finds distance between ambulance incident and hospital site
## Created in separate script as can only do so many calls to the hereR pacakage per month

library(data.table)
library(sf)
library(hereR)
library(tictoc)
source("R/cleaning_fns_etl.r")

## For using hereR package, an API key has to be set each session
## Key created for freemium 'here' project 2021-07-29

  set_key("lPG7Ynu7KmHJW2OzpX7Kh0zIT2EOA4W6tdHqG_pfS1I")


## AE linked YAS data, and IMD file for long-lat points for all postcodes

  yas_ae_ecds_data <- readRDS("data/datasets/yas_ae_ecds_linked_2021-08-05-144835.rds")
  
  load("D:/reference_data/pc_to_oa11_classes.rda")


# Create distance to hospital outcome -------------------------------------

## Create table of incident postcodes and receiving hospital postcodes
## Can unique as long as re-merge into yas_ae_ecds data via both postcodes

  incident_hospital_geo_data <- unique(yas_ae_ecds_data[!is.na(epr_postcode) & !is.na(AEKEY), .(epr_postcode, epr_receiving_hospital_postcode)])


## Check that both sets of postcodes are in the correct format (one and only one space between in/outward portions)

  stopifnot(incident_hospital_geo_data[, max(nchar(epr_postcode), na.rm = TRUE)] <= 8 & incident_hospital_geo_data[, min(nchar(epr_postcode), na.rm = TRUE)] >= 6 &
              incident_hospital_geo_data[substr(epr_postcode, nchar(epr_postcode) - 3, nchar(epr_postcode) - 3) == " ", .N] == incident_hospital_geo_data[!is.na(epr_postcode), .N])
  
  stopifnot(incident_hospital_geo_data[, max(nchar(epr_receiving_hospital_postcode), na.rm = TRUE)] <= 8 & incident_hospital_geo_data[, min(nchar(epr_receiving_hospital_postcode), na.rm = TRUE)] >= 6 &
              incident_hospital_geo_data[substr(epr_receiving_hospital_postcode, nchar(epr_receiving_hospital_postcode) - 3, nchar(epr_receiving_hospital_postcode) - 3) == " ", .N] == incident_hospital_geo_data[!is.na(epr_receiving_hospital_postcode), .N])


## Merge in long/lat data from ONS postcode file for incident and then receiving hospital postcodes

  incident_hospital_geo_data <- merge(incident_hospital_geo_data,
                                      pc_to_oa11_classes[, .(pcds, lat, long)],
                                      by.x = "epr_postcode",
                                      by.y = "pcds",
                                      all.x = TRUE)
  
  setnames(incident_hospital_geo_data, c("lat", "long"), c("epr_lat", "epr_long"))
  
  
  incident_hospital_geo_data <- merge(incident_hospital_geo_data,
                                      pc_to_oa11_classes[, .(pcds, lat, long)],
                                      by.x = "epr_receiving_hospital_postcode",
                                      by.y = "pcds",
                                      all.x = TRUE)
  
  setnames(incident_hospital_geo_data, c("long", "lat"), c("receiving_hospital_long", "receiving_hospital_lat"))
  

## Route function works by taking origin and destination tables that both have to be sf object, NOT sfc points inside of a table, and going line by line across tables

## For this have to split origin and destination into separate tables but keeping them in the correct order

  incident_hospital_geo_data[, row_id := 1:.N]
  
  origin <- incident_hospital_geo_data[, .(epr_postcode,epr_lat, epr_long, row_id)]
  destination <- incident_hospital_geo_data[, .(epr_receiving_hospital_postcode, receiving_hospital_lat, receiving_hospital_long, row_id)]


## Create sf object from each lat-long co-ords
  
  origin <- st_as_sf(origin, coords = c("epr_long", "epr_lat"), remove = FALSE)

  destination <- st_as_sf(destination, coords = c("receiving_hospital_long", "receiving_hospital_lat"), remove = FALSE)

  
## Need to set the CRS for each table, ONS uses WGS 84 - EPSH value 4326

  st_crs(origin) <- 4326L
  st_crs(destination) <- 4326L

## Do time test on ~10 and 20 routes and save output
  
  tic(paste(nrow(origin),"routes calculated"))
  
  routes <- route(origin, destination, transport_mode = "car", routing_mode = "short")

  toc(log = TRUE)
  
  
## After running routes twice, once on 10 routes and then on 20 routes, output log of timings and save this. (left in as will give time when run fully)
  
  log.txt <- tic.log(format = TRUE)
  
  lapply(log.txt, write, "data/datasets/routes/results_of_tictoc_of_routes_function.txt", append = TRUE)
  
  tic.clearlog()
  

## Want to save these route outcomes as there is a limit of calls can make to hereR package per month - save origin and destination tables as well

  save_time <- getDateTimeForFilename()
  
  saveRDS(routes, file = paste("data/datasets/routes/routes", save_time, "amb_incident_hospital_site.rds", sep = "_"))
  saveRDS(origin, file = paste("data/datasets/routes/routes", save_time, "origin_sf_objects.rds", sep = "_"))
  saveRDS(destination, file = paste("data/datasets/routes/routes", save_time, "destination_sf_objects.rds", sep = "_"))


## Only keep routes information needed, do not nee to geographical data, and therefore can turn it back into a data table without concern
  
  st_geometry(routes) <- NULL
  
  routes <- data.table(routes)
    
  incident_hospital_routes <- merge(incident_hospital_geo_data[, .(epr_receiving_hospital_postcode, epr_postcode, row_id)],
                                    routes[, .(id, distance, duration_base)],
                                    by.x = "row_id",
                                    by.y = "id",
                                    all.x = TRUE)[, row_id := NULL]
  
  
  saveRDS(incident_hospital_routes, file = "data/datasets/incident_hospital_routes_", save_time, ".rds")
  
  
## Could add in distance between points WITH traffic included from time of incident/arrival on scene
## Would give time between 'assumed' arrival at destination and AE record time?
  
  
  