## Script for creating lookup values and working on AE data

library(data.table)
library(igraph)

source("R/cleaning_fns_etl.r")
source("R/outcome_functions.R")



# Read in HES data --------------------------------------------------------

  ae_data <- fread("D:/data/nhs_digital/ae/FILE0119465_NIC284866_HES_AE_201999.txt",
                   colClasses = "character")
  
  setnames(ae_data, make.names(toupper(colnames(ae_data)), unique = TRUE))
  ae_col_names <- colnames(ae_data)
  ae_data[, (ae_col_names) := lapply(.SD, fn_removeBlanks), .SDcols = ae_col_names]


# Remove all empty cols

  empty_cols <- getNamesOfEmptyFields(ae_data)
  ae_data[, (empty_cols) := NULL]


# Convert date fields to dates

  ae_date_cols <- c("ARRIVALDATE")
  ae_data[, (ae_date_cols) := lapply(.SD, fn_covertStrToDate), .SDcols = ae_date_cols]
  

# Convert AE times to ISO times

  ae_data[, ARRIVALTIME := lubridate::fast_strptime(paste(ARRIVALDATE, ARRIVALTIME),
                                                    format = "%Y-%m-%d %H%M",
                                                    tz = "Europe/London",
                                                    lt = FALSE)]
  
## Convert fields to integers

  ae_data[, ':=' (ACTIVAGE = as.integer(ACTIVAGE),
                  ARRIVALAGE = as.integer(ARRIVALAGE))]  


## Check that all records have a HES_ID and study_id and AEKEY

  stopifnot(ae_data[is.na(ENCRYPTED_HESID) | is.na(STUDY_ID) | is.na(AEKEY), .N] == 0)



# Resolve issue with multiple HESIDs per AEKEY ----------------------------

## Should only have one ENCRYPTED_HESID per AEKEY but this is not the case
## NHSD define AEKEY as a unique record identifier, it can't belong to 2 different patients

  ae_duplicate_aekeys <- ae_data[, .N, by = .(ENCRYPTED_HESID, AEKEY)][, .N, by = AEKEY][N > 1, AEKEY]
# length(ae_duplicate_aekeys)


## For these records we confirm that, aside from STUDY_ID and HESID, they are identical for the same AEKEY
  
  duplicated_fields <- colnames(ae_data)[!(colnames(ae_data) %in% c("ENCRYPTED_HESID", "STUDY_ID"))]
  ae_data[AEKEY %chin% ae_duplicate_aekeys, duplicates := duplicated(ae_data[AEKEY %chin% ae_duplicate_aekeys], by = duplicated_fields)]


  stopifnot(ae_data[AEKEY %chin% ae_duplicate_aekeys, .(distinct_records = sum(duplicates == FALSE), duplicated_records = sum(duplicates)), by = AEKEY][distinct_records != 1, .N] == 0)
  ae_data[, duplicates := NULL]


## Check that each HESID sharing an AEKEY only links to shared AEKEYs
hesid_shares <- ae_data[AEKEY %chin% ae_duplicate_aekeys, .N, by = ENCRYPTED_HESID][, ENCRYPTED_HESID]
stopifnot(identical(ae_data[ENCRYPTED_HESID %chin% hesid_shares, .N, by = AEKEY][order(AEKEY), AEKEY], sort(ae_duplicate_aekeys)))


## Within the set of shared AEKEY records, confirm that the sharing is consistent (HESIDs sharing any AEKEY share all their AEKEYs with the same set of HESIDs)
aekey_hesid_shares_table <- copy(ae_data[AEKEY %chin% ae_duplicate_aekeys, .N, by = .(AEKEY, ENCRYPTED_HESID)])


### Bit of graph representation to find all connected (by any AEKEY) HESIDs

# For each AEKEY do the function and then bind the entire result together and convert to a matrix after unique-ing it
hesid_links <- as.matrix(unique(rbindlist(lapply(unique(aekey_hesid_shares_table$AEKEY), function(aekey, aekey_hesid_shares) {
  # Return every pairwise combination of HESIDs for a AEKEY
  return(data.table(t(combn(aekey_hesid_shares[AEKEY == aekey, ENCRYPTED_HESID], 2))))
}, aekey_hesid_shares = aekey_hesid_shares_table))))


# Convert to a graph
hesid_graph <- graph_from_edgelist(hesid_links, directed = FALSE)


# Find clusters of HESIDs that are connected (since there may be "enchained" connections)
hesid_cluster_graph <- clusters(hesid_graph)


# Output table of connected HESIDs and their respective cluster_id's
hesid_clusters <- data.table(cluster_id = hesid_cluster_graph$membership,
                             ENCRYPTED_HESID = names(hesid_cluster_graph$membership),
                             stringsAsFactors = FALSE)


# Arbitrarily choose the alphabetically "earliest" HESID for each cluster, for later use 
hesid_clusters[, cluster_HESID := ENCRYPTED_HESID[order(ENCRYPTED_HESID) == 1], by = cluster_id]


### For each connected cluster, check each member HESID shares the same AEKEYs.

# Merge the list of AEKEYs for each HESID
aekeys_per_hesid <- aekey_hesid_shares_table[, .(aekey_list = paste(sort(AEKEY), collapse = ";")), by = ENCRYPTED_HESID]
hesid_clusters <- merge(hesid_clusters, 
                        aekeys_per_hesid,
                        by = "ENCRYPTED_HESID",
                        all = TRUE)


# Ensure there is only one distinct list of AEKEYs for each cluster
stopifnot(hesid_clusters[, .N, by = .(aekey_list, cluster_id)][, .N, by = cluster_id][N != 1, .N] == 0)


## Resolve HESIDs so we do not have more than one HESID per AEKEY
ae_data <- merge(ae_data,
                 hesid_clusters[, .(ENCRYPTED_HESID, cluster_HESID)],
                 by = "ENCRYPTED_HESID",
                 all.x = TRUE)

ae_data[!is.na(cluster_HESID), ENCRYPTED_HESID := cluster_HESID]
ae_data[, cluster_HESID:= NULL]



# Create STUDY_ID to HESID look-up ----------------------------------------

## REQUIRE 1 HES_ID per study_id

stopifnot(ae_data[, .N, by = .(STUDY_ID, ENCRYPTED_HESID)][, .N, by = STUDY_ID][N > 1, .N] == 0)


## Create lookup from study_id to HES_ID

study_id_encrypted_hesid_lookup <- unique(ae_data[, .(STUDY_ID, ENCRYPTED_HESID)])

save_time <- getDateTimeForFilename()
saveRDS(study_id_encrypted_hesid_lookup, paste0("data/linkage/study_id_hesid_lookup_", save_time, ".rds"))



# Ensure we have distinct AE records (based on AEKEY) ---------------------

ae_data[, STUDY_ID := NULL]
ae_data <- unique(ae_data)

stopifnot(ae_data[, .N, by = AEKEY][N > 1, .N] == 0)




# Resolve issue with multiple records per patient-datetime ----------------

ae_duplicated_hesid_arrivaltime_records <- copy(ae_data[, .N, by = .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)])
ae_duplicated_hesid_arrivaltime_records[, N_records := .N, by = .(ENCRYPTED_HESID, ARRIVALTIME)]

ae_duplicated_hesid_arrivaltime_aekeys <- ae_duplicated_hesid_arrivaltime_records[N_records > 1, AEKEY]

## For these records, not all are exact duplicates (other than AEKEY)
duplicated_fields <- colnames(ae_data)[!(colnames(ae_data) %in% c("AEKEY"))]
ae_data[AEKEY %chin% ae_duplicated_hesid_arrivaltime_aekeys, duplicates := duplicated(ae_data[AEKEY %chin% ae_duplicated_hesid_arrivaltime_aekeys], by = duplicated_fields)]

# stopifnot(ae_data[AEKEY %chin% ae_duplicated_hesid_arrivaltime_aekeys, .(distinct_records = sum(duplicates == FALSE), duplicated_records = sum(duplicates)), by = .(ENCRYPTED_HESID, ARRIVALTIME)][distinct_records != 1, .N] == 0)
ae_data[, duplicates := NULL]

## Instead we pick the "most complete" record based on variables required to calculate "low acuity attendances"
la_fields_completeness <- calcLowAcuity(ae_data)

ae_data <- merge(ae_data,
                 la_fields_completeness,
                 by = "AEKEY",
                 all.x = TRUE)

ae_data[, ':=' (la_fields_present = (!is.na(AEATTENDDISP) & !is.na(AEATTENDCAT) & valid_invests > 0 & valid_treats > 0),
                la_completeness = as.numeric(!is.na(AEATTENDDISP)) + valid_invests + valid_treats)]

setorder(ae_data, -la_fields_present, -la_completeness, AEKEY) # Use AEKEY as "tie-breaker"
ae_data[, record_order := 1:.N, by = .(ENCRYPTED_HESID, ARRIVALTIME)]
ae_data <- ae_data[record_order == 1L]

ae_data[, c("la_fields_present", "la_completeness", "valid_invests", "valid_treats", "record_order") := NULL]

stopifnot(ae_data[, .N, by = .(ENCRYPTED_HESID, ARRIVALTIME, AEKEY)][, .N, by = .(ENCRYPTED_HESID, ARRIVALTIME)][N > 1, .N] == 0)

# Save A&E data -----------------------------------------------------------

save_time <- getDateTimeForFilename()

saveRDS(ae_data, file = paste0("data/datasets/ae_processed_", save_time, ".rds"))

