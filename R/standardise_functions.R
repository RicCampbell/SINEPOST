library(data.table)
library(lubridate)


## function for finding the number of non NA fields for each col in a table

nonNaRecordsTable <- function(table, over_id_col = NA) {
  stopifnot(length(over_id_col) == 1)
  stopifnot(is.na(over_id_col) | is.character(over_id_col))
  stopifnot(is.na(over_id_col) | over_id_col %in% colnames(table))

  if(!is.na(over_id_col)) {
    record_count <- table[, .N, by = eval(over_id_col)][, .N]
  } else {
    record_count <- table[, .N]
  }

  cols <- colnames(table)[!(colnames(table) %in% over_id_col)]

  if(!is.na(over_id_col)) {
    non_na_table_patient <- table[, lapply(.SD, function (y) any(!is.na(y))), by = eval(over_id_col), .SDcols = cols]
    non_na_table_count <- non_na_table_patient[, lapply(.SD, function(z) sum(z) / record_count * 100), .SDcols = cols]
  } else {
    non_na_table_count <- table[, lapply(.SD, function(z) sum(!is.na(z)) / record_count * 100), .SDcols = cols]
  }

  return(melt(non_na_table_count,
              measure.vars = colnames(non_na_table_count),
              variable.name = "field",
              variable.factor = FALSE,
              value.name = "non_missing_pc"))
}

## function that returns all date col names in table

getPOSIXtFields <- function(table) {
  return(colnames(table)[sapply(table, is.POSIXt)])
}


## function for checking min and max of a date col

datesWithinRange <- function(table, date_cols, min_date, max_date) {

  return(table[, .(out_of_range = Reduce(`+`, lapply(.SD, function(x) {
    return(as.Date(max(x, na.rm = TRUE), "%Y-%m-%d", tz = "Europe/London") > max_date |
      as.Date(min(x, na.rm = TRUE), "%Y-%m-%d", tz = "Europe/London") < min_date)
    }))), .SDcols = date_cols][1, out_of_range] == 0)
}



## Function for checking postcodes of sites that are being used (actually works on ods code but don't have that so working backwards as not many hospitals)

getODSDetails <- function(ods_code) {
  
  print(ods_code)
  
  url <- paste0("https://directory.spineservices.nhs.uk/STU3/Organization/", ods_code)
  
  doc <- httr::GET(url)
  
  if(httr::status_code(doc) != 200) {
    return(data.frame(ods_code,
                      name = NA,
                      org_type = NA,
                      status_active = NA,
                      postcode = NA,
                      stringsAsFactors = FALSE))
  }
  
  payload <- httr::content(doc)
  
  
  primary_role <- sapply(payload$extension, function(object) {
    if(object$url == "https://fhir.nhs.uk/STU3/StructureDefinition/Extension-ODSAPI-OrganizationRole-1") {
      if(object$extension[[2]]$url == "primaryRole" & object$extension[[2]]$valueBoolean) {
        TRUE
      } else {
        FALSE
      }
    } else {
      FALSE
    }
  })
  
  payload$extension[[which(primary_role)]]$extension[[1]]$valueCoding$display
  
  data.frame(ods_code,
             name = payload$name,
             org_type = payload$extension[[which(primary_role)]]$extension[[1]]$valueCoding$display,
             status_active = payload$active,
             postcode = payload$address$postalCode,
             stringsAsFactors = FALSE)
}


## Function for replacing snomed code field with readable label - reduces all conceptids to one row each with no clever selection

getSnomedLabel <- function(field_contents, dt, snomed_code_term_table){
  
  # field_contents <- ecds_data$ACUITY
  # dt <- ecds_data
  # snomed_code_term_table <- snomed_codes
    
    dt_copy <- copy(dt)
    
    
  ## Get list of all snomed codes in field, removing NA, and the matched concept ids in full snomed
    
    codes_present <- unique(field_contents)
    
    codes_present <- codes_present[!is.na(codes_present)]
    
    stopifnot(length(codes_present) != 0)
    
    relavent_snomed_codes <- snomed_code_term_table[conceptid %chin% codes_present]
    
    
  ## Reduce to one line per concept id - want to keep the fully specified name, fsn, for each one, held in typeid
    
    relavent_snomed_codes <- relavent_snomed_codes[typeid == "900000000000003001"]
                                                   
  ## May still be more than one per conceptid, in this case, order and take first by being; active, effective time, and perfered case significance
  
    setorder(relavent_snomed_codes, conceptid, -active, -effectivetime, casesignificanceid, na.last = TRUE)
  
    relavent_snomed_codes <- relavent_snomed_codes[, order := 1:.N, by = conceptid][order == 1][, order := NULL]
    
    
  ## Length and number of records not always going to be the same, dataset might not have instances of all options
    
    stopifnot(length(codes_present) <= relavent_snomed_codes[, .N])
    stopifnot(setdiff(codes_present, relavent_snomed_codes$conceptid) == 0)
    
    field_lable_values <- relavent_snomed_codes$term[match(field_contents, relavent_snomed_codes$conceptid)]
    
    return(field_lable_values)
}


