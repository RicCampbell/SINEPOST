## Script that reads in YAS ePR data and creates majority of meta-data file for fields
## Utilities previous meta data to ensure consistency across projects
## Extra destination fields are added outside of R prior to files next use

library(data.table)
library(readxl)


# Read in data ------------------------------------------------------------

  yas_data <- data.table(readRDS("D:/data/yas/sinepost.rds"))
  drug_fields <- data.table(readRDS("D:/data/yas/missing-drugs-final.rds"))


## Read in Jamies selection of variables that will be used

  variable_selection_table <- data.table(read_excel("D:/reference_data/jamie_variable_selection.xlsx",
                                                    sheet = "Sheet1",
                                                    col_names = TRUE,
                                                    col_types = "text",
                                                    trim_ws = TRUE))
  
## Read in previous meta-data (from another study)
  
  previous_meta_data <- data.table(read_excel("D:/reference_data/field_mapping_and_standardisation_priest.xlsx",
                                              sheet = "ePR",
                                              col_names = TRUE,
                                              col_types = "text",
                                              trim_ws = TRUE))

  

# Col name checks and meta data creation ---------------------------------------------------------

## Ensure col names of both tables are acceptable col names

  setnames(yas_data, make.names(tolower(colnames(yas_data)), unique = TRUE))
  setnames(variable_selection_table, make.names(tolower(colnames(variable_selection_table)), unique = TRUE))
  setnames(drug_fields, make.names(tolower(colnames(drug_fields)), unique = TRUE))

  variable_selection_table[, variable.name := tolower(variable.name)]
  

## Create table with col names and type of col
  
  yas_col_names_info <- data.table(source_field = colnames(yas_data),
                                   field_type = sapply(yas_data, typeof),
                                   field_class = sapply(yas_data, class))
  
  
## Add field for if col has been marked as one of interest to study
  
  yas_col_names_info[, wanted_col := source_field %chin% variable_selection_table[included == "1", variable.name]]
  

## Add in extra drug field
  
  drug_fields_col_names <- colnames(drug_fields)[!(colnames(drug_fields) %chin% "unique_epr_id")]
  
  yas_col_names_info <- rbind(yas_col_names_info,
                              data.table(source_field = colnames(drug_fields[, ..drug_fields_col_names]),
                                         field_type = sapply(drug_fields[, ..drug_fields_col_names], typeof),
                                         field_class = sapply(drug_fields[, ..drug_fields_col_names], class),
                                         wanted_col = TRUE))
  
  
## Merge table to add cols of other mark-up, and anything of interest from previous study to save time
  
  yas_col_names_info <- merge(yas_col_names_info,
                              variable_selection_table[, .(variable.name, jamie_type = type, notes)],
                              by.x = "source_field",
                              by.y = "variable.name",
                              all.x = TRUE)
  
  yas_col_names_info <- merge(yas_col_names_info,
                              previous_meta_data[, .(source_field, destination_field, field_group)],
                              by = "source_field",
                              all.x = TRUE)

  
# Save data ---------------------------------------------------------------

  
## Order then write out table to csv
## Missing 'destination fields' will be done manually
  
  setcolorder(yas_col_names_info, c("source_field", "field_type", "field_class", "wanted_col", "destination_field", "field_group", "jamie_type", "notes"))
  
  fwrite(yas_col_names_info, "D:/reference_data/yas_meta_data_sinepost.csv")
                               

