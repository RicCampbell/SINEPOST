## Non-urgent attenders code


## CHECK THIS AGAINST PUBLISHED PAPER
## - DO WE CARE ABOUT AEDEPTTYPE OR ATTENDCAT??

# Definition from doi: doi.org/10.1371/journal.pone.0192855

## "..who attended a type 1 ED"
# AEDEPTTYPE values:
# 01 - Emergency department

## "..all follow up attendances, whether planned or unplanned were considered urgent."
# AEATTENDCAT values:
# 1 - First A&E attendance
# 2 - Follow-up A&E attendance, planned
# 3 - Follow-up A&E attendance, unplanned
# 9 - Not known

# 1. Not investigated in ED (except by urinalysis, pregnancy test or dental investigation)
# INVEST2_N values:
# 06 - Urinalysis
# 21 - Pregnancy test
# 22 - Dental investigation
# 24 - None

#2. Not treated in ED (except by prescription, recording vital signs, dental treatment or guidance/advice)
# TREAT2_N values:
# 07 - [No value]
# 22 - Guidance/advice only
# 30 - Recording vital signs
# 56 - Dental treatment
# 57 - Prescription/medicines prepared to takeaway
# 99 - None (consider guidance/advice option)

# 3. Discharged completely from care in ED or referred to their GP
# AEATTENDDISP values:
# 01 - Admitted to hospital bed
# 07 - Transferred to healthcare provider
# 02 - Discharged, GP follow-up
# 03 - Discharged, no follow-up
# 12 - Left department before being treated


calcLowAcuity <- function(dt) {
  
## Make copy of data and keep only records that '..attended a type 1 ED'
  
  # dt <- copy(ae_data)
  
  
  dt_long <- melt(copy(dt[AEDEPTTYPE == "01"]), 
                  id.vars = c("AEKEY", "AEATTENDDISP", "AEATTENDCAT"),
                  measure.vars = patterns(treat = "TREAT2_", invest = "INVEST2_"),
                  variable.name = "position", 
                  na.rm = FALSE,
                  variable.factor = FALSE)
  
  la_outcomes <- dt_long[, .(low_acuity_invest = all(invest %chin% c("06", "21", "22", "24", NA)),
                             valid_invests = sum(!is.na(invest)),
                             low_acuity_treat = all(treat %chin% c("07", "22", "30", "56", "57", "99", NA)),
                             valid_treats = sum(!is.na(treat))), by = .(AEKEY, AEATTENDDISP, AEATTENDCAT)]
  

  la_outcomes[valid_invests == 0L, low_acuity_invest := NA]
  la_outcomes[valid_treats == 0L, low_acuity_treat := NA]
  
  la_outcomes[!low_acuity_invest | 
                !low_acuity_treat | 
                AEATTENDDISP %chin% c("01", "07"), 
              low_acuity_attendance := FALSE]
  
  la_outcomes[(low_acuity_invest & 
                 low_acuity_treat & 
                 AEATTENDDISP %chin% c("02", "03")) | 
                AEATTENDDISP == "12", 
              low_acuity_attendance := TRUE]
  
## Factor in that '..all follow up attendances were considered urgent
  
  la_outcomes[AEATTENDCAT == 2 | AEATTENDCAT == 3, low_acuity_attendance := FALSE]
  
  la_outcomes[AEATTENDCAT == 9 & low_acuity_attendance == TRUE, low_acuity_attendance := NA]
  
  return(la_outcomes[, .(AEKEY, valid_invests, valid_treats, low_acuity_invest, low_acuity_treat, low_acuity_attendance)])
}