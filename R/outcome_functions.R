## Non-urgent attenders code


## CHECK THIS AGAINST PUBLISHED PAPER
## - DO WE CARE ABOUT AEDEPTTYPE OR ATTENDCAT??

# Definition from doi: doi.org/10.1371/journal.pone.0192855

## "..who attended a type 1 ED"
# AEDEPTTYPE values:
# Is this 1-3, 4 is walk-in centre, 99 is not known

## "..all follow up attendances, whether planned or unplanned were considered urgent."
# ATTENDCAT values:
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


calcLowAcuity <- function(dt, report = "outcome") {
  
  dt_long <- melt(copy(dt), 
                  id.vars = c("AEKEY", "AEATTENDDISP"),
                  measure.vars = patterns(treat = "TREAT2_", invest = "INVEST2_"),
                  variable.name = "position", 
                  na.rm = FALSE,
                  variable.factor = FALSE)
  
  la_outcomes <- dt_long[, .(low_acuity_invest = all(invest %chin% c("06", "21", "22", "24"), na.rm = TRUE),
                             valid_invests = sum(!is.na(invest)),
                             low_acuity_treat = all(treat %chin% c("07", "22", "30", "56", "57", "99"), na.rm = TRUE),
                             valid_treats = sum(!is.na(treat))), by = .(AEKEY, AEATTENDDISP)]
  
  if(report == "completeness") {
    return(la_outcomes[, .(AEKEY, valid_invests, valid_treats)])
  }
  
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
  
  if(report == "outcome") {
    return(la_outcomes[, .(AEKEY, low_acuity_attendance)])
  }
  
  return(la_outcomes[, .(AEKEY, valid_invests, valid_treats, low_acuity_invest, low_acuity_treat, low_acuity_attendance)])
}