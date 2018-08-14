# pre_avl_data_linkage_3_timetable_matching.R
# 
# @Description
# 
# The objective in this script is to match each transaction to a plausible
# timetable identifier. The availability of such an identifier will provide a 
# usefule means by which an individuals possible alighting locations may be 
# determined. 

# Import required libraries ----------------------------------------------------
require(RPostgreSQL)
require(dplyr)
require(lubridate)
require(glue)
library(stringr)

# Establish Database Connection ------------------------------------------------
con 
# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")


# Set parameters for data processing -------------------------------------------

# The datas object contains a vector of all data to be processed. 
#dates <- seq.Date(as.Date("2015-04-05"), as.Date("2015-04-05"), by = "day")
# The tt_version determines which timetable is used for the extraction of
# a plausible timetable id.

# 2015-Jul
#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); 

# 2015-Oct
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  

# 2016-Jan
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); 

# 2016-Apr - Note square brackets to omit days where AVL data were unavailable.
#dates <-  seq.Date(as.Date("2016-04-11"), 
#as.Date("2016-07-17"), by = "day")[c(1:74,79:98)]; 

# 2016-Jul
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); 


dates <- seq.Date(as.Date("2015-10-12"), as.Date("2015-10-18"), by = "day"); 

tt_version <- "2015_oct"


for (i in 1:length(dates)) {
  
  # date parameter based on loop iteration.
  date_p <- dates[i]
  print(paste0("starting process for ", date_p, " at ", Sys.time()))
  
  
  case = db_has_table(con, paste0("pre_avl_transactions_",
                                  gsub(pattern = "-", "_", date_p), "_rep"))
  
  if (case == TRUE) {
    print("skipping")
    next()
  }
  # Import transactions --------------------------------------------------------
  
  afc_transactions <-
    dbGetQuery(
      con, paste0("
        select t1.record_id, t2.account, t1.isam_op_code, t1.etm_service_number, 
        t1.start_stage, t3.direction, t1.transaction_datetime 
        from afc t1 
        left join cch t2 using (card_isrn) 
        left join afc_direction_of_travel t3
        using (record_id) 
        where t1.isam_op_code like '??%' and t2.account is not null and
        t1.transaction_datetime 
        between '",date_p," 00:00:00' and '",date_p," 23:59:59' and
        type = 'Over 60 Concession'
        "
      )
    )
  
  
  # Combine transactions with tt_ids. ------------------------------------------
  
  loop.result <- dbGetQuery(con, 
                            paste0("select * from pre_avl_afc_boarding.
       pre_avl_boardings_", gsub("-", "_", date_p)))
  
  # It is necessary to convert the record_id column to an integer for subsequent
  # join to original afc_transactions dataset.
  loop.result$record_id <- as.integer(loop.result$record_id)
  
  # Update the afc transactions data with the loop.results dataset.
  afc_transactions <- afc_transactions %>% 
    inner_join(loop.result, by = c("record_id"))
  
  afc_transactions$merge_case <- NA
  afc_transactions$id <- NA
  afc_transactions$n <- NA
  afc_transactions$tt_direction <- NA
  
  ##### Update with lookup table between isam operator, tt operator and at operator.
  
  
  afc_transactions <-  afc_transactions %>% 
    mutate(operator = ifelse(str_sub(isam_op_code,3,4) == '??', '????', '????'))
  
  
  # day of week allows us to target specific portions of the timetable such that
  # our choice of tt_id is as close to the true value as is realistly possible.
  

  
  if (strftime(date_p, format = "%w") == 0) {
    dow = 7 
  } else { 
    dow = strftime(date_p, format = "%w")
  } 
  
  
  
  # Interate through transactions to identify tt_id ----------------------------
  
  #ystem.time({

    for (j in 1:nrow(afc_transactions)) {
    if(j %% 1000 == 0) print(round(j/nrow(afc_transactions),2)*100)
      mergeData <-
      dbGetQuery(
        con,
        paste0(
          "select id, n, direction from timetables.tt_",tt_version," where
        route = '",
          afc_transactions$etm_service_number[j],
          "'
        and direction = '",
          afc_transactions$direction[j],
          "' and
        naptan_code = '",
          afc_transactions$naptan_code[j],
          "' and
        operator = '",
          afc_transactions$operator[j],
          "' and
        substring(dow,",dow,",1) = '1'
        and arrive between '", 
          format(afc_transactions$transaction_datetime[j] - 7200,
                 "%Y-%m-%d %H:%M:%S"),"'::time and
        '",format(afc_transactions$transaction_datetime[j] + 7200,
                  "%Y-%m-%d %H:%M:%S"),"'::time
        order by n desc limit 1"
        )
      )
    
    if (mergeData %>% nrow() > 0) {
      afc_transactions$merge_case[j] <- "1"
      afc_transactions$id[j] <- mergeData$id[1]
      afc_transactions$n[j] <- mergeData$n[1]
      afc_transactions$tt_direction[j] <- mergeData$direction[1]
    } else {
    
      mergeData <-
        dbGetQuery(
          con,
          paste0(
            "select 2 as merge_case, id, n, direction from timetables.tt_",
            tt_version," 
            where 
            operator = '",
            afc_transactions$operator[j],
            "' and
            naptan_code = '",
            afc_transactions$naptan_code[j],
            "' and
            route = '",
            afc_transactions$etm_service_number[j],
            "' and arrive between '", 
            format(afc_transactions$transaction_datetime[j] - 7200,
            "%Y-%m-%d %H:%M:%S"),"'::time and
            '",format(afc_transactions$transaction_datetime[j] + 7200,
            "%Y-%m-%d %H:%M:%S"),"'::time and
            substring(dow,",dow,",1) = '1'
            order by n desc limit 1"
          )
        )
      
      if (mergeData %>% nrow() > 0) {
        afc_transactions$merge_case[j] <- "2"
        afc_transactions$id[j] <- mergeData$id[1]
        afc_transactions$n[j] <- mergeData$n[1]
        afc_transactions$tt_direction[j] <- mergeData$direction[1]
      }
      
    }
    rm(mergeData)
  }

  
  # Output results to database and index ---------------------------------------
  
  dbWriteTable(con,
               name = c("pre_avl_afc_transactions", 
                        paste0("pre_avl_transactions_",
                               gsub(pattern = "-", "_", date_p))),
               afc_transactions,
               row.names = F)
  
  dbGetQuery(
    con,
    paste0("create index on pre_avl_afc_transactions.pre_avl_transactions_",
           gsub(pattern = "-", "_", date_p), " (account)")
  )
  dbGetQuery(
    con,
    paste0("create index on pre_avl_afc_transactions.pre_avl_transactions_",
           gsub(pattern = "-", "_", date_p), " (record_id)")
  )
  
}






