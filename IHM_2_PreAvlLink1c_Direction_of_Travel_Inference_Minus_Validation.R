# Script to determine the direction of travel of buses based on the analysis of
# consecutive journey stages.


# Import required libraries.
library(tidyr)
library(dplyr)
library(glue)
library(RPostgreSQL)
library(lubridate)

# Function designed to collapse list of stages down to remove side-by-side
# repetition.
collapseList <- function(list) {
  newList <- c()
  newList[1] <- list[1]
  for (i in 2:length(list)) {
    if ((list[i] != newList[length(newList - 1)]) == TRUE) {
      newList[length(newList) + 1] <- list[i]
    } else {
      next()
    }
  }
  return(newList)
}


# Establish a connection with the database.

# Increase database cache size. This effects how the query planner assignes the
# task.
dbGetQuery(con, "set effective_cache_size = '128GB'")

# Specify the date ranges for the data processing to be performed.


#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), 
#by = "day")[1:55] # set 1

#dates <-  seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), 
#by = "day")[1:105] # set 2

#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), 
#by = "day")[1:84] # set 3

#dates <- seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), 
#by = "day")[c(1:74,79:98)] # set 4

#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), 
#by = "day") # set 5


##### First tier loop needs to go through each day of data. #####

#dates <- seq.Date(as.Date("2015-10-06"), as.Date("2015-10-11"), by = "day")

dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");
 dates <- '2015-10-01'

# Loop through each of the dates.
for (n in 1:length(dates)) {
  
  # Set the date for analysis
  date_p <- dates[n]
  
  print(glue("Starting process for {date_p} at {Sys.time()}"))
  
  
  ## To be corrected!!!: use new bussiness day!!!!!!!!
  
  # Import AFC data for target data appended with known direction where the data
  # are available.
  afcOneDay <- 
    dbGetQuery(con, 
               glue("select t1.record_id, t1.isam_op_code,
                    t1.etm_service_number, t1.transaction_datetime, 
                    t1.start_stage as start_stage_hex, t1.sealer_id, 
                    row_number() over(partition by t1.sealer_id,
                    t1.etm_service_number order by t1.sealer_id, 
                    t1.etm_service_number, t1.transaction_datetime, 
                    t1.record_id) 
                    from afc t1 
                    where t1.transaction_datetime 
                    between '{date_p} 00:00:00' and '
                    {date_p} 23:59:59'")) %>% 
    filter(substring(isam_op_code,1,2) == '??')
  
  
  # From raw afc data select unique isam_op_code, etm_service_number and start
  # stage. This information is used to cap the stages on each route.
  
  dow <- wday(ymd(date_p) - 86400)
  
  afcOneweek <- 
    dbGetQuery(con, 
               glue("select isam_op_code, transaction_datetime,
                    extract('dow' from transaction_datetime) as dow, 
                    etm_service_number, start_stage as start_stage_hex from afc 
                    where transaction_datetime between '{
                    as.character(as.Date(date_p) - 10)} 00:00:00'
                    and '{as.character(as.Date(date_p) + 10)} 23:59:59' 
                    and extract('dow' from transaction_datetime) = {dow}")) %>% 
    distinct %>% 
    filter(substring(isam_op_code,1,2) == '??')
  
  # Convert the start_stage hexidecimal column into a decimal value.
  afcOneDay$start_stage <- as.numeric(paste0("0x", afcOneDay$start_stage_hex))
  afcOneweek$start_stage <- as.numeric(paste0("0x", afcOneweek$start_stage_hex))
  
  # Create an empty id column on the afc data. We will use this to split up all
  # transactions based on unique sealer ids (part ISAM code)
  # and the servive number being operator.
  afcOneDay$id = NA
  
  # PROGRESS: using progress bar slows progress. Uncomment if required.
  # create a progress bar object for the subsequent loop.
  #p <- progress_estimated(nrow(afcOneDay))
  
  # Set the first value to 1
  afcOneDay$id[1] <- 1
  print(paste("Processing id for", date_p, "at", Sys.time()))
  system.time({
    for (i in 2:nrow(afcOneDay)) {
      #p$tick()$print()
      if (afcOneDay$row_number[i] > afcOneDay$row_number[i - 1]) {
        afcOneDay$id[i] <- afcOneDay$id[i - 1] 
      } else {
        afcOneDay$id[i] <- afcOneDay$id[i - 1] + 1 
    }
  }})
  
  ################# Second tier loop needs to go through each isolated set of
  ################# journey data.
  print(glue("Starting process directions for {date_p} at {Sys.time()}"))
  # Create an empty list to record the output from each iteration of the loop.
  results = list()
  
  # progress bar slows progress (ironic). Useful for diagnostics. Uncomment
  # p$tick()$print() in next loop.
  # 
  #p <- progress_estimated(length(unique(afcOneDay$id)))
  
  
  for (k in 1:length(unique(afcOneDay$id))) {
    #p$tick()$print()
    # Create a subset of the afcOneDay data which captures a single
    # Sealer_id/etm_service_number combination.
    afcOneDay_sealer <- afcOneDay %>% filter(id == k)
    
    # How many records contain direction information from the AFC_AVL_TT_Link
    # data. (Important during validation)
    #afcOneDay_sealer_n <- afcOneDay_sealer %>% count(direction)
    # Examine the data to confirm contents
    #afcOneDay_sealer %>% head
    
    
    ######## Stage progression versus vehicle direction ########
    
    # As a rule of thumb, ???? stages rise outbound and decrease inbound
    #                     ???? stages rise outbound and decrease inbound
    if (!na.omit(afcOneDay_sealer$isam_op_code)[1] == '????') {
      dir1_p <- "I"
      dir2_p <- "O"
      dir.unknown <- "U"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????') {
      dir1_p <- "O"
      dir2_p <- "I"
      dir.unknown <- "U"
    }
    
    
    
    # ???? options
    if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
        afcOneDay_sealer$etm_service_number == '2') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' && 
               afcOneDay_sealer$etm_service_number == '3') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' && 
               afcOneDay_sealer$etm_service_number == '6S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' && 
               afcOneDay_sealer$etm_service_number == '9S') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '11') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '12X') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '13') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '13A') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' && 
               afcOneDay_sealer$etm_service_number == '16S') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '18') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '18A') {
      dir1_p <- "I"
      dir2_p <- "O"
    }  else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
                afcOneDay_sealer$etm_service_number == '20') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '20A') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '20E') {
      dir1_p <- "I"
      dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '23') {
      dir1_p <- "I"
      dir2_p <- "O"
    }
    
    
    # ???? options
    
    if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
        afcOneDay_sealer$etm_service_number == '8A') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '8C') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '11A') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '11C') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '15A') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '15C') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '18') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '22S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '31') {
      dir1_p <- "I"
      dir2_p <- "O"
      #} 
      #else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
      #          afcOneDay_sealer$etm_service_number == '37') {
      # dir1_p <- "I"
      #  dir2_p <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '46A') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '59S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '68A') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '68C') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '70A') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    }else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
              afcOneDay_sealer$etm_service_number == '241') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '297') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '335') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '336') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '728') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '937') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == '997') {
      dir1_p <- "O"
      dir2_p <- "I"
    } else if (na.omit(afcOneDay_sealer$isam_op_code)[1] == '????' &&
               afcOneDay_sealer$etm_service_number == 'S1') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    }
    
    # need to add third variable for unknowns.
    
    
    if (afcOneDay_sealer$etm_service_number == '6E') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '7S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '32') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '46S') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '47S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '59A') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '59C') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '71A') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '71S') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '225S') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '710') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '712') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '762') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '791') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '827') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '828') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '838A') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '855') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '872') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '877') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '878') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '879') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '881') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } else if (afcOneDay_sealer$etm_service_number == '888') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '897') {
      dir1_p <- "I"
      dir2_p <- "I"
      dir.unknown <- "I"
    } else if (afcOneDay_sealer$etm_service_number == '900A') {
      dir1_p <- "O"
      dir2_p <- "O"
      dir.unknown <- "O"
    } 
    
    # A challenge is to determine the direction of travel when a vehicle is at
    # its terminal stages. i.e where there 10 stages, we cannot easily determine
    # wheather a journey is is inbound or outbound when at stages 1 and 10. In 
    # practice, both options are possible. Thus, we make an assumption that, 
    # dependent on our previously specified directions, the bus is begining the
    # next journey number in opposition to the previous directon of travel. In
    # the case of '????', this would mean a transaction at stage 1 will be
    # recorded as outbound. However, not all journeys originate at 1 and finish
    # at 10. Thus, the following identifies the highest and lowest stages based
    # on the data that we hold.
    
    # Need to determine the highest and lowest stages recorded on the service
    # being observed.
    # The start_stage < 100 parameter is designed to omit invalid journey
    # stages.
    
    # looks for the highest and lowest stages within the days AFC data to
    # determine the highest and lowest associated start stages.
    # I think we need to look more widely for this information. Where few
    # journeys occur this may result in the incorrect stage being
    # considered as the final stage. Possibly the query should be extended to
    # cover a full week.
    
    # Based on all data from the following week.
    
    afcOneDay_sealer_paramsa <- afcOneweek %>% 
      filter(etm_service_number == afcOneDay_sealer$etm_service_number[1] &
               start_stage < 100 &
               isam_op_code == na.omit(afcOneDay_sealer$isam_op_code)[1]) %>%
      arrange(desc(start_stage)) %>%
      select(start_stage) %>%
      head(1)
    
    afcOneDay_sealer_params2a <- afcOneweek %>%
      filter(etm_service_number == afcOneDay_sealer$etm_service_number[1] &
               start_stage < 100 &
               isam_op_code == na.omit(afcOneDay_sealer$isam_op_code)[1]) %>%
      arrange(start_stage) %>% select(start_stage) %>% head(1)
    
    # Based on the max journey stage identified previously, here we filter the
    # afc data to omit the 'invalid' records.
    
    # try(
    # afcOneDay_sealer[afcOneDay_sealer$start_stage >= 100,]$start_stage <- 
    #   afcOneDay_sealer_paramsa[1,1]
    # , silent = T)
    
    results[[length(results) + 1]] <- afcOneDay_sealer %>% 
      filter(start_stage > afcOneDay_sealer_paramsa$start_stage[1]) %>% 
      mutate(case2 = dir.unknown) %>% 
      select(id, 
             record_id, 
             start_stage, 
             isam_op_code,
             etm_service_number, 
             transaction_datetime,
              
             case2)
    
    afcOneDay_sealer <- afcOneDay_sealer %>% 
      filter(start_stage <= afcOneDay_sealer_paramsa$start_stage[1])
    
    
    if (nrow(afcOneDay_sealer) < 1) next()
    
    # Where an extended time period occurs between two transactions it is
    # possible that a bus could have reached its destination and inititated a
    # second journey in the opposing direction. Such behaviour may appear to
    # suggest the bus is traveling in the incorrect direction. To address this
    # we will incorporate a time constraint designed to further filter the data.
    
    # Calculate time interval in seconds between each transaction. If the
    # interval is greater that 30 minutes (1800 sescond),
    # record 'yes' else record 'no'
    # 
    afcOneDay_sealer <- afcOneDay_sealer %>%
      mutate(interval = transaction_datetime - lag(transaction_datetime,1)) %>%
      mutate(interval = ifelse(is.na(interval < 1), 0, interval)) %>% 
      mutate(interval_case = ifelse(interval >= 1800, 'yes', 'no'))
    
    # Create a new id field which will be used to identify subsets of journeys
    # based on time constraint.
    afcOneDay_sealer$id2 <- NA
    # Set the inital value to 1.
    afcOneDay_sealer$id2[1] <- 1
    
    # For each row confirm wheather time interval has exceed 30 minutes. If so,
    # restart numbering.
    
    if (nrow(afcOneDay_sealer) > 1) {
      for (i in 2:nrow(afcOneDay_sealer)) {
        if (!afcOneDay_sealer$interval_case[i] == 'yes') {
          afcOneDay_sealer$id2[i] <- afcOneDay_sealer$id2[i - 1] 
        } else if (!afcOneDay_sealer$interval_case[i] == 'no') {
          afcOneDay_sealer$id2[i] <- afcOneDay_sealer$id2[i - 1] + 1 
        } 
      }
    }
    
    ##### Third tier loop needs to go through each set transactions split by
    ##### time interval. #####
    
    # Count how many time each id2 is recorded in the data
    afcOneDay_sealerCount <- afcOneDay_sealer %>% count(id2)
    
    for (j in 1:nrow(afcOneDay_sealerCount)) {
      
      
      afcOneDay_sealer_sub <- afcOneDay_sealer %>% filter(id2 == j) 
      
      
      # if count is 1 we have no complementary information to process the
      # transactions. However, we may apply our heuristic based on the first and
      # last start stage.
      
      if (length(unique(afcOneDay_sealer_sub$start_stage)) == 1) {
        afcOneDay_sealer_sub <- afcOneDay_sealer_sub %>%
          mutate(
            case = ifelse(start_stage == 
                            afcOneDay_sealer_paramsa$start_stage[1],
                          dir1_p,
                          ifelse(start_stage ==
                                   afcOneDay_sealer_params2a$start_stage[1], 
                                 dir2_p, dir.unknown)))  
        
        #results <- results %>% rbind(afcOneDay_sealer_sub %>% select(id,
        #record_id, tt_operator, isam_op_code,etm_service_number, start_stage,
        #transaction_datetime, direction, case2 = case))
        
        if (nrow(afcOneDay_sealer_sub[
          which(afcOneDay_sealer_sub$start_stage ==
                afcOneDay_sealer_params2a$start_stage[1]),]) > 0) {
          
          afcOneDay_sealer_sub[
            which(afcOneDay_sealer_sub$start_stage == 
                    afcOneDay_sealer_params2a$start_stage[1]),]$case <- dir2_p
        }
        
        if (nrow(afcOneDay_sealer_sub[
          which(afcOneDay_sealer_sub$start_stage == 
                afcOneDay_sealer_paramsa$start_stage[1]),]) > 0) {
          afcOneDay_sealer_sub[
            which(afcOneDay_sealer_sub$start_stage == 
                    afcOneDay_sealer_paramsa$start_stage[1]),]$case <- dir1_p
        }
        
        
        results[[length(results) + 1]] <- 
          afcOneDay_sealer_sub %>% 
          select(id, record_id, 
                 isam_op_code,etm_service_number, start_stage,
                 transaction_datetime, case2 = case)
      } else {
        
        afcOneDay_sealer_sub$case <- NA
        
        afcOneDay_sealer_sub$id3[1] <- 1
        
        for (i in 2:nrow(afcOneDay_sealer_sub)) {
          if (afcOneDay_sealer_sub$start_stage[i] == 
              afcOneDay_sealer_sub$start_stage[i - 1]) {
            afcOneDay_sealer_sub$id3[i] = afcOneDay_sealer_sub$id3[i - 1]  
          } else {
            afcOneDay_sealer_sub$id3[i] <- afcOneDay_sealer_sub$id3[i - 1] + 1 
          }
        }
        
        stages_collapse <- collapseList(afcOneDay_sealer_sub$start_stage)
        stagesCount <- c(1:length(stages_collapse))
        
        # Create an empty chatacter vector containing case U (Unknown) values.
        # These will be updated in the subsequent stage.
        
        result_collapse <- rep(dir.unknown, length(stages_collapse))
        
        for (i in 2:length(stages_collapse)) {
          if (stages_collapse[i] < stages_collapse[i - 1]) {
            result_collapse[i] <- dir1_p
          } else {
            result_collapse[i] = dir2_p
          }
        }
        
        # Based on the second value, we can infer the probable direction of the
        # first recording.
        # 
        
        
        if (stages_collapse[1] <= stages_collapse[2]) {
          result_collapse[1] <- dir2_p
        } else {
          result_collapse[1] <- dir1_p
        }
        
        
        
        # Combine the lookup data and inferred direction results into a new
        # lookup table.
        join_res <- data.frame(stagesCount, stages_collapse, result_collapse,
                               stringsAsFactors = F)
        
        afcOneDay_sealer_sub <- 
          afcOneDay_sealer_sub %>% 
          left_join(join_res,
                    by = c("start_stage" = "stages_collapse",
                           "id3" = "stagesCount"))
        
        if (length(afcOneDay_sealer_sub[
          which(afcOneDay_sealer_sub$start_stage ==
                afcOneDay_sealer_params2a$start_stage[1]),
          ]$result_collapse) > 0) {
          afcOneDay_sealer_sub[
            which(afcOneDay_sealer_sub$start_stage == 
                    afcOneDay_sealer_params2a$start_stage[1]),
            ]$result_collapse <- dir2_p
        }
        if (length(afcOneDay_sealer_sub[
          which(afcOneDay_sealer_sub$start_stage == 
                afcOneDay_sealer_paramsa$start_stage[1]),
          ]$result_collapse) > 0) {
          afcOneDay_sealer_sub[
            which(afcOneDay_sealer_sub$start_stage == 
                    afcOneDay_sealer_paramsa$start_stage[1]),
            ]$result_collapse <- dir1_p
        }
        
        results[[length(results) + 1]] <- 
          afcOneDay_sealer_sub %>%
          select(id, record_id, start_stage,
                 isam_op_code, etm_service_number, transaction_datetime,
                 case2 = result_collapse)
      }
    }
  }
  
  do.call("rbind", results) %>% 
    data.frame %>% 
    select(record_id, direction = case2) %>% 
    dbWriteTable(con, name = "direction_of_travel", 
                 value = ., 
                 row.names = F, 
                 append = T)

  }

