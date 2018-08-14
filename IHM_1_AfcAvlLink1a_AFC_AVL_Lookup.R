######### Read in data from afc and avl ###########

# Script 1a_AFC_AVL_Lookup.R

# This script brings together the afc and avl data
# using the asset tracker as a means to match the records.


# The primary challenge in linking the transaction and GPS data is that there is
# no direct lookup between the two sets of collected data. the reason for this 
# is that the GPS logger has attribution linked to specific buses based on the 
# fleet number, whilst, the AFC data are recorded based on the ticketing machine 
# ISAM number. 
# 
# The issue arrises because ticketing machines are redistributed around the fleet 
# at various intervals. consequently, the link between Fleet number and transaction
# is time dependent. Consequently, it is necessary to first determine the appropriate 
# fleet number for each transaction based on asset tracker information. The asset 
# tracker provides a lookup between ticketing machines (ISAM number) and fleet number 
# at regular time intervals. 
# 
# The approach to matching fleet number to transactions here is fairly simplistic 
# and could potentially be enhanced. Here, we seek to make the link first based 
# on the most recent fleet number, then the previous fleet number and then the following 
# fleet number from the asset tracker. 

# A trial run for a sunday took 

# Import the required libraries
library(dplyr)
library(DBI)
library(lubridate)
library(stringr)
library(data.table)

# Establish a connection with the database
con <- src_postgres(
  dbname = "",
  host = "",
  user = "",
  password = ""
)

## Set the Working directory.
setwd("\\\\jdi-blue-fs1/cdrc017$/Analysis/WP3/AFC_AVL_Data_Linkage_Scripts")

# create a new directory to store the outputted files and then create an empty 
# folder to store the results
dir.create("afc_avl_data_linkage_files")
filepath <- "/afc_avl_data_linkage_files/"

# Alternativly, the output for each day could be piped directly to the database. 


dbSendQuery(con$con, "create schema afc_avl_lookup")




# Create a link to each database table using dplyr

# In these earlier scripts we make use of the dplyr database interface. In effect,
# the database is set as the data source using tbl() and then standard dplyr commands 
# can be applied to the database. When executed, the dplyr commands are transformed 
# into the appropriaet SQL. The SQL command can be seen by adding %>% explain() 
# to the end of the query. 

AT <- tbl(con, "asset_tracker_final")
cch <- tbl(con, "cch")

# Import all of the unique concessionary card isrns. A filter is applied such
# that the data relate solely to Passengers with Over-60 Concessionary travel
# passes.
concessionaryCards <-
  cch %>%
  filter(type == "Over 60 Concession") %>%
  select(card_isrn) %>%
  distinct() %>%
  collect()

# Import the Asset Tracker data
asset_tracker <-
  AT %>%
  select(date_trunc = date, operator, isam, bus) %>%
  collect() %>%
  filter(!bus == "-1") %>%
  as.data.frame()

############### functions #########################

#Time matching function to identify the nearest observation from the avl table.
# The calculation looks both forwards and backwards.
# Note, the match includes both fleet_no and service number.
# Use of both parameters reduces the risk that incorrect records are matched.

timeMatch <- function(transaction_datetime,
                      etm_service_number,
                      bus) {
  t = NA 
  # specify NA as the default value to be returned
  # based on the model parameters, filter the avl data so that only data from
  # the specific bus (=fleet number!) and service_number are collected. Calculate the absolute
  # difference between the transaction time and each avl timestamp. arramge by
  # distance in time and return the nearest avl measurement basedo on time.
  tripData <-
    avl_one_day %>%
    filter(fleet_no_clean == bus,
           public_service_code == etm_service_number) %>%
    data.frame %>%
    mutate(time_diff = abs(int_length(
      interval(timestamp,
               transaction_datetime)
    ))) %>%
    arrange(time_diff) %>%
    slice(1)
  if (exists("tripData"))
    t <- tripData[1, 1]
  return(t)
}


# The process is divided into a series of sets which reflect the temporal period
# covered by each timetable.

############### Loop Setup #########################

# Set 1A
dates <- as.character(seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day")[1:27])
log_file <- file(paste0(getwd(), filepath, "logfile_1a.txt"), "w")

# Set 1B
dates <- as.character(seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day")[28:55])
log_file <- file(paste0(getwd(), filepath, "logfile_1b.txt"), "w")

# Set 2A
dates <-  as.character(seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day")[1:52])
log_file <- file(paste0(getwd(), filepath, "logfile_2a.txt"), "w")

# Set 2
dates <- as.character(seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day")[53:105])
log_file <- file(paste0(getwd(), filepath, "logfile_2b.txt"), "w")

# Set 3A
dates <- as.character(seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day")[1:41])
log_file <- file(paste0(getwd(), filepath, "logfile_3a.txt"), "w")

# Set 3B
dates <- as.character(seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day")[42:84])
log_file <- file(paste0(getwd(), filepath, "logfile_3b.txt"), "w")

# Set 4A
dates <- as.character(seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), by = "day")[1:44])
log_file <- file(paste0(getwd(), filepath, "logfile_4a.txt"), "w")

#Set 4B # note the array has a gap where AVL data are not available.
dates <- as.character(seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), by = "day")[c(45:74, 79:98)])
log_file <- file(paste0(getwd(), filepath, "logfile_4b.txt"), "w")

#Set 5
dates <- as.character(seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"))
log_file <- file(paste0("/Analysis/testrun/logfile_5.txt"), "w")  # !!! filename for test run purpose 


dates <- '2016-07-17'

# For each date, perform the following series of operations.
for (date in dates) {
  # Print to console and write to log initial status for each days processing.
  cat(paste("Starting process for", date, "at", Sys.time()) ,
    file = log_file, append = T, sep = "\n"
  )
  
  print(paste("Starting process for", date, "at", Sys.time()))
  
  
  # Set the start datetime and end datetime to be employed in each query.
  start_date <- paste0(date, " 00:00:00")
  end_date <- paste0(date, " 23:59:59")
  
  
  ################################## Data Preparation #######################
  
  # Import the relevant AVL data for the specified day. Note that Fleet_no is
  # processed to remove any non-numeric characters.
  
  # note the use of regexp_replace. Fleet numbers are always numeric. However,
  # in the AVL data some operators include extra letters.
  


  avl_one_day <-
    dbGetQuery(con$con,
              paste0(
              "select _id as avl_id, public_service_code, timestamp, journey_no,
              journey_scheduled, lon, lat, date_trunc('minute', timestamp),
              regexp_replace(fleet_no, '[^0-9]', '', 'g')::integer as fleet_no_clean
              from avl
              where timestamp between '", start_date, "' and '", end_date, "'")
    )

  
  # Write to log how many avl records have been returned from the database.
  cat(
    paste("Raw AVL Data contains", nrow(avl_one_day), "rows.") ,
    file = log_file,
    append = T,
    sep = "\n"
  )
  
  # Import the AFC data for one day
  afc_input_data <- 
    dbGetQuery(con$con,
          paste0(
            "select record_id, card_isrn, date_trunc('day',  transaction_datetime),
            transaction_datetime, etm_service_number, right(sealer_id, 8) as isam,
            trim(leading '0' from trip_or_train_number) as trip_or_train_number
            from afc
            where transaction_datetime between '", start_date,"' and '", end_date, "'")
        )
  
  # Write to log how many AFC records have been returned from the database.
  cat(
    paste("Raw AFC Data contains", nrow(afc_input_data), "rows."),
    file = log_file, append = T, sep = "\n")
  
  # Identify unique ISAM codes recorded within the Asset Tracker data. objective
  # here to filter out buses that are not operated by National Express.
  isams <- AT %>%
    select(isam) %>% 
    distinct %>% 
    collect()
  
  # Remove any afc records where the afc isam is not in the Asset tracker table. It
  # is belived these records correspond with services not operator by National
  # Express
  afc_input_data <- afc_input_data %>%
    filter(isam %in% isams$isam)
  
  cat(
    paste(
      "AFC Data filtered by isams contains", nrow(afc_input_data), "rows."),
    file = log_file, append = T, sep = "\n")
  
  # Remove any records which are not associated with those using Over 60 Travel
  # concessionary passes
  afc_input_data <- afc_input_data %>%
    filter(card_isrn %in% 
             concessionaryCards$card_isrn)
  
  cat(
    paste(
      "AFC Data filtered by isams amd cards contains", nrow(afc_input_data), "rows."),
    file = log_file, append = T, sep = "\n")
  
  # Remove any records where servive number does not appear in the AVL data.
  afc_input_data <- afc_input_data %>%
    filter(etm_service_number %in% (avl_one_day$public_service_code %>% unique))
  
  print(paste(
    "AFC Data filtered by isams, cards and service numbers contains",
    nrow(afc_input_data),
    "rows."
  ))
  
  cat(
    paste(
      "AFC Data filtered by isams, cards and service numbers contains",
      nrow(afc_input_data),
      "rows."
    ),
    file = log_file,
    append = T,
    sep = "\n"
  )
  
  # Number of rows in AFC where etm_service_number is in AVL
  afc_feasible_rows <- afc_input_data %>%
    filter(etm_service_number %in% (avl_one_day$public_service_code %>%
                                      unique)) %>%
    nrow()
  
  # calculate the mean lat and mean lon for each minute (Truncated). Necessary
  # given that the AFC timestamps are truncated.
  
  avl_minute_aggregate <- avl_one_day %>%
    select(date_trunc, fleet_no_clean, public_service_code, lat, lon) %>%
    group_by(date_trunc, fleet_no_clean, public_service_code) %>%
    summarise(avg_lon = mean(lon), avg_lat = mean(lat))
  
  # Convert both AFC and Asset tracker data into identical time class.
  afc_input_data$date_trunc <- ymd(afc_input_data$date_trunc)
  asset_tracker$date_trunc <- ymd(asset_tracker$date_trunc)
  
  
  ##############################################################################
  #                                                                            #
  #         Run 1: Based on the first fleet number                             #
  #                                                                            #
  ##############################################################################
  
  cat("starting run 1",
      file = log_file,
      append = T,
      sep = "\n")
  
  print("starting run 1")
  
  # For clarity, create a data set for the first run.
  run1.data <- afc_input_data
  
  # Create the ISAM Bus Lookup table for run 1
  run1.isam_bus_lookup <- 
    run1.data %>%
    select(isam) %>%
    distinct() %>%
    left_join(asset_tracker, by = c("isam")) %>% 
    filter(!is.na(bus), 
           ymd(date_trunc) <= ymd(str_sub(start_date, 1, 10))) %>%
    group_by(isam) %>%
    mutate(date_trunc1 = ymd(date_trunc)) %>%
    arrange(isam, desc(date_trunc1)) %>%
    filter(row_number() == 1)
  
  # THIS IS IMPORTANT TO FIX
  run1.isam_bus_lookup$date_trunc <- ymd(date)
  
  
  ########################
  # Run 1 Phase 1
  ########################
  
  # First join is based on four parameters: transaction datetime, service number
  # and fleet number
  run1.phase1.join <- 
    run1.data %>%
    left_join(run1.isam_bus_lookup, by = c("date_trunc" = "date_trunc", "isam" = "isam")) %>%
    left_join(
      avl_minute_aggregate,
      by = c(
        "transaction_datetime" = "date_trunc",
        "etm_service_number" = "public_service_code",
        "bus" = "fleet_no_clean"
      )
    )
  
  
  # What proportion of the data have been matched in Run 1 phase 1
  print(paste("1_1_match:", run1.phase1.join %>%
                filter(!is.na(avg_lat)) %>% nrow))
  cat(
    paste("1_1_match:", run1.phase1.join %>%
            filter(!is.na(avg_lat)) %>%
            nrow),
    file = log_file,
    append = T,
    sep = "\n"
  )
  
  
  run1.phase1.output <- run1.phase1.join %>%
    filter(!is.na(avg_lat)) %>%
    mutate(case = "1_1",
           time_diff = NA,
           avl_id = NA) %>%
    select(
      case,
      record_id,
      avl_id,
      fleet_no_clean = bus,
      lon = avg_lon,
      lat = avg_lat,
      time_diff
    )
  
  ########################
  # Run 1 Phase 3: identify closest AVL record where no match was found based on 
  # timestamp of AFC data (1 minute resolution)
  ########################
  
  # Subset of phase 2 data where data have not been joined.
  run1.phase3.data <- run1.phase1.join %>% filter(is.na(avg_lat))
  
  # Subset those rows which have an asociated bus id
  # remove rows where fleet number not in avl. Saves time.
  run1.phase3.withBus <- run1.phase3.data %>%
    filter(!is.na(bus)) %>%
    filter(bus %in% avl_one_day$fleet_no_clean)
  
  if (nrow(run1.phase3.withBus) > 0) {

    # Perform the rolling time join.
    # This join can be quite slow. For each transaction, fleet number etc, the 
    # script attempts to identify the nearest GPS observervation in time.
    # Here we are using the non-aggregate gps data.
    
    run1.phase3.join <-  
      run1.phase3.withBus %>%
      rowwise() %>%
      mutate(column = timeMatch(transaction_datetime, etm_service_number, bus)) %>%
      left_join(avl_one_day, by = c("column" = "avl_id")) %>%
      mutate(time_diff =
               abs(int_length(as.interval(
                 interval(timestamp, transaction_datetime)
               ))))
    
    # What proportion of the data have been matched in Run 1 phase 3
    print(paste("1_3_match:", 
                run1.phase3.join %>% filter(!is.na(lat)) %>% nrow))
    
    cat(
      paste("1_3_match:", run1.phase3.join %>% filter(!is.na(lat)) %>% nrow),
      file = log_file,
      append = T,
      sep = "\n"
    )
    
    
    # Create the output table for Run 1 Phase 3
    run1.phase3.output <-
      run1.phase3.join %>% 
      filter(!is.na(time_diff)) %>% 
      mutate(case = "1_3") %>% 
      select(case,
             record_id,
             avl_id = column,
             fleet_no_clean,
             lon,
             lat,
             time_diff)
    
    # Create a list of the record_ids of those transactions that have been
    # sucessfully matched.
    run1.matched_record_ids <-
      c(run1.phase1.output$record_id,
        run1.phase3.output$record_id)
    
    # Combine the summary data frome each phase of run 1
    run1.output <- rbind(run1.phase1.output, run1.phase3.output)
  } else{
    run1.output <- rbind(run1.phase1.output)
    # keep a record of what transactions have been matched.
    run1.matched_record_ids <- c(run1.phase1.output$record_id)
    
  }
  
  ##############################################################################
  #                                                                            #
  #         Run 2: Based on the following fleet number                         #
  #                                                                            #
  ##############################################################################
  
  # A useful addition to this section would be to exlude those combinations
  # which have been tested previously I.e, removing duplication of effort.
  
  cat("starting run 2",
      file = log_file,
      append = T,
      sep = "\n")
  print("starting run 2")
  
  # Create new data object based on records not joined in the preceding phase.
  run2.data <-
    afc_input_data %>% filter(!record_id %in% run1.matched_record_ids)
  
  # only run this if there are unmatched rows left over
  if (nrow(run2.data) > 1) {
    # Create the ISAM Bus Lookup table for run 2
    # Version looks for pre-ceding isam code.
    
    
    
    run2.isam_bus_lookup <-
      run2.data %>% 
    select(isam) %>% 
      distinct %>% 
      data.frame %>%
      left_join(asset_tracker, by = c("isam")) %>%
      filter(!is.na(bus), ymd(date_trunc) > ymd(str_sub(start_date, 1, 10))) %>%
      group_by(isam) %>% 
      mutate(date_trunc1 = ymd(date_trunc)) %>% 
      arrange(isam, date_trunc1) %>% 
      filter(row_number() == 1)
    
    
    # THIS IS IMPORTANT TO FIX
    run2.isam_bus_lookup$date_trunc <- ymd(date)
    
    
    ########################
    # Run 2 Phase 1
    ########################
    
    # First join is based on parameters: transaction datetime, service number anf fleet number
    run2.phase1.join <-
      run2.data %>% left_join(run2.isam_bus_lookup,
                              by = c("date_trunc" = "date_trunc", "isam" = "isam")) %>%
      left_join(
        avl_minute_aggregate,
        by = c(
          "transaction_datetime" = "date_trunc",
          "etm_service_number" = "public_service_code",
          "bus" = "fleet_no_clean"
        )
      )
    
    
    # What proportion of the data have been matched in Run 2 phase 1
    print(paste("2_1_match:", run2.phase1.join %>% filter(!is.na(avg_lat)) %>% nrow))
    #print(paste("2_1_no_match:", run2.phase1.join %>% filter(is.na(avg_lat)) %>% nrow))
    cat(
      paste("2_1_match:", run2.phase1.join %>% filter(!is.na(avg_lat)) %>% nrow),
      file = log_file,
      append = T,
      sep = "\n"
    )
    
    
    # Create the output table for Run 2 Phase 1
    run2.phase1.output <-
      run2.phase1.join %>% 
      filter(!is.na(avg_lat)) %>% 
      mutate(case = "2_1",
             time_diff = NA,
             avl_id = NA) %>% select(
               case,
               record_id,
               avl_id,
               fleet_no_clean = bus,
               lon = avg_lon,
               lat = avg_lat,
               time_diff
               )
    
    ########################
    # Run 2 Phase 3: identify closest AVL record where no match was found based on 
    # timestamp of AFC data (1 minute resolution)
    ########################
    
    # Subset of phase 2 data where data have not been joined.
    run2.phase3.data <- run2.phase1.join %>% filter(is.na(avg_lat))
    
    run2.phase3.data <-
      anti_join(run2.phase3.data,
                run1.phase3.data,
                by = c("record_id", "card_isrn", "bus"))
    
    
    # Subset those rows which have an asociated bus id
    # remove rows where fleet number not in avl. Saves time.
    run2.phase3.withBus <-
      run2.phase3.data %>% 
      filter(!is.na(bus)) %>% 
      filter(bus %in% 
               avl_one_day$fleet_no_clean)
    
    if (nrow(run2.phase3.withBus) > 0) {
      # Perform the rolling time join.
      system.time({
        run2.phase3.join <-
          run2.phase3.withBus %>% 
          rowwise() %>% 
          mutate(column = 
                   timeMatch(transaction_datetime, etm_service_number, bus)) %>%
          left_join(avl_one_day, by = c("column" = "avl_id")) %>%
          mutate(time_diff = 
                   abs(int_length(
                     as.interval(
                       interval(
                         timestamp, transaction_datetime
                         )
                       )
                     )
                     )
                 )
      })
      
      # What proportion of the data have been matched in Run 2 phase 3
      print(paste(
        "2_3_match:",
        run2.phase3.join %>% 
          filter(!is.na(lat)) %>% 
          nrow()
      ))

      cat(paste("2_3_match:", run2.phase3.join %>% filter(!is.na(lat)) %>% nrow()),
        file = log_file,
        append = T,
        sep = "\n"
      )
      
      # Create the output table for Run 2 Phase 3
      run2.phase3.output <-
        run2.phase3.join %>% 
        filter(!is.na(time_diff)) %>% 
        mutate(case = "2_3") %>% 
        select(case, 
               record_id,
               avl_id = column,
               fleet_no_clean,
               lon,
               lat,
               time_diff
               )
      
      
      # Create a list of the record_ids of those transactions that have been
      # sucessfully matched.
      run2.matched_record_ids <-
        c(run2.phase1.output$record_id,
          run2.phase3.output$record_id)
      
      
      # Combine the summary data frome each phase of run 2
      run2.output <- rbind(run2.phase1.output, run2.phase3.output)
      
      
    } else {
      run2.output <- rbind(run2.phase1.output)
      run2.matched_record_ids <- c(run2.phase1.output$record_id)
      
    }
    
  } else {
    run2.output <- NA
  }
  
  
  ##############################################################################
  #                                                                            #  
  #         Run 3: Based on the preceding fleet number                         #
  #                                                                            #
  ##############################################################################
  
  cat("starting run 3",
      file = log_file,
      append = T,
      sep = "\n")
  print("starting run 3")
  
  # Create new data object based on records not joined in the preceding phase.
  run3.data <-
    afc_input_data %>% 
    filter(!record_id %in% c(run1.matched_record_ids, run2.matched_record_ids))
  
  if (nrow(run3.data) > 1) {
    # Create the ISAM Bus Lookup table for run 3
    # Version looks for previous isam-fleetnumber combination.
    run3.isam_bus_lookup <-
      run3.data %>% 
      select(isam) %>%
      distinct %>%
      data.frame %>%
      left_join(asset_tracker, by = c("isam")) %>%
      filter(!is.na(bus), ymd(date_trunc) <= ymd(str_sub(start_date, 1, 10))) %>%
      group_by(isam) %>% mutate(date_trunc1 = ymd(date_trunc)) %>%
      arrange(isam, desc(date_trunc1)) %>%
      # same logic as before, second row provides previous isam lookup. May be
      # the same as before..
      filter(row_number() == 2)
    
    
    # THIS IS IMPORTANT TO FIX
    run3.isam_bus_lookup$date_trunc <- ymd(date)
    
    
    ########################
    # Run 3 Phase 1
    ########################
    
    # First join is based on four parameters: journey number, transaction
    # datetime, service number anf fleet number
    run3.phase1.join <-
      run3.data %>% 
      left_join(run3.isam_bus_lookup,
                by = c("date_trunc" = "date_trunc",
                       "isam" = "isam")) %>% 
      left_join(avl_minute_aggregate,
                by = c(
                  "transaction_datetime" = "date_trunc",
                  "etm_service_number" = "public_service_code",
                  "bus" = "fleet_no_clean"
                  )
                )
    
    
    # What proportion of the data have been matched in Run 3 phase 1
    print(paste("3_1_match:", 
                run3.phase1.join %>% 
                  filter(!is.na(avg_lat)) %>%
                  nrow()
                )
          )

    cat(
      paste("3_1_match:", run3.phase1.join %>% 
              filter(!is.na(avg_lat)) %>% 
              nrow()
            ),
      file = log_file,
      append = T,
      sep = "\n"
    )
    
    
    # Create the output table for Run 3 Phase 1
    run3.phase1.output <-
      run3.phase1.join %>% 
      filter(!is.na(avg_lat)) %>% 
      mutate(case = "3_1",
             time_diff = NA,
             avl_id = NA) %>% select(
               case,
               record_id,
               avl_id,
               fleet_no_clean = bus,
               lon = avg_lon,
               lat = avg_lat,
               time_diff
               )
    
    
    ########################
    # Run 3 Phase 3
    ########################
    
    # Subset of phase 2 data where data have not been joined.
    run3.phase3.data <-
      run3.phase1.join %>% filter(is.na(avg_lat))
    run3.phase3.data <-
      anti_join(run3.phase3.data,
                run2.phase3.data,
                by = c("record_id", "card_isrn", "bus"))
    
    # Subset those rows which have an asociated bus id
    # remove rows where fleet number not in avl. Saves time.
    run3.phase3.withBus <-
      run3.phase3.data %>% 
      filter(!is.na(bus)) %>% 
      filter(bus %in% avl_one_day$fleet_no_clean)
    
    if (nrow(run3.phase3.withBus) > 0) {
      # Perform the rolling time join.
      run3.phase3.join <-
        run3.phase3.data %>% 
        rowwise() %>% 
        mutate(column = 
                 timeMatch(transaction_datetime, etm_service_number, bus)) %>%
        left_join(avl_one_day, by = c("column" = "avl_id")) %>%
        mutate(time_diff = abs(int_length(as.interval(
        interval(timestamp, transaction_datetime)
        )
        )
        )
        )
      
      
      # What proportion of the data have been matched in Run 3 phase 3
      print(paste(
        "3_3_match:",
        run3.phase3.join %>% filter(!is.na(lat)) %>% nrow
      ))
      
      cat(
        paste(
          "3_3_match:",
          run3.phase3.join %>% 
            filter(!is.na(lat)) %>% 
            nrow()
        ),
        file = log_file,
        append = T,
        sep = "\n"
      )
      
      # Create the output table for Run 3 Phase 3
      run3.phase3.output <-
        run3.phase3.join %>% 
        filter(!is.na(time_diff)) %>% 
        mutate(case = "3_3") %>% 
        select(case, 
               record_id,
               avl_id = column,
               fleet_no_clean,
               lon,
               lat,
               time_diff)
      
      
      # Create a list of the record_ids of those transactions that have been
      # sucessfully matched.
      
      run3.matched_record_ids <-
        c(
          run2.phase1.output$record_id,
          run2.phase3.output$record_id
        )
      
      # Combine the summary data frome each phase of run 3
      run3.output <-
        rbind(run3.phase1.output,
              run3.phase3.output)
      
    } else {
      run3.output <- rbind(run3.phase1.output)
      run3.matched_record_ids <-
        c(run3.phase1.output$record_id,
          run3.phase3.output$record_id)
      
    }
    
  } else {
    run3.output <- NA
  }
  
  
  ##############################################################################
  #                                                                            #
  #     Recombine all of the merged data.                                      #
  #                                                                            #
  ##############################################################################
  
  # Combine the outputs generated from runs 1 through 3.
  day.output <- rbind(run1.output, run2.output, run3.output)
  
  # Construct output table for those afc records which were not matched
  unmatched <- afc_input_data %>% 
    filter(!record_id %in% day.output$record_id) %>%
    mutate( case = 4, avl_id = NA, fleet_no_clean = NA, lon = NA, lat = NA, time_diff = NA) %>% 
    select(case, record_id, avl_id, fleet_no_clean, lon, lat, time_diff)
  
  # Combine the table of match data with the unmatched data. Note, the unmatched
  # data are recorded as code 4.
  final.output <- rbind(day.output, unmatched)
  
  # Write out the complete join table.

  fwrite(
    final.output,
    paste0(getwd(), filepath , date, ".csv"), row.names = F
  )

  # Instead of writing each file to a csv file, it may be better to write the data
  # directly to the database. 
  

  
  dbWriteTable(con$con, name = c('afc_avl_lookup', 
                                 paste0("afc_avl_lookup_", str_replace_all(date, '-','_'))), 
               final.output, row.names=F)
  
  
  
  #### print diagnostic messages and record in log file
  
  print(paste0("For ",date, " match rate was ", round(nrow(day.output) / nrow(final.output), 2)*100, "%"))
  
  cat(
    paste(paste0("For ",date, " match rate was ", round(nrow(day.output) / nrow(final.output), 2)*100, "%")),
    file = log_file, append = T, sep = "\n"
  )
  cat(
    paste("Finishing process for", date, "at", Sys.time()) ,
    file = log_file, append = T, sep = "\n"
  )
  
  # clear all the files created as a byproduct of matching for each date.  
  rm_list <- as.list(str_subset(ls(), "run"), start_date, end_date, avl_one_day, unmatched, avl_minute_aggregate, 
     afc_input_data, isams, final.output, day.output, afc_feasible_rows)
  do.call(what = 'rm', rm_list)
  
} # End of loop


# Close the log file
close(log_file)
