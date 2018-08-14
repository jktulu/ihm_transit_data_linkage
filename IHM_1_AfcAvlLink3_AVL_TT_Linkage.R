# Script 3 of 4.

#######################################################
## Raw Data Required ##
#
# 1. AVL - Vehicle automated locations
# 2. CCH - Customer details (Over-60 Concessionary travellers)
# 3. Asset Tracker - to determine bus operator
# 4. Timetable data for the appropriate period
######################################################

## Processed Data Required ##
#
# 1. Requires Processed timetable.
# 2. Requires Naptan Stops table
# 3. Requires avl_tt_lookup_",year_p,"_",month_p,"
######################################################

# Import the required libraries
library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(lubridate)
library(geosphere)

# Establish a connection with the database.

# Increasing the effective cache size provides the sql interpreter with a better impression of the database's capabilities.   
dbGetQuery(con, "set effective_cache_size to '64GB'")


# For the purpose of efficiency it is necessary that the data processing be split into a series of distinct parts. 
# For pragmatic reasons, the data are split based on the timetable period of validity.

#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day")[1:27]; year_p = 2015; month_p = "jul" # set 1a
#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day")[28:55]; year_p = 2015; month_p = "jul" # set 1b
#dates <-  seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day")[1:52]; year_p = 2015; month_p = "oct" # set 2a
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day")[53:105]; year_p = 2015; month_p = "oct" # set 2b
dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day")[1:41]; year_p = 2016; month_p = "jan" # set 3a
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day")[42:84]; year_p = 2016; month_p = "jan" # set 3b
#dates <- seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), by = "day")[c(1:44)]; year_p = 2016; month_p = "apr" # set 4a
#dates <- seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), by = "day")[c(45:74,79:98)]; year_p = 2016; month_p = "apr" # set 4b
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); year_p = 2016; month_p = "jul" # set 5a


####################################
## Test Dates
dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-11"), by = "day")
year_p = 2016; month_p = 'apr'
###################################

# Determine the last arrival time for any journey on the day of interest.
# This is used as a threshold for subsetting data at a later stage.
last_scheduled_arrival <-
  dbGetQuery(
    con,
    paste0(
      "select arrive from timetables.tt_", year_p, "_", month_p,
      "_processed where type = 'QT' and journey_scheduled >= '23:00:00' 
      and depart <= '06:00:00' and operator in ('?','?') order by 
      arrive desc limit 1"
    )
  )[1, 1]

# Edit the last scheduled time variable to include a further 30 minutes.
last_scheduled_arrival  <-
  format(strptime(last_scheduled_arrival, format = "%H:%M:%S") + 1800, "%H:%M:%S")


system.time({
  # First tier of loop designed to systematically process data based on date.
  for (j in 1:length(dates)) {
    
    # Set the date variable
    date_p = dates[j]
    
    # Print diagnostic message to console
    print(paste("Starting process for", date_p, "at", Sys.time()))
    
    # Set the date plus 1 day variable. Ensure character class.
    date_plus_1.p  <- as.character(format(ymd(date_p) + 1, "%Y-%m-%d"))
    
    
    
    
    # Identify unique timetable ids for journeys completed on the date being 
    # investigated.
    tt_ids <-
      dbGetQuery(
        con,
        paste0(
          "select distinct tt_id from avl_tt_lookup_", year_p, "_", month_p,"_test4_unique_all
          where timestamp between '", date_p, " 00:00:00' and  '", date_p, " 23:59:59' and tt_id is not null"
        ) 
      )
    
    print(paste("Unique tt_ids identified:", nrow(tt_ids)))
    
    # Create a progress bar which will update in the console.
    p <- progress_estimated(nrow(tt_ids))
    # Second tier of loop, run through each individual timetable id.
    system.time({
      for (i in c(1:nrow(tt_ids))) {
        
        # Set timetable id based on the loop iterator.
        code = tt_ids$tt_id[i]
        p$tick()$print()
        
        # From the linked AVL data, extract all data relevant to the specified timetable id.

        avl_coded <-
          dbGetQuery(
            con,
            paste0(
              "select t1.avl_id, t1.timestamp, t1.tt_date, t1.tt_id, t1.fleet_no, 
              t1.journey_scheduled, t1.route, t1.lat, t1.lon
              from avl_tt_lookup_", year_p, "_", month_p,"_test4_unique_all t1
              where 
              t1.tt_id = '", code, "' 
              and t1.timestamp between '",
              date_p, " 00:00:00' and '", date_plus_1.p, " ", last_scheduled_arrival, "'")
          )
        
        
        # Extract the relevant data from the timetable based on the code being investigated
        # Left join to naptan stop latitudes and longitudes.
        

        tt_coded <-
          dbGetQuery(
            con,
            paste0(
              "select t1.type, t1.id, t1.route, t1.timing_point, t1.journey_scheduled, t1.direction, 
              t1.naptan_code, t1.arrive, t2.stop_lon, t2.stop_lat, t1.n 
              from timetables.tt_", year_p, "_", month_p, "_processed t1 
              left join naptan_stops t2 on t1.naptan_code=t2.naptan_code 
              where t1.id = '", code, "' order by n"
            )
          )

        
        # Determine if the journey spans multiple days.
        tt_coded <-
          tt_coded %>% mutate(
            prev = lag(arrive, 1),
            difference = as.POSIXct(strptime(arrive, "%H:%M:%S")) - as.POSIXct(strptime(prev, "%H:%M:%S")),
            day = 0
          ) 
        n <-
          which(tt_coded$difference < 0) # identify first record where time_difference is negative.
        
        if (length(n) > 0) tt_coded$day[n:nrow(tt_coded)] <- 1 
        
        
        # Remove any avl recordings prior the the journey official scheduled start.
        avl_coded <-
          avl_coded %>% filter(ymd_hms(timestamp) >= ymd_hms(paste0(
            date_p, " ", tt_coded$journey_scheduled[1]
          )))
        
        # ERROR handling. In some cases there will be no avl data. If so, the loop kicks to the next code.
        if (nrow(avl_coded) < 1) {
          print(paste("skipping", code))
          dbWriteTable(
            con,
            name = paste0("avl_tt_linked_", year_p, "_", month_p, ""),
            
            tt_coded %>% 
              mutate(case = 'NM', date = as.character(date_p),
              timestamp = as.POSIXct(NA), tt_date = date_p, distance = NA, time_diff = NA,
              distance_tt_avl = NA, fleet_no = NA, n = NA, time_diff_tt_avl = NA,
              date = ifelse(day == 1, as.character(date_plus_1.p), as.character(tt_date)),
              arrive = paste(date, arrive)) %>% 
              select(case, id, route, type, direction, naptan_code, tt_date, arrive, 
                         timestamp, timing_point, day, distance_tt_avl, fleet_no, n, date, 
                         time_diff_tt_avl), 
            append = T,
            row.names = F
          )
          next()
        }
        
        # ERROR handling. In some cases, stops will not have a latitude and longitude. This 
        # issue is associated with the naptan stops table.
        if (nrow(tt_coded) > length(na.omit(tt_coded$stop_lon))) {
          tt_coded_omitted <- 
            tt_coded %>% 
            filter(is.na(stop_lon)) %>%
            mutate(case = "MS", tt_date = date_p, distance_tt_avl = NA, 
                   time_diff_tt_avl = NA, timestamp = as.POSIXct(NA),
                   date = NA) %>%  
            mutate(date = ifelse(day == 1, as.character(date_plus_1.p),
                                 as.character(tt_date)),
              arrive = as.POSIXct(paste(date, arrive))) %>%
            select(case, id, route, type, direction, naptan_code, tt_date, arrive,
                   timestamp, timing_point, day, distance_tt_avl, fleet_no, n, 
                   date, time_diff_tt_avl)
        }
        
        ## Once the first 50% of journeys are joined, the pool of possible avl data 
        # is subset based on the last time already matched.
        
        tt_coded1 <- tt_coded[which(!is.na(tt_coded$stop_lon)),][c(1:floor(nrow(tt_coded) / 2)),]
       
        tt_coded2 <- tt_coded[which(!is.na(tt_coded$stop_lon)),][c((floor(nrow(tt_coded) / 2) + 1):nrow(tt_coded[which(!is.na(tt_coded$stop_lon)),])),]
        
        rm(tt_coded)
       
        # calculate the distance between each stop and each of the associated avl points.
        D1 = distm(tt_coded1[, c("stop_lon", "stop_lat")], avl_coded[, c("lon", "lat")])
        
        # Based on the spatial data, match each bus stop location to the nearest avl measurement.
        # Include the euclidean distance in metres between the bus stop and avl recording.
        avlTTcomb1 <-
          cbind(tt_coded1, avl_coded[apply(D1, 1, which.min), 
                                     c("fleet_no","timestamp", "lat", "lon")], 
                apply(D1, 1, min))
        
        names(avlTTcomb1)[19] <- "distance_tt_avl"
        
        
        
        avl_coded <- avl_coded %>% filter(timestamp >= max(avlTTcomb1$timestamp))
        
        D2 = distm(tt_coded2[, c("stop_lon", "stop_lat")], avl_coded[, c("lon", "lat")])
        
        # Based on the spatial data, match each bus stop location to the nearest avl measurement.
        # Include the euclidean distance in metres between the bus stop and avl recording.
        avlTTcomb2 <-
          cbind(tt_coded2, avl_coded[apply(D2, 1, which.min), 
                                     c("fleet_no","timestamp", "lat", "lon")], apply(D2, 1, min))
        
        names(avlTTcomb2)[19] <- "distance_tt_avl"
        
        
        # Here, we supplement the joined data with the date of departure, the timetable code.
        avlTTcomb2 <-
          rbind(avlTTcomb1, avlTTcomb2) %>% 
          mutate(case = 'M',
                 id = code,
                 tt_date = date_p) %>%
          select(case, id, route, type, direction, naptan_code, tt_date, arrive, timestamp,
            timing_point, day, distance_tt_avl, fleet_no, n) %>%
          # Here, we update the arrive date if it spans multiple days
          mutate(date = ifelse(day == 1, as.character(date_plus_1.p), as.character(tt_date)),
            arrive = as.POSIXct(paste(date, arrive)),
            time_diff_tt_avl = int_length(interval(timestamp, arrive)))
        
        if (exists("tt_coded_omitted")) {
          avlTTcomb2 <- rbind(avlTTcomb2, tt_coded_omitted)
          rm(tt_coded_omitted)
        }
        
        dbWriteTable(
          con,
          name = paste0("avl_tt_linked_", year_p, "_", month_p, "_test4"),
          avlTTcomb2,
          append = T,
          row.names = F
        )
        rm(avlTTcomb1, avlTTcomb2, avl_coded,tt_coded1, tt_coded2, D1, D2, n, tt_coded_omitted)
      }
    })
    gc()
  }
  
  # Print diagnostic message. Complete procecesing data at system time.
  print(paste("Finished process for", date_p, "at", Sys.time()))
})


which(tt_ids$tt_id == '3B89')
