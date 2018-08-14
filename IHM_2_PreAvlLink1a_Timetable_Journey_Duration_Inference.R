# pre_avl_data_linkage_a_timetable_journey_duration_inference.R
# 
# @Description
# 
# The objective in this script is to record the time interval between all pairs
# of stops for all routes. The resutlant data are stored in a disaggregate 
# manner such that queries may be made on the basic of time of day and day of 
# week. Consequently, a good estimate of journey duration should be obtainable.


# Establish Database Connection ------------------------------------------------


dbGetQuery(con, "set effective_cache_size = '128GB'")

# Specify scrip parameters -----------------------------------------------------

year_p <- "2015"
month_p <- "jan"


# Create database table to hold output ---------------------------------------

dbGetQuery(
  con,
  paste0(
    "
    CREATE TABLE timetables.tt_",
    year_p,
    "_",
    month_p,
    "_journey_times
    (
    operator text,
    id text,
    i numeric,
    j numeric,
    route text,
    dow text,
    direction text,
    orig_naptan text,
    dest_naptan text,
    orig_arrive text,
    dest_arrive text,
    interval text
    )
    WITH (
    OIDS=FALSE
    );
    ALTER TABLE timetables.tt_",
    year_p,
    "_",
    month_p,
    "_journey_times
    OWNER TO \"cdrc-017-users\";
    "
  )
)


# Create iterator for timetable ------------------------------------------------

candidates <-
  dbGetQuery(con, paste0(
             "select distinct operator, id from timetables.tt_", year_p, "_", month_p))


# The first tier of the loop iterates through the operator-id candidates. In
# each case, the journey data are extracted from the database using the
# operator-id criterion.
# 
# The second tier increments through each of the possible start locations. I.e,
# the first iteration starts at stop 1, the second iteration starts from stop 2
# and so on.
# 
# The third tier increments through each of the possible stops beyond the
# starting stop. In each case, the operator, timetable id, origin, destination,
# origin-arrival, destination-arrival and the interval between the origin and
# destination times is recorded.

# The ouput from each first tier loop is recorded in the results list object and
# then written to the database.


# Iterate through timetable ----------------------------------------------------

system.time({
  for (h in 1:nrow(candidates)) {
    tt_data <-
      dbGetQuery(
        con,
        paste0(
          "select operator, id, route, dow, direction, naptan_code,
          arrive from timetables.
          tt_",
          year_p,
          "_",
          month_p,
          " where
          id = '",
          candidates$id[h],
          "' and
          operator = '",
          candidates$operator[h],
          "'
          order by n;"
        )
        )

    # Empty list used to store the results.
    results <- list()
    
    for (i in 1:(nrow(tt_data) - 1)) {
      for (j in i:(nrow(tt_data))) {
        results[[length(results) + 1]] = c(
          tt_data$operator[1],
          tt_data$id[1],
          i,
          j,
          tt_data$route[1],
          tt_data$dow[1],
          tt_data$direction[1],
          tt_data$naptan_code[i],
          tt_data$naptan_code[j],
          as.character(tt_data$arrive[i]),
          tt_data$arrive[j],
          as.numeric(difftime(
            strptime(tt_data$arrive[j], format = "%H:%M:%S"),
            strptime(tt_data$arrive[i], format = "%H:%M:%S"),
            units = "secs"))
        )
      }
    }
    
    # ifelse statement used to address tt arival time spanning multiple days.
    # Where the duration <0 seconds, indicating the day has changed, 86400, 
    # the number of seconds in a day is added.
    
    results %>% 
      do.call("rbind", .) %>% 
      data.frame(stringsAsFactors = F) %>%
      mutate(X12 = as.numeric(X12), 
             X12 = ifelse(X12 < 0, X12 + 86400, X12)) %>%
      dbWriteTable(
      con,
      c("timetables", paste0("tt_", year_p, "_", month_p, "_journey_times")),
      . ,
      row.names = F,
      append = T
    )
    rm(results)
  }
})


# Optimise output database table for queries -----------------------------------

dbGetQuery(con, 
           paste0("create index on timetables.tt_", year_p, "_", month_p, 
                  "_journey_times (orig_naptan, dest_naptan, route, operator, direction, dow)"))




