# Code for determination of journey times between stops by route and operator.
# Alistair Leak
# 04/10/17
#
# Object here is to work out the typical time it take to travel between two
# stops on any given bus route.
#
# The approach to be used is based on iterating through all possible timetables
# ids and subsequently all possible combinations of stops such that summary
# journey times may be calcualted.

# Establish a connection with the database.
con <-
  dbConnect(
    drv = dbDriver("PostgreSQL"),
    dbname = "",
    host = "",
    user = "",
    password = ""
  )

# Set the parameters based on the name of the timetable in the database.
year.p <- "2014"
month.p <- "oct"

# Candidates are unique pairs of operators and ids. This can be used to extract
# each timetabled jouney in order.
candidates <-
  dbGetQuery(con, paste0(
             "select distinct operator, id from timetables.
             tt_",
             year.p,
             "_",
             month.p,
             ""))


# Create an empty table in the database "timetables" schema for the relevant
# journey times to be recorded. Note the table ownership is updated.

dbGetQuery(
  con,
  paste0(
    "
    CREATE TABLE timetables.tt_",
    year.p,
    "_",
    month.p,
    "_journey_times
    (
    operator text,
    id text,
    i numeric,
    j numeric,
    route text,
    direction text,
    orig_naptan text,
    dest_naptan text,
    orig_arrive text,
    dest_arrive text,
    interval numeric
    )
    WITH (
    OIDS=FALSE
    );
    ALTER TABLE timetables.tt_",
    year.p,
    "_",
    month.p,
    "_journey_times
    OWNER TO \"cdrc-017-users\";
    "
  )
)

# May be necessary truncate table during testing. If so, the following code may
# be employed.

dbGetQuery(con, paste0(
"truncate table timetables.tt_",year.p,"_",month.p,"_journey_times")
)

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
# 
# The ouput from each first tier loop is recorded in the results list object and
# then written to the database.

# During trials, performance approx 1000 took 350 seconds.




system.time({
  for (h in 1:nrow(candidates)) {
    tt_data <-
      dbGetQuery(
        con,
        paste0(
          "select operator, id, route, direction, naptan_code,
          arrive from timetables.
          tt_",
          year.p,
          "_",
          month.p,
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
    results <- list()
    
    for (i in 1:(nrow(tt_data) - 1)) {
      for (j in i:(nrow(tt_data))) {
        results[[length(results) + 1]] = c(
          tt_data$operator[1],
          tt_data$id[1],
          i,
          j,
          tt_data$route[1],
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
    dbWriteTable(
      con,
      c(
        "timetables",
        paste0("tt_", year.p, "_", month.p, "_journey_times")
      ),
      as.data.frame(do.call("rbind", results)),
      row.names = F,
      append = T
    )
  }
})

# Once complete, index should be created to optimize query speed.

dbGetQuery(con, 
           paste0("create index on timetables.tt_", year.p, "_", month.p, 
                  "_journey_times (operator, route, direction, orig_naptan, 
                  dest_naptan)"))
				  
				  
dbGetQuery(con, 
           paste0("create table timetables.tt_", year.p, "_", month.p,"_journey_times_agg as select orig_naptan, dest_naptan, route, direction, operator, dow, round(avg(interval)) as trip_time 
from timetables.tt_", year.p, "_", month.p,"_journey_times where interval > 0 group by orig_naptan, dest_naptan, route, direction, operator, dow;")

dbGetQuery(con, 
           paste0("create index on timetables.tt_", year.p, "_", month.p,"_journey_times_agg (orig_naptan, dest_naptan, route, direction, operator, dow text_pattern_ops, interval);")		  
