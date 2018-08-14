# Import the required libraries
library(RPostgreSQL)
library(dplyr)
library(DBI)
library(purrr)

# Establish a connection with the database.
con <- dbConnect(drv = dbDriver("PostgreSQL"), 
                 dbname = "", 
                 host = "", 
                 user = "", 
                 password = "")



# in sql: 'create table avl_unique_fleet_no as select fleet_no from avl group by fleet_no'
# (took 9 min), now get the new table:
unique_fleet_no <- dbGetQuery(con, "select fleet_no from avl_unique_fleet_no order by fleet_no")
unique_fleet_no <- unique_fleet_no$fleet_no


# Create an new schema
# dbGetQuery(con, "create schema avl_enhanced_fleet")


# loop: for each fleet number, infer service

for (j in 1:length(unique_fleet_no)) {
  
  fleet_no = unique_fleet_no[j]
  print(paste0(j, " at ", Sys.time(),  " processing ", fleet_no))
  
  dbGetQuery(con, paste0(
    "create table avl_enhanced_fleet.avl_enhanced_",fleet_no," (
  avl_id integer,
  fleet_no text,
  timestamp  timestamp without time zone,
  public_service_code text,
  journey_scheduled  time without time zone
  )
WITH (
  OIDS=FALSE
);
ALTER TABLE 
avl_processed
OWNER TO \"cdrc-017-users\";
"))
  
  
  
  query <- paste0("select _id as avl_id, timestamp, fleet_no, public_service_code, journey_scheduled, 
case when timestamp-lag(timestamp, 1) over() between '00:00:00' and '00:15:00' or
timestamp-lead(timestamp, 1) over() between '00:00:00' and '00:15:00' then TRUE else FALSE end as link_case
from avl where fleet_no = '",fleet_no,"'")
  
  # Extract the data from the database
  system.time(
  bus_data <- dbGetQuery(con, query)
)

 
# Determine journey number
  bus_data$journey_count <- cumsum(!bus_data$link_case)

  # function to fill service number and schedule forwards
  fill_table <- function(i) {
    bus_data[bus_data$journey_count == i,] %>% 
      mutate(service_inferred = zoo::na.locf(public_service_code, na.rm=FALSE)) %>%
      mutate(schedule_inferred = zoo::na.locf(journey_scheduled, na.rm=FALSE)) 
  }
  
  # apply fill table function based on journey_count

  bus_data <- map_df(unique(bus_data$journey_count), fill_table) 


  # Percentage of observations with service number
  print(paste0("Before: ", round((1-mean(is.na(bus_data$public_service_code)))*100,2), "% complete."))
  print(paste0("After: ", round((1-mean(is.na(bus_data$service_inferred)))*100,2), "% complete."))
  
 
 # Write the results back to the database
  bus_data %>% 
    select(avl_id, fleet_no, timestamp, public_service_code = service_inferred, journey_scheduled = schedule_inferred) %>%
    dbWriteTable(con, c("avl_enhanced_fleet", paste0("avl_enhanced_", fleet_no)), value = ., row.names = F, append = T)
}


# load into one single table

# create new table

# for tab in tables{
#   insert tab into new table
# }













