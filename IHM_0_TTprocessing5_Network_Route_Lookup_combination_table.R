######### Create the net_route_lookup and associated lat lon tables. ###########

# This script creates two database tables for each timetable period which are
# used at regular stages throughout the subsequent data linkage. In hindsight, 
# these could be combined into a single table. 

# Rather than create the lat, lons table, the lat, lon and geom columns could be 
# addeed to the original net route lookup table. The code required to do this is
# at the end of the script. 

# Import the required libraries
library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(lubridate)
library(geosphere)

# Establish a connection with the database.
con <- dbConnect(drv = dbDriver("PostgreSQL"), 
                 dbname = "", 
                 host = "", 
                 user = "", 
                 password = "")

year_p <- '2016'
month_p <- 'apr'


dbGetQuery(con, "create schema net_route_lookup with owner \"cdrc-017-users")

# For each combination of year and month (corresponding with timetable validity 
# periods) create the net_route_lookup table	  (time: could take an hour) 
system.time({
dbGetQuery(con, 
           paste0("
          create table net_route_lookup.net_route_lookup_",year_p,"_",month_p,"2 as
                       select 
                       t3.*, 
                       case when direction = 'O' then concat(origin_stop,' to ',
                       destination_stop) else concat(destination_stop,' to ',origin_stop) end 
                       as std_route_stop_to_stop_out, t4.stop_lon, t4.stop_lat, t4.geom
                       from
                       (select
                       t1.id,
                       t1.operator,
                       t1.naptan_code,
                       t1.service_no,
                       t1.direction,
                       t1.journey_scheduled,
                       t1.arrive,
                       t1.depart,
                       t1.origin, 
                       t1.n,
                       t1.destination,
                       substring(t1.origin,1,9) as origin_stop, 
                       substring(t1.destination,1,9) as destination_stop
                       from 
                       (
                       select 
                       (select naptan_code from timetables.tt_",year_p,"_",month_p," s2 
                       where s1.id=s2.id and type = 'QO' and s1.operator=s2.operator) as origin, 
                       (select naptan_code from timetables.tt_",year_p,"_",month_p," s2 
                       where s1.id=s2.id and type = 'QT' and s1.operator=s2.operator) as destination, 
                       naptan_code,
                       route as service_no,
                       direction,
                       journey_scheduled,
                       operator,
                       n,
                       arrive,
                       depart,
                       id
                       from 
                       timetables.tt_",year_p,"_",month_p," s1) t1 ) t3
                       left join naptan_stops t4 on t3.naptan_code = t4.naptan_code 
                       order by
                       id, n;
                      ALTER TABLE net_route_lookup_",year_p,"_",month_p,"2 OWNER TO \"cdrc-017-users\";")) 


# Create the four indexes required in the subsequent analysis to boost performance.
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (id, n, geom);"))
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (operator, n);"))
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (naptan_code);"))
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (operator, service_no, id, naptan_code);"))
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (service_no);"))
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (geom)"))			
dbSendQuery(con, paste0("create index on net_route_lookup_",year_p,"_",month_p,"2 (id, naptan_code, operator)"))			


})
