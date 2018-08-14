## Join AFC and TT.

# Script 4 of 4.

# Requires previous creation of avl_tt_linked_year_month table
#								afc_avl_lookup

# Import the required libraries
library(dplyr)
library(DBI)
library(lubridate)
library(stringr)

# Establish the database connection

dbGetQuery(con, "set effective_cache_size to '64GB'")
dbSendQuery(con, "set enable_hashjoin=off;")


#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); year_p = 2015; month_p = "jul" # set 1a
#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); year_p = 2015; month_p = "jul" # set 1b
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  year_p = 2015; month_p = "oct" # set 2a
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  year_p = 2015; month_p = "oct" # set 2b
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); year_p = 2015; month_p = "jan" # set 3a
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); year_p = 2016; month_p = "jan" # set 3b
#dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-17"), by = "day"); year_p = 2016; month_p = "apr" # set 4a
#dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-17"), by = "day"); year_p = 2016; month_p = "apr" # set 4b
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); year_p = 2016; month_p = "jul" # set 5

system.time({
  for (i in 1:length(dates)) {
    date_p = dates[i]
    
    print(paste("Starting process for", date_p, "at", Sys.time()))
    
    dbGetQuery(con,
      paste0("
		create table afc_avl_tt_link.afc_avl_tt_link_",gsub(pattern = "-", "_", date_p),"_test4 as
    select t6.*, t7.route, t7.date, t7.arrive, t7.timestamp, t7.time_diff_tt_avl, 
    t7.\"case\" as tt_case, t7.direction, t7.n as tt_n, t7.type         
    from (         
    select * , 
    
    -- find the naptan code of the nearest bus stop based on the timetable.
    (select naptan_code
    from net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t6
    where t6.id = t5.tt_id and t6.operator = t5.tt_operator
    order by st_distance_sphere(geom, t1_geom) asc
    limit 1),
    
    -- find the stop number for the given journey
    (select n
    from net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t6
    where t6.id = t5.tt_id and t6.operator = t5.tt_operator
    order by st_distance_sphere(geom, t1_geom) asc
    limit 1),
    
    (select distinct st_distance_sphere(geom, t1_geom) as distance
    from net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t6
    where t6.id = t5.tt_id and t6.operator = t5.tt_operator
    order by st_distance_sphere(geom, t1_geom) asc
    limit 1)



    from (

    select t1.\"case\" as avl_case, t1.record_id, t1.fleet_no_clean, 
    t2.sealer_id, t1.geom as t1_geom, t2.etm_service_number, 
    t2.transaction_datetime, t2.card_isrn,
    
    (select tt_id
    from avl_tt_lookup_", year_p, "_", month_p, "_unique_all t3
    where t3.tt_date = '", date_p, "' 
    and t3.fleet_no=t1.fleet_no_clean::text 
    and timestamp <= transaction_datetime
    order by timestamp desc
    limit 1)
    as tt_id,
		
    (select tt_operator
    from avl_tt_lookup_", year_p, "_", month_p, "_unique_all t3
    where t3.tt_date = '", date_p, "' 
    and t3.fleet_no=t1.fleet_no_clean::text 
    and timestamp <= transaction_datetime
    order by timestamp desc
    limit 1) 
    as tt_operator,
		
    (select operator
    from avl_tt_lookup_", year_p, "_", month_p, "_unique_all t3
    where t3.tt_date = '", date_p, "' 
    and t3.fleet_no_clean=t1.fleet_no_clean::text 
    and timestamp <= transaction_datetime
    order by timestamp desc
    limit 1) 
    as operator

    from
    afc_avl_lookup t1
    left join afc t2
    on t1.record_id=t2.record_id 
    where t2.transaction_datetime between '", date_p," 00:00:00' 
    and '", date_p, " 23:59:59' 
    and t1.\"case\" is not null 
    and t1.\"case\" != '4'
    ) t5
    ) t6
    -- Join the final table back to the original timetable.
    left join avl_tt_linked_", year_p, "_", month_p, "_test t7 
    on 
    t6.n=t7.n 
    and t6.tt_id=t7.id 
    and t7.date = '", date_p, "'"
      ))
    }
  })






