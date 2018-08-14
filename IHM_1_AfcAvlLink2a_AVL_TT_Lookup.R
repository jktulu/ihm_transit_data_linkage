######### Read in data from afc and avl ###########

# Script 2-a of 4.
# This script links the avl and timetable data. It provides a means to measure
# delay at each bus stop. It is used later to determine the delay experience by
# passengers. (systematic delay)

## This can subsequently be used to match the afc and avl data.


# Peformance appears to be 10 - 15 minutes per day of data.

#######################################################

## Raw Data Required ##
#
# 1. AVL - Vehicle automated locatons
# 2. CCH - Customer details (Over-60 Concessionary travellers)
# 3. Asset Tracker - to determine bus operator
# 4. Timetable data for the appropriate period

######################################################

# Import the required libraries
library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(lubridate)
library(geosphere)

# Establish a connection with the database.


years = c("2015", "2016")
months = c("jan", "apr", "jul", "oct")

# Set 1
dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day")
random = "h"; year_p = years[1]; month_p = months[3]

# Set 2
dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day")
random = "c"; year_p = years[1]; month_p = months[4]

# Set 3
dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day") ; 
random = "d"; year_p = years[2]; month_p = months[1]

# Set 4
dates <- seq.Date(as.Date("2016-04-11"), as.Date("2016-07-17"), by = "day")[c(1:74,79:98)] 
random = "e"; year_p = years[2]; month_p = months[2]

# Set 5
dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day")
random = "a"; year_p = years[2]; month_p = months[3]

## test5 Dates
dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-17"), by = "day")
random = "a"; year_p = 2016; month_p = 'apr'


dbGetQuery(con, 
           paste0("
                  CREATE TABLE 
                  avl_tt_lookup_", year_p, "_", month_p, "_test5
                  (
                  avl_id integer,
                  \"timestamp\" timestamp without time zone,
                  at_date text,
                  tt_version text,
                  tt_date date,
                  tt_id text,
                  tt_operator text,
                  operator text,
                  route text,
                  fleet_no text,
                  tt_direction text,
                  lon numeric,
                  lat numeric,
                  journey_scheduled time without time zone
                  )
                  WITH (
                  OIDS=FALSE
                  );
                  ALTER TABLE 
                  avl_tt_lookup_", year_p, "_", month_p, "_test5
                  OWNER TO \"cdrc-017-users\";"
                  )
           )

# Determine the last arrival time for any journey on the day of interest. We want 
# ensure that we have all gps data timetabled journeys originating on the date 
# being processed.
last_scheduled_arrival <- 
  dbGetQuery(con, 
             paste0("select arrive from timetables.tt_",
                    year_p,"_",month_p,"_processed 
                    where 
                    type = 'QT' and 
                    operator in ('????', '????') and 
                    journey_scheduled >= '23:00:00' and 
                    depart <= '06:00:00' order by arrive desc limit 1"))[1,1]


# Edit the last scheduled time variable to include a further 30 minutes.
last_scheduled_arrival <- format(strptime(last_scheduled_arrival,
                                           format = "%H:%M:%S") + 1800,
                                  "%H:%M:%S")

# Identify all routes operated by the main operator groups. # This is specific to 
# the main transport provider.
routes <- 
  dbGetQuery(con, paste0("select distinct route from timetables.tt_",
                         year_p,"_",month_p,"_processed where operator 
                         in ('????', '????')"))


system.time({
for (j in 1:length(dates)) {
  # Set date as the first date of the unique dates specified
  date_p = dates[j]
  
  
  
  
  ############# Holiday or termtime ##############
  
  # we need a lookup between the data being processed and a list of term/holiday 
  # dates. Using term will tend to put individuals on an apparently slower route. 
  
  hol_or_trm <- 'trm'
  
  ################################################
  
  print(paste0("starting ", date_p, " at ", Sys.time()))
  
  # Create a new variable containing the next date
  date_plus_1_p  <- as.character(format(ymd(as.Date(date_p)) + 1, "%Y-%m-%d"))
  
  # what day of the week does the date correspond to? 1 = Mon, 7 = Sun. Note, 
  # in postgresql, Sunday is 0.
  if (strftime(date_p, format = "%w") == 0) {
    dow = 7 
  } else { 
    dow = strftime(date_p, format = "%w")
  } 
  

#  if asset_tracker_date_lookup does not exist, it may be necessary to recreate the asset tracker date
#  lookup table as below.
  
  # dbGetQuery(con, "create table public.asset_tracker_date_lookup as select distinct 
  #            date from asset_tracker_final order by date desc") 

  
  # Based on the date being processed, identify the most recent asset tracker
  # data.
  ATdate_p <- 
    as.character(
      dbGetQuery(con, 
                 paste0("select date from asset_tracker_date_lookup 
                        where date <= '", date_p, "' 
                        order by date desc limit 1"))[1,1])
  
      # initiate the progress bar. This will give an indicator of expected run time.
    p <- progress_estimated(nrow(routes), min_time = 0)
    
    for (i in c(1:nrow(routes))) {
      route <- routes$route[i]
      
      #print(paste(i,"Starting route", route, "at", Sys.time()))
      p$tick()$print()
      # The first table is designed to identify scheduled journeys in the avl
      # dataset in which only one operator is in operation. This means that
      # there is no ambiguity between who the operator is for that service.
      
      # Previously this query led to duplicates being added to the data. This
      # happened when joining each avl record to the asset tracker. There are
      # apparent duplicated in the asset tracker where a bus may be recorded twice
      # with different batch numbers.
      
      dbGetQuery(con, 
                 paste0("
                  create or replace View 
                  temp", random, "_bus_operator_lookup_", year_p, "_", month_p, " as 
                  -- find unique operator scheduled journey times
                    select journey_scheduled, operator from 
                    (
                    
                    select distinct t1.fleet_no, t1.timestamp::date, t1.journey_no, t1.journey_scheduled, t2.operator 
                    from avl t1 
                    -- asset tracker final was the source of duplicates
                    left join asset_tracker_final t2 
                    on regexp_replace(t1.fleet_no, '[^0-9]', '', 'g')::integer =t2.bus 
                    where t1.timestamp between '",date_p," 00:00:00' and '",date_plus_1_p," ", last_scheduled_arrival, "' 
                      and t1.public_service_code = '", route, "' 
                      and t2.date = '",ATdate_p,"' 
                      and t1.timestamp::date = '",date_p,"'::date 
                    
                    
                  ) t5
                  -- second part is looking for distinct scheduled journey times in the AVL.
                  where journey_scheduled in 
                    (
                    select t6.journey_scheduled 
                    from 
                      (
                        select distinct t1.fleet_no, t1.timestamp::date, t1.journey_no, t1.journey_scheduled, t2.operator
                        from avl t1 
                        left join asset_tracker_final t2 on regexp_replace(t1.fleet_no, '[^0-9]', '', 'g')::integer =t2.bus 
                        where t1.timestamp between '",date_p," 00:00:00' and '",date_plus_1_p," ", last_scheduled_arrival, "' 
                          and t1.public_service_code = '", route, "' 
                          and t2.date = '",ATdate_p,"' 
                          and t1.timestamp::date = '",date_p,"'::date 
                        
                    ) t6 
                    group by journey_scheduled having count(*) = 1
                    );
                  "))
      
      # This query is similar to the above. It identifies journeys in the
      # timetable which only occur once.The end results of this query is a
      # table containing journey ids from the timetable, the route name,
      # operator and journey_scheduled time.
      
      dbGetQuery(con, 
        paste0("
              create or replace view temp", random, "_bus_tt_operator_link_", year_p, "_", month_p, " as
                select * from (
                  select  * from (
                    select distinct t9.type, t9.id, t9.ttroute, t9.operator2 as tt_operator, t10.operator, journey_scheduled, direction 
                    from (
                      select distinct t1.type, t1.operator as operator2, t1.start_date, t1.route as ttroute,
                        t1.journey_scheduled, t1.id, t1.direction, t1.naptan_code, t2.origin, t1.arrive, t1.depart, t1.dow, 
                        t1.timing_point, t2.std_route_stop_to_stop_out 
                      from timetables.tt_", year_p, "_", month_p, "_processed t1 
                      left join net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 
                      on t1.naptan_code=t2.naptan_code 
                        and t1.id=t2.id 
                        and t1.operator= t2.operator 
                        where t1.operator in ('????', '????') 
                        and t1.\"case\" in ('std', '", hol_or_trm,"')
                        and t1.route = '", route, "' 
                        and substring(t1.dow,", dow, ",1) = '1' 
                        ) t9 
                      left join
                        (

                        -- create a lookup between route, origin and avl operator. 
                        select distinct route, origin, operator 
                          from (
                          select t7.*, t8.* from 
                            (
                            select t5.route, t5.journey_scheduled, t5.origin 
                            from ( 
                                select distinct t1.route, t1.journey_scheduled, t1.id,t2.origin
                                from timetables.tt_", year_p, "_", month_p, "_processed t1 
                                left join net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 
                                on 
                                  t1.naptan_code=t2.naptan_code 
                                  and t1.id=t2.id 
                                  and t1.operator= t2.operator 
                                where
                                  t1.operator in ('????', '????')
                                  and t1.\"case\" in ('std', '", hol_or_trm,"')
                                  and route = '", route, "' 
                                  and substring(dow,", dow, ",1) = '1' 
                                  and type = 'QO') t5 
                                where t5.journey_scheduled in (
                                  select t5.journey_scheduled from
                                    (
                                      select distinct t1.type, t1.operator, t1.start_date, t1.route, t1.journey_scheduled, t1.id, t1.direction,
                                        t1.naptan_code, t2.origin, t1.arrive, t1.depart, t1.dow, t1.timing_point, t2.std_route_stop_to_stop_out 
                                      from timetables.tt_", year_p, "_", month_p, "_processed t1 
                                      left join net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 
                                      on 
                                        t1.naptan_code=t2.naptan_code 
                                        and t1.id=t2.id 
                                        and t1.operator= t2.operator 
                                      where
                                        -- operator filter is based on what we know about the avl data.
                                        t1.operator in ('????', '????')
                                        and t1.\"case\" in ('std', '", hol_or_trm,"')
                                        and route = '", route, "' 
                                        and substring(dow,", dow, ",1) = '1' 
                                        and type = 'QO' 
                                      ) t5 
                                    group by t5.route, t5.journey_scheduled 
                                    having count(*) = 1 
                                    ) 
                                ) t7 
                              left join temp", random, "_bus_operator_lookup_", year_p, "_", month_p, " as t8 
                            on t7.journey_scheduled = t8.journey_scheduled 
                            
                          ) t9  
                        where operator is not null) t10 
                        on t9.origin=t10.origin
                          and t9.ttroute = t10.route
                        ) th 
                      union (
                        select distinct type, id, ttroute, operator2 as tt_operator, 
                          operator, journey_scheduled, direction 
                        from (
                          select distinct t1.type, t1.operator as operator2, t1.start_date, 
                            t1.route as ttroute, t1.journey_scheduled, t1.id, t1.direction, 
                            t1.naptan_code, t2.destination, t1.arrive, t1.depart, t1.dow, 
                            t1.timing_point, t2.std_route_stop_to_stop_out 
                          from timetables.tt_", year_p, "_", month_p, "_processed t1 
                          left join net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 
                          on 
                            t1.id=t2.id 
                            and t1.naptan_code=t2.naptan_code 
                            and t1.operator= t2.operator 
                          where t1.operator in ('????', '????')
                            and t1.\"case\" in ('std', '", hol_or_trm,"')
                            and route = '", route, "' 
                            and substring(dow,", dow, ",1) = '1' 
                          ) t9 
                        left join
                          (
                          -- create a lookup between route, destination and avl operator. 
                          select distinct route, destination, operator 
                            from (
                              select t7.*, t8.* 
                              from 
                                (
                                select t5.route, t5.journey_scheduled, t5.destination 
                                from (
                                  select distinct t1.route, t1.journey_scheduled, t1.id,t2.destination
                                  from timetables.tt_", year_p, "_", month_p,"_processed t1 
                                  left join net_route_lookup.net_route_lookup_", year_p, "_", month_p,"2 t2 
                                  on t1.naptan_code=t2.naptan_code 
                                    and t1.id=t2.id 
                                    and t1.operator= t2.operator 
                                  where t1.operator in ('????', '????')
                                    and t1.\"case\" in ('std', '", hol_or_trm,"')
                                    and route = '", route, "' 
                                    and substring(dow,", dow,",1) = '1' 
                                    and type = 'QT') t5 
                                where t5.journey_scheduled in (
                                  select t5.journey_scheduled from (
                                  select distinct t1.type, t1.operator, t1.start_date, t1.route, t1.journey_scheduled, t1.id, t1.direction,
                                  t1.naptan_code, t2.destination, t1.arrive, t1.depart, t1.dow, t1.timing_point, t2.std_route_stop_to_stop_out 
                                  from timetables.tt_", year_p, "_", month_p, "_processed t1 
                                  left join net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 
                                  on t1.naptan_code=t2.naptan_code 
                                    and t1.id=t2.id 
                                    and t1.operator= t2.operator
                                  where 
                                  t1.operator in ('????', '????')
                                    and t1.\"case\" in ('std', '", hol_or_trm,"')
                                    and route = '", route,"' 
                                    and substring(dow,", dow, ",1) = '1' 
                                    and type = 'QT' 
                                  ) t5 
                                group by t5.route, t5.journey_scheduled 
                                having count(*) = 1 ) 
                                ) t7 
                            left join temp", random, "_bus_operator_lookup_", year_p, "_", month_p, " t8 
                            on t7.journey_scheduled= t8.journey_scheduled 
                            ) t9
                            where operator is not null) t10 
                            on t9.destination=t10.destination
                              and t9.ttroute = t10.route
                            )
                ) t
              where 
              operator is not null
            ")
        )
      
      # This table combines the database views to create an avl table for which each
      # record is appended with the timetable journey id. With this information,
      # we are in the perfect position to measure delay period at each bus stop!
      # For the period where the avl data exist.
      
      dbGetQuery(con, 
                 paste0("
                        create or replace view 
                          temp", random, "_bus_linked_data_", year_p, "_", month_p, " as
                        select t3.*, t4.direction, t4.tt_operator, t4.id, st_point(t3.lon,t3.lat) as geom 
                        from (
                          select distinct t1.*, t2.operator 
                            from (
                              select * 
                              from 
                              avl 
                              where timestamp between '", date_p, " 00:00:00' 
                                and '", date_plus_1_p, " ", last_scheduled_arrival, "' 
                                and public_service_code = '", route, "'
                            ) t1 
                            left join asset_tracker_final t2
                            on regexp_replace(t1.fleet_no, '[^0-9]', '', 'g')::integer = t2.bus 
                            where t1.journey_no > 0 and t2.date = '", ATdate_p,"' 
                              and t1.timestamp between '", date_p, " 00:00:00' 
                              and '", date_plus_1_p, " ", last_scheduled_arrival, "'
                            ) t3 
                          left join (
                            select distinct id, ttroute, tt_operator, operator, journey_scheduled, direction 
                            from 
                            temp", random, "_bus_tt_operator_link_", year_p,"_",month_p,") t4 
                          on t3.operator=t4.operator 
                            and t3.public_service_code = t4.ttroute
                            and t3.journey_scheduled = t4.journey_scheduled
                          ")
                 )
      
      # Insert the resultant data into the avl_tt_lookup_", year_p, "_", month_p, "
      # table
      dbSendQuery(con, 
                  paste0("insert into avl_tt_lookup_", year_p, "_", month_p, "_test5 
                          select _id as avl_id, timestamp, '",ATdate_p,"' as at_date,
                            'tt_", year_p, "_", month_p, "' as tt_version,
                            '",date_p,"' as tt_date, id as tt_id, 
                            tt_operator, operator, public_service_code, fleet_no, 
                            direction, lon, lat, journey_scheduled
                          from 
                            temp", random, "_bus_linked_data_", year_p, "_", month_p, " 
                          where timestamp-('", date_p,"'::date+journey_scheduled) 
                            between '-12:00:00' and '12:00:00'
                          "
                         )
                  )
    }
}
})

# Once all of the data have been processed, the
dbSendQuery(con, paste0("create index on avl_tt_lookup_", year_p, "_", month_p,"_test5 (avl_id)"))





