# Script 5: Journey Inference
#
# The objective of this script is to infer individuals probable journeys based
# on what is known of their travel behaviour. In particular, the approach is
# based on trip-training in which the destination of each journey is inferred
# based on the origin of the subsequent journey. For the last journey, the first
# journey is used as a proxy implying that an individual will return to the
# location at which they originally boarded.

# Import required libraries for data processing.
library(dplyr)
library(lubridate)
library(RPostgreSQL)

# Establish a connection with the PostgreSQL database.


# Increase the effective cach size of the database. This can improve overall
# performance of the query.
dbGetQuery(con, "set effective_cache_size to '128GB'")

# Base parameters to be used when partitioning the data for processing.
years = c("2015", "2016")
months = c("jan", "apr", "jul", "oct")

# Set out the sets of dates to be processed. The goal is to distribute the
# analysis across several processes such that performance is improved.

#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); year_p = 2015; month_p = "jul" # set 1a
#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); year_p = 2015; month_p = "jul" # set 1b
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  year_p = 2015; month_p = "oct" # set 2a
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  year_p = 2015; month_p = "oct" # set 2b
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); year_p = 2015; month_p = "jan" # set 3a
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); year_p = 2016; month_p = "jan" # set 3b
#dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-17"), by = "day"); year_p = 2016; month_p = "apr" # set 4a
#dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-17"), by = "day"); year_p = 2016; month_p = "apr" # set 4b
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); year_p = 2016; month_p = "jul" # set 5


dates <- seq.Date(as.Date("2016-07-18"),as.Date("2016-07-18"), by = "day");
year_p = 2016; month_p = "jul" # set 5


# Create an empty table to store the results of the journey analysis. 
dbGetQuery(con, paste0("
CREATE TABLE afc_avl_tt_journeys.full_user_journeys_",year_p,"_",month_p,"_test
(
  date date,
  journey_no integer,
  row_number bigint,
  record_date text,
  record_id integer,
  avl_case text,
  card_isrn text,
  transaction_datetime timestamp without time zone,
  arrive time without time zone,
  orig_naptan_code text,
  orig_n numeric,
  isam_op_code text,
  etm_service_number text,
  tt_id text,
  operator text,
  direction text,
  orig_geom geometry,
  dest_id text,
  dest_geom geometry,
  dest_n numeric,
  dest_naptan_code text,
  st_makeline geometry,
  dest_arrive time without time zone,
  \"timestamp\" timestamp with time zone,
  time_diff_tt_avl double precision,
  dwell_time interval,
  journey_duration interval,
  make_route geometry
)
WITH (
  OIDS=FALSE
);
ALTER TABLE afc_avl_tt_journeys.full_user_journeys_",year_p,"_",month_p,"date_p
OWNER TO \"cdrc-017-users\";
"))

system.time({  

for (j in 1:length(dates)) {
  # Set date parameter to be used in subsequent processing.
  date_p <- dates[j] 
  
  # Print status message to inform user on overall progress.
  print(paste("Starting process for", date_p, "at", Sys.time()))
  
  
  date_plus_1_p  <- as.character(format(ymd(as.Date(date_p)) + 1, "%Y-%m-%d"))
  
  # using date_p, calculate frequency of each card_isrn and store result.
  card_isrns <-
    dbGetQuery(con,
               paste0("select t2.account, t2.card_isrn, count(*) from afc t1
                      left join cch t2
                      using (card_isrn)
                      where transaction_datetime
                      between '",date_p," 04:00:00' and '",date_plus_1_p," 03:59:59'
                      and isam_op_code like 'NX%' and type = 'Over 60 Concession'
                      group by t2.account, t2.card_isrn order by count(*) desc
                      "
               ))
  
  # Create a progress bar which reports estimated time to completion (dplyr).
  p <- progress_estimated(nrow(card_isrns))
  
  # For each day, using card_isrns data, infer individual users journeys 
  # based on trip-chaining logic.
  for (i in 1:nrow(card_isrns)) {
    
    # Update progress bar
    p$tick()$print()
    
    # Set parameters for loop. 
    card_isrn <- card_isrns$card_isrn[i] 
    nRows <- card_isrns$count[i]
    
    # insert the query results into that table.
    dbGetQuery(con,
               paste0("
                      insert into afc_avl_tt_journeys.full_user_journeys_",year_p,"_",month_p,"
	                    
                      select '",date_p,"', ",i," as journey_no, t5.*, 
                      tt2.arrive as dest_arrive, tl.timestamp,
                      tl.time_diff_tt_avl,
                      lead(transaction_datetime) over()-timestamp as dwell_time,
                      timestamp-transaction_datetime as journey_duration,
	                    
                      -- create a geom object for the whole route.
		                  (select st_makeline(geom order by n) 
                        from 
                        net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t7 
                        where t7.operator = t5.operator and 
                        t7.id = t5.tt_id and
                        t7.n::integer between t5.orig_n and t5.dest_n 
                      ) as make_route
	                    
                      from 
	                    (
	                    select row_number() over(),
                      t4.record_date,
                      t4.record_id,
                      t4.avl_case_2,                      
                      t4.card_isrn,
                      t4.transaction_datetime,
                      t4.arrive,
                      t4.naptan_code as orig_naptan_code,
                      t4.n as orig_n,
                      t4.isam_op_code,
                      t4.etm_service_number,
                      t4.tt_id,
                      t4.operator,
                      t4.direction,
                      t4.geom as orig_geom,
                      t4.dest_id,
                      t4.dest_geom,
	                   

                       case when dest_geom is not null then
                      (select n as dest_n
                      from 
                      net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t5 
                      where t4.operator = t5.operator and 
                      t5.id = t4.tt_id and
                      t5.n > t4.n 
                      order by st_distance_sphere(t4.dest_geom,
                        t5.geom) asc
                        limit 1) else NULL end as dest_n,
	                   
 
                      case when dest_geom is not null then
                      (select naptan_code as dest_naptan_code
                      from 
                      net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t5 
                      where t4.operator = t5.operator and 
                      t5.id = t4.tt_id and
                      t5.n > t4.n 
                      order by st_distance_sphere(t4.dest_geom, t5.geom) asc
                      limit 1) else NULL END as dest_naptan,


		                  st_makeline(geom, dest_geom) 
		                  
                      from 
	                    
                      (
                      select t3.*,
                      case when t3.avl_case is null then '5' else t3.avl_case end as avl_case_2,
                      lead(t3.tt_id) over() as dest_id,
                      t2.geom as geom, 
                      lead(t2.geom) over() as dest_geom 


                      from
                      (
                      select * 
                      from 
                      (
					            
                      select
                      'a'::text as table_id, 
                      t1.transaction_datetime::date as record_date,
                      t1.isam_op_code,                       
                      t1.record_id,         
                      t1.card_isrn,         
                      t1.transaction_datetime,         
                      t2.naptan_code,         
                      t2.n,         
                      t2.time_diff_tt_avl,         
                      t1.etm_service_number,         
                      t2.tt_id,         
                      t2.direction,         
                      t2.arrive,
                      t2.tt_operator as operator,
                      t2.avl_case   
                      from afc t1 
                      left join
                        afc_avl_tt_link.afc_avl_tt_link_",gsub("-", "_", date_p),
                                        " t2
                      using(record_id)
                      where t1.card_isrn = '",card_isrn,"'
                      and
                      t1.transaction_datetime 
                      between '",date_p," 00:00:00' and '",date_p," 23:59:59'
                      order by t1.transaction_datetime) tx 
                      

                      union all (
                      select
                      'b'::text as table_id,
                      t1.transaction_datetime::date as record_date,
                      t1.isam_op_code,                       
                      t1.record_id,         
                      t1.card_isrn,         
                      t1.transaction_datetime,         
                      t2.naptan_code,         
                      t2.n,         
                      t2.time_diff_tt_avl,         
                      t1.etm_service_number,         
                      t2.tt_id,         
                      t2.direction,         
                      t2.arrive,
                      t2.tt_operator as operator,
                      t2.avl_case   
                      from afc t1 
                      left join
                      afc_avl_tt_link.afc_avl_tt_link_",gsub("-", "_", date_p)," t2
                      using(record_id)
                      where t1.card_isrn = '",card_isrn,"'
                      and
                      t1.transaction_datetime 
                      between '",date_p," 00:00:00' and '",date_p," 23:59:59'
                      order by t1.transaction_datetime 
                      limit 1
                      )  
	                    ) t3 
					            left join 
                      naptan_stops t2 
                      on 
                      t3.naptan_code=t2.naptan_code
                      where t3.table_id = 'a' 
			                ) t4 
                      ) t5 
	                    left join 
                      timetables.tt_", year_p, "_", month_p, "_processed tt2 
                      on t5.tt_id=tt2.id and
                      t5.dest_n=tt2.n 
                      left join avl_tt_linked_", year_p,
                                            "_", month_p, " tl 
                      on 
                      t5.tt_id=tl.id and 
                      t5.dest_n=tl.n and 
                      date = '",date_p,"'
                      order by row_number 
                      
                    "))
  }

}
})
  