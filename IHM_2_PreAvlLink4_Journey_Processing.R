# pre_avl_data_linkage_4_journey_processing.R
# 
# @Description
# 
# The objective in this script is to use the processed afc transactions data in 
# order to determine the probable journeys made by individuals. In additions, 
# efforts are made to impute the probable jounrey time based on the timetable
# and journey distance based on the distance between stops. 


# IT IS KEY THAT THE JOURNEY TIME TABLE IS INDEX CORRECTLY TO OMPTIMISE QUERY
# PERFORMANCE. Note, order is critical!
# journey_times_table (orig_naptan, dest_naptan, route, operator, 
#                       direction, dow, interval)
# TT (id, operator.)

# Import required libraries ----------------------------------------------------

require(RPostgreSQL)
require(dplyr)
require(lubridate)


# Establish Database Connection ------------------------------------------------

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '256GB'")


# Set parameters for data processing -------------------------------------------


# The datas object contains a vector of all data to be processed. 
#dates <- seq.Date(as.Date("2015-04-05"), as.Date("2015-04-05"), by = "day")
# The tt_version determines which timetable is used for the extraction of
# a plausible timetable id.

# 2015-Jul
#dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day"); 

# 2015-Oct
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  

# 2016-Jan
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); 

# 2016-Apr - Note square brackets to omit days where AVL data were unavailable.
#dates <-  seq.Date(as.Date("2016-04-11"), 
#as.Date("2016-07-17"), by = "day")[c(1:74,79:98)]; 

# 2016-Jul
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); 



dates <- seq.Date(as.Date("2016-07-19"), as.Date("2016-08-18"), by = "day")

# Specify which timetable should be used.
tt_version <- "2016_jul"



for (j in 1:length(dates)) {
  
  
  
  
  # Set database parameters ----------------------------------------------------
  
  date_p <- dates[j]
  
  case = db_has_table(con, paste0("pre_avl_afc_journeys.pre_avl_journeys_",
                                  gsub(pattern = "-", "_", date_p),"_rep4"))
  if (case == TRUE) next()
  
  
  print(paste0("starting process for ", date_p, " at ", Sys.time()))
  
  candidates <- 
    dbGetQuery(con, 
               paste0("
                      select t2.account, t2.card_isrn, count(*) from afc t1 
                      left join cch t2 using (card_isrn) 
                      where t1.transaction_datetime
                      between '",date_p," 00:00:00' and '",date_p," 23:59:59' 
                      and t2.type = 'Over 60 Concession'
                      group by t2.account, t2.card_isrn order by count(*) desc"
               ))
  

  # Create database table to hold output ---------------------------------------
  
  dbGetQuery(con,paste0( 
    "CREATE TABLE pre_avl_afc_journeys.pre_avl_journeys_",
    gsub(pattern = "-", "_", date_p),"
    (
    row_number integer,
    journey_id integer,
    record_id text,
    account text,
    isam_op_code text,
    etm_service_number text,
    start_stage text,
    direction text,
    transaction_datetime timestamp without time zone,
    \"case\" text,
    naptan_code text,
    merge_case text,
    tt_operator text,
    id text,
    n double precision,
    tt_direction text,
    geom text,
    next_boarding text,
    dest_naptan_code text,
    dest_n double precision,
    trip_time integer,
    make_route text,
    journey_end_time timestamp without time zone,
    dwell_time interval,
    trip_distance numeric
    )
    WITH (
    OIDS=FALSE
    );
    ALTER TABLE pre_avl_afc_journeys.pre_avl_journeys_",
    gsub(pattern = "-", "_", date_p), "  
    OWNER TO \"cdrc-017-users\";
    "))
  
  
  
  
  
  # Perform journey inference iterations -----------------------------------------
  
  # For each candidate set of parameters the journey is inferred. In addition,
  # journey time is inferred based on the origin and destination of the journey
  
  
  if (strftime(date_p, format = "%w") == 0) {
    dow = 7 
  } else { 
    dow = strftime(date_p, format = "%w")
  } 
  
  date_p_sub <- gsub(pattern = "-", "_", date_p)
  direction_constraint <- "and t5.n > t3.n"
  
  #system.time({
  
  
  p <- progress_estimated(nrow(candidates))
  for (i in 1:nrow(candidates)) {
    p$tick()$print()
    dbGetQuery(
      con,
      paste0(
        "
        insert into 
        pre_avl_afc_journeys.
        pre_avl_journeys_", gsub(pattern = "-", "_", date_p), "_rep4
        select 
        *, 
        case when row_number != ", candidates$count[i]," then      
        lead(transaction_datetime,1) over() - journey_end_time 
        else NULL END as dwell_time,
        st_length(st_transform(make_route, 27700))
        from (
        select *,
        transaction_datetime + (interval '1 second' * floor(trip_time)) as 
        journey_end_time
        from
        (
        
        select 
        row_number() over(), *, 
        (select trip_time from 
        timetables.tt_",tt_version,"_journey_times_agg jt
        where
        orig_naptan = naptan_code and
        dest_naptan = dest_naptan_code and
        route = etm_service_number and 
        jt.direction = t10.direction and
        operator = t10.tt_operator and
        substring(dow, ",dow,", 1) = '1' order by trip_time desc limit 1) as trip_time,

        (select st_makeline(geom order by t5.n) from net_route_lookup_"
        ,tt_version,"_lat_lons t5      
        where t5.operator = t10.tt_operator and t5.id = t10.id and        
        t5.n between t10.n and t10.dest_n      
        ) as make_route

        from (
        select 
        *,
        case when next_boarding is not null then 
        (select 
        naptan_code 
        from 
        net_route_lookup_",tt_version,"_lat_lons t5 
        where
       t5.operator = t3.tt_operator and  
        t5.id = t3.id        
        and t5.n > t3.n        
        order by 
        st_distance_sphere(t3.next_boarding, t5.geom) asc      
        limit 1) else NULL end as dest_naptan_code,

        case when next_boarding is not null then 
        (select 
        n
        from 
        net_route_lookup_",tt_version,"_lat_lons t5
        where 
        t5.operator = t3.tt_operator and 
        t5.id = t3.id and 
        t5.n > t3.n      
        order by 
        st_distance_sphere(t3.next_boarding, t5.geom)        
        limit 1) else NULL end as dest_n

        from (
        
        select 
        *, 
        lead(t1.geom, 1) over() as next_boarding 

        from (
        select 
        * 
        from (
        select ",i," as journey_id, t1.record_id, '",candidates$account[i],"' as account, t1.isam_op_code, 
        t1.etm_service_number, t1.start_stage, t4.direction, 
        t1.transaction_datetime, t4.case, t4.naptan_code, t4.merge_case, 
        case when t1.isam_op_code = '????' then '????'
        else (case when substring(t1.isam_op_code,1,2) = '??' then '????' else
        NULL end) end as tt_operator, 
        t4.id, t4.n, t4.tt_direction, t3.geom
        from  
        afc t1 
        
        left join 
          
        pre_avl_afc_transactions.pre_avl_transactions_", date_p_sub," t4 
        using (record_id) 
        
        left join
        
        naptan_stops t3 using (naptan_code)
        where t1.card_isrn = '",candidates$card_isrn[i],"' and
        t1.transaction_datetime between 
        '",date_p," 00:00:00' and '",date_p," 23:59:59'        
        ) tz
        
        union all
        
        (
        select 0 as journey_id, t1.record_id, '",candidates$account[i],"' as account, t1.isam_op_code, 
        t1.etm_service_number, t1.start_stage, t4.direction, 
        t1.transaction_datetime, t4.case, t4.naptan_code, t4.merge_case, 
        case when t1.isam_op_code = '????' then '????'
        else 
        (case when substring(t1.isam_op_code,1,2) = '??' then '????' else
        NULL end) end as tt_operator, 
        t4.id, t4.n, t4.tt_direction, t3.geom 
        from  
        afc t1 

        left join 

        pre_avl_afc_transactions.pre_avl_transactions_", date_p_sub," t4 
        using (record_id)
        
        left join
        
        naptan_stops t3 using (naptan_code)
        where t1.card_isrn = '",candidates$card_isrn[i],"' 
        and t1.transaction_datetime         
        between '",date_p," 00:00:00' and '",date_p," 23:59:59'        
        order by t1.transaction_datetime limit 1 
        ) order by journey_id desc, transaction_datetime
        ) t1  where journey_id != 0
        order by journey_id desc, transaction_datetime
        ) t3
        ) t10
        ) t11
        ) t12"
        )
    )
  }
  #})  
  }

# Summary ----------------------------------------------------------------------

# At the conclusion of this script, a table of journeys should exist for each 
# day. 


