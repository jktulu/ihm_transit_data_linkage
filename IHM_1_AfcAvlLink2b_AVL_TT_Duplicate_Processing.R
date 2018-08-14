###

# The objective in this script is to assertain the correct avl_tt_id linkage
# where there exists two possible outcomes. Such outcomes occur where two or
# more services with the same service number and operator depart simultaneously.


# Some duplication can occur here due to the tiemtables containing both term 
# time and non-term time data In these cases it is necessary to manually select 
# the correct option.

# in the processed timetables is a column called 'case' this is either std, trm or hol 
# indicating a standard timetable, term only timetable or holiday timetable.

# Import the required libraries
library(RPostgreSQL)
library(dplyr)


# Establish a connection with the database.


#############################
## test4 Dates
dates <-  seq.Date(as.Date("2016-04-11"), as.Date("2016-04-11"), by = "day")
year_p = 2016; month_p = 'apr'
############################


# Set the Year and Month while you want to process.
year_p = "2016"
month_p = "apr"




# Based on the table created in the previous script identify any results where 
# there is possible duplication.
dbGetQuery(
  con,
  paste0(
    "create table avl_tt_lookup_", year_p, "_",  month_p, "_test4_duplicates 
     as select  * from  avl_tt_lookup_", year_p, "_", month_p, "_test4 where avl_id in 
    (select avl_id from  avl_tt_lookup_", year_p, "_", month_p, "_test4 
    group by avl_id having count(*) > 1)"
  )
)

# Index the duplicates table. This is an important performance step.
dbGetQuery(
  con,
  paste0(
    "create index on avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates (avl_id)"
  )
)

dbGetQuery(
  con,
  paste0(
    "create index on avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates (tt_id)"
  )
)


# This table would no longer be necessary if we create the combined net route 
# lookup tables as discussed in the original creation script. 

# dbGetQuery(
#   con,
#   paste0(
#     "create table net_route_lookup.net_route_lookup_comb_", year_p, "_", month_p,
#     " as select t2.id, t2.journey_scheduled, t3.geom, t3.n from
#     net_route_lookup.net_route_lookup_", year_p, "_", month_p, " t2  left join
#     net_route_lookup.net_route_lookup_", year_p, "_", month_p, "_lat_lons t3 
#     on t2.operator = t3.operator and t2.id = t3.id and t2.n=t3.n
#     where t2.operator in ('????','????') and t2.n=1"
#   )
# )

# dbGetQuery(
#   con,
#   paste0(
#     "create index on net_route_lookup.net_route_lookup_comb_", year_p, "_", month_p, " (id, geom)"
#   )
# )

# This table takes the avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates table and joins it to 
# a merged version of the net_route_lookup table created in the previous step. It calculates the time 
# between the journey being scheduled and the avl recording. ALso, the distance in metres between the 
# present location and the origin stop. It is believed that this will provie an indicator as to the 
# correct journey direction. 

dbGetQuery(
  con,
  paste0(
    "create table avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_filter as
    select t1.*,
    timestamp-(tt_date+t1.journey_scheduled) as interval_time,
    st_distance_sphere(st_point(t1.lon, t1.lat), t2.geom) as avl_tt_id_origin_dist
    from avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates t1 left join
    net_route_lookup.net_route_lookup_", year_p, "_", month_p, "2 t2 on 
    t1.tt_id = t2.id where t2.n=1"
  )
)

# Index the table to boost future query performance

dbGetQuery(
  con,
  paste0(
    "create index on avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_filter 
    (tt_date, route, journey_scheduled, operator, timestamp)"
  )
)

# In the first instance, it is necessary that we identify the unique combination
# of variables which will allow us to differentiate between each of the observed
# journeys. Here, we create a list of distinct tt_date, tt_id, fleet_no,
# journey_scheduled and operator.

duplicate_considerations <-
  dbGetQuery(
    con,
    paste0(
      "select distinct tt_date, operator, route, journey_scheduled 
       from avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_filter 
       where journey_scheduled is not null")
  ) 


######### Processing ##############


# In the subsquent section, the objective is to determine which of the fleet_no's 
# matches with each of the Timetable ids.

system.time({
  pb <-
    txtProgressBar(
      min = 1,
      max = nrow(duplicate_considerations),
      title = paste0("Processing ", nrow(duplicate_considerations), " records."),
      style = 3
    )
  
  for (j in 1:nrow(duplicate_considerations)) {
    setTxtProgressBar(pb, j)
    date.p <- duplicate_considerations$tt_date[j]
    operator.p <- duplicate_considerations$operator[j]
    route.p <- duplicate_considerations$route[j]
    journey_scheduled.p <- duplicate_considerations$journey_scheduled[j]
    
    values <-
      dbGetQuery(
        con,
        paste0(
          "select distinct * 
          from avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_filter 
          where tt_date = '", date.p, "' 
          and operator = '", operator.p,"' 
          and route = '", route.p,"' 
          and journey_scheduled = '", journey_scheduled.p,"' 
          and timestamp >= (tt_date+journey_scheduled) 
          order by timestamp, tt_id"
        )
      )
    
    if (nrow(values) == 0) {
      #print(paste0("fleet_no ", fleet_no$fleet_no[i], " assigned ", fleet_tt_ids[n,1]))
      dbWriteTable(
        con,
        name = paste0("avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_bad"),
        fleet_data[fleet_data$tt_id ==  fleet_tt_ids[n, 1], ] %>% 
        mutate(case = '1'),
        append = T, row.names = F
      )
      next()
    }
    
    
    # Identify the unique fleet_numbers associated with each set of values.
    fleet_no <- values %>% 
      select(fleet_no) %>% 
      distinct()
    
    # Loop through each fleet_no
    for (i in 1:nrow(fleet_no)) {
      ### operation for fleet_no 1
      
      # Create a subset of the values data based on the first fleet_no.
      fleet_data <-
        values %>% 
        filter(fleet_no == fleet_no$fleet_no[i])
      
      # From the fleet_data, select the unique timetable ids.
      fleet_tt_ids <- fleet_data %>% select(tt_id) %>% distinct
      
      # For each tt_id, further subset the data such that each table represents the data for one tt_id.
      fleet_data_a <- fleet_data[which(fleet_data$tt_id == fleet_tt_ids$tt_id[1]), ]
      fleet_data_b <- fleet_data[which(fleet_data$tt_id == fleet_tt_ids$tt_id[2]), ]
      fleet_data_c <- fleet_data[which(fleet_data$tt_id == fleet_tt_ids$tt_id[3]), ]
      fleet_data_d <-fleet_data[which(fleet_data$tt_id == fleet_tt_ids$tt_id[4]), ]
      
      # For each subset of fleet_data, calculate the difference between the
      # avl_id and believed journey origin (based on tt_id). Is the distance
      # increasing or decreasing.
      
      fleet_data_a_op <- sum(diff(fleet_data_a$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_a))]))
      fleet_data_b_op <- sum(diff(fleet_data_b$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_b))]))
      fleet_data_c_op <- sum(diff(fleet_data_c$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_c))]))
      fleet_data_d_op <- sum(diff(fleet_data_d$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_d))]))

      
      # For each subset of fleet_data, calculate the cumulative sum of distances
      # between the avl_id and believed journey origin. This provided a means to
      # differentiate between tt_ids which both have positive directions.
      
      fleet_data_a_dist <- sum(fleet_data_a$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_a))])
      fleet_data_b_dist <- sum(fleet_data_b$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_b))])
      fleet_data_c_dist <- sum(fleet_data_c$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_c))])
      fleet_data_d_dist <- sum(fleet_data_d$avl_tt_id_origin_dist[1:(.5*nrow(fleet_data_d))])
      
      # Create an array containing each of the distance differences in order.
      dis_dif <- c(fleet_data_a_op, fleet_data_b_op, fleet_data_c_op, fleet_data_d_op)
      
      # Create a array containing each of the cumulative distances in order.
      dists_cum <- c(fleet_data_a_dist, fleet_data_b_dist, fleet_data_c_dist, fleet_data_d_dist)
      
      # Identify which of the timetable ids are associated with the match
      # positive values.
      n <- which(na.omit(dis_dif) == dis_dif[which(dis_dif > 0)])
      
      if (length(n) == 1) {
        dbWriteTable(
          con,
          name = paste0(
            "avl_tt_lookup_",
            year_p,
            "_",
            month_p,
            "_test4_duplicates_good"
          ),
          fleet_data[fleet_data$tt_id ==  fleet_tt_ids[n, 1], ] %>% 
            mutate(case = '1'),
          append = T,
          row.names = F
        )
        
      } else if (length(n) > 1) {
        
        n_dist_cum <- which(min(dists_cum, na.rm = T) == na.omit(dists_cum))

        if (length(fleet_tt_ids[n_dist_cum, 1]) > 1) {
          
          
          #### 
          
          # select result based on highest numbr of stops. 
          
          ###
          
          
          
          
          dbWriteTable(
            con,
            name = paste0("avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_bad"
            ),
            fleet_data %>%
              mutate(case = '3'),
            append = T,
            row.names = F
          )

        } else {
          dbWriteTable(
            con,
            name = paste0("avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_good"
            ),
            fleet_data[fleet_data$tt_id ==  fleet_tt_ids[n_dist_cum, 1], ] %>%
              mutate(case = '2'),
            append = T,
            row.names = F
          )

        }
      }
    }
  }
  close(pb)
})


##########################

#create a subset of the bad data where conflicts are resolved based on the longest journey.








# Need to create a union between the unique avl_rows in the original table and
# the unique rows in the new table. The result is one table containing unique avl
# data with the matched timetable.

dbGetQuery(
  con,
  paste0(
    "
    create table avl_tt_lookup_", year_p, "_", month_p, "_test4_unique_all as
    select * from
    (select * from  avl_tt_lookup_", year_p, "_", month_p,"_test4
     where avl_id in 
    (select avl_id from  avl_tt_lookup_", year_p, "_", month_p, "_test4
    group by avl_id having count(*) = 1)) tn union all
    (select avl_id, timestamp, at_date, tt_version, tt_date, tt_id, tt_operator, 
    operator, route, fleet_no, t3.tt_direction, lon, lat, journey_scheduled::time without time zone
    from 
    avl_tt_lookup_", year_p, "_", month_p, "_test4_duplicates_good t3)
    "
  )
)

# Given the presense of non-numeric characters in the avl, here 
# we create a new column where the non numeric characters are dropped from the fleet number.

dbGetQuery(
  con,
  paste0(
    "alter table avl_tt_lookup_", year_p, "_", month_p, "_test4_unique_all 
    add column fleet_no_clean integer"
    )
  )

dbGetQuery(
  con,
  paste0(
    "update avl_tt_lookup_", year_p, "_", month_p, "_test4_unique_all 
      set fleet_no_clean = regexp_replace(fleet_no, '[^0-9]', '', 'g')::numeric"
  )
)

# Here we create the indexes required to optimise future data queries.

dbGetQuery(
  con,
  paste0(
    "create index on avl_tt_lookup_", year_p, "_", month_p, "_test4_unique_all 
    (tt_date, fleet_no_clean, timestamp)"
  )
)

dbGetQuery(
  con,
  paste0(
    "create index on avl_tt_lookup_", year_p, "_", month_p, "_test4_unique_all 
      (timestamp, tt_id, avl_id)"
  )
)
  