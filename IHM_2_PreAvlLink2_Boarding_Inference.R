# pre_avl_data_linakge_2_database_setup.R
# 
# @Description
# 
# In the previous script (pre_avl_data_linakge_1_database_setup.R), the goal was 
# to establish the required database tables to enable queries to be made as to 
# the probable location at which individuals boarded busses. 
# 
# The script uses the previously consructed tables to match individuals to a 
# probable boarding location based on information stored within the AFC 
# transaction. In particular, Start_stage, isamn_op_code, direction of travel 
# and etm service number. Note, Direction of Travel is an imputed variable and 
# may not be correct.
# 
# The logic in the following code is as follows.
# 1) Where possible, users future journeys are used to determine probable 
# boarding locations based on the AFC transaction data.
# 2) where the outcome of the user-based data is tied, the general behaviour
# table is employed to break the tie.
# 3) Where no prior data are available for the user, the probable boarding 
# location is assigned on the general-based table.    

# Indexs which are required!
# 
# 
# 
# 
# 

# Import required libraries ----------------------------------------------------
require(RPostgreSQL)
require(dplyr)
require(lubridate)


# Establish Database Connection ------------------------------------------------

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")


# Set parameters for data processing -------------------------------------------

# The datas object contains a vector of all data to be processed. 
#dates <- seq.Date(as.Date("2015-04-05"), as.Date("2015-04-05"), by = "day")
# The tt_version determines which timetable is used for the extraction of
# a plausible timetable id.

# 2015-Jul
dates <- seq.Date(as.Date("2014-01-01"), as.Date("2014-01-31"), by = "day"); 

# 2015-Oct
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  

# 2016-Jan
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); 

# 2016-Apr - Note square brackets to omit days where AVL data were unavailable.
#dates <-  seq.Date(as.Date("2016-04-11"), 
#as.Date("2016-07-17"), by = "day")[c(1:74,79:98)]; 

# 2016-Jul
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); 


#dates <- "2015-10-08"
#dates <- seq.Date(as.Date("2015-10-12"), as.Date("2015-10-31"), by = "day")

system.time({
  for (i in 1:length(dates)) {
    
    # date parameter based on loop iteration.
    date_p <- dates[i]
    print(paste0("starting process for ", date_p, " at ", Sys.time()))
    
    # Import transactions ------------------------------------------------------
    
    # All transaction by National Express Service extracted from AFC and joined
    # to CCH for account numbers and Afc_direction of travel table for inferred 
    # direction.
     
    # index - Table: CCH - Columns: Card_isrn, type.
    afc_transactions <-
      dbGetQuery(
        con, paste0("
        select t1.record_id, t2.account, t1.isam_op_code, t1.etm_service_number, 
        t1.start_stage, t3.direction, t1.transaction_datetime 
        from afc t1 
        left join cch t2 using (card_isrn) 
        left join afc_direction_of_travel_2014_2016 t3
        using (record_id) 
        where t1.isam_op_code like 'NX%' and t2.account is not null and
        t1.transaction_datetime 
        between '",date_p," 00:00:00' and '",date_p," 23:59:59' and
        type = 'Over 60 Concession'
        order by account"
        )
      )
    
    # Prepare database output --------------------------------------------------
    
    # Create an empty table in the database to store the results of the boarding 
    # imputation analysis.  
    dbGetQuery(
      con, paste0("
      CREATE TABLE pre_avl_afc_boarding.
      pre_avl_boardings_", gsub("-", "_", date_p),"
      (
      \"case\" text,
      record_id text,
      naptan_code text
      )
      WITH (
      OIDS=FALSE
      );
      ALTER TABLE pre_avl_afc_boarding.
      pre_avl_boardings_", gsub("-", "_", date_p)," 
      OWNER TO \"cdrc-017-users\";"
      )
    )
    
    # Initiate iteration over transactions ---------------------------------------------

    for (j in 1:nrow(afc_transactions)) {
      record_id.p <- afc_transactions$record_id[j]
      
      user_spec_journeys <-
        dbGetQuery(con, 
                   paste0("
                  select
                  '",afc_transactions$record_id[j],"'::text as record_id,
                  naptan_code,t2.count   
                  from 
                  (
                  select
                  isam_op_code, etm_service_number, start_stage,
                  direction, naptan_code, count(*)            
                  from            
                  afc_avl_tt_link_boarding_imputation.           
                  afc_avl_tt_lookup_all_imputation_user_based           
                  where account = '", 
                  afc_transactions$account[j], "' and 
                  isam_op_code = '", 
                  afc_transactions$isam_op_code[j], "' and 
                  etm_service_Number = '", 
                  afc_transactions$etm_service_number[j],
                  "' and start_stage = '",
                  afc_transactions$start_stage[j],
                  "' and direction = '",
                  afc_transactions$direction[j],"'        
                  group by  isam_op_code, etm_service_number, 
                  start_stage, direction,           
                  naptan_code           
                  order by count(*) desc
                  ) t1 left join 
                  afc_avl_tt_link_boarding_imputation.           
                  afc_avl_tt_lookup_all_imputation_general_based t2
                  using (etm_service_number, isam_op_code,
                  start_stage,
                  direction,
                  naptan_code) order by t2.count desc"
                   )) %>% mutate(rank = rank(desc(count)))
        
      
      
      if (user_spec_journeys %>% filter(rank == 1) %>% nrow() == 1) {
        
        user_spec_journeys %>%
          mutate(case = 'full match') %>% 
          select(case, record_id, naptan_code) %>%
          dbWriteTable(con, c("pre_avl_afc_boarding",
                              paste0("pre_avl_boardings_", 
                                     gsub("-", "_", date_p))),
                       ., append = T, row.names = FALSE)
        
        
      } else if (user_spec_journeys %>% nrow() == 0) {
        
        user_spec_journeys <-  
          dbGetQuery(con, paste0("
            select 'gen_based_dir'::text as case, ",
            record_id.p,"::integer as record_id, naptan_code 
            from
            afc_avl_tt_link_boarding_imputation.
            afc_avl_tt_lookup_all_imputation_general_based 
            where 
            isam_op_code = '",
            afc_transactions$isam_op_code[j],
            "' and etm_service_Number = '",
            afc_transactions$etm_service_number[j],
            "' and start_stage = '",
            afc_transactions$start_stage[j],
            "' and direction = '",
            afc_transactions$direction[j],
            "' and naptan_code is not null
            order by count desc limit 1
            "))
        
        # if the first query of the general information is null, the query is 
        # repeated without the direction constraint. 
        
        if (user_spec_journeys %>% nrow() == 1) {
          
          user_spec_journeys %>%
            mutate(record_id = record_id.p) %>% 
            select(case, record_id, naptan_code) %>%
            dbWriteTable(con, 
                         c("pre_avl_afc_boarding",
                           paste0("pre_avl_boardings_",
                                  gsub("-", "_", date_p))),
                         ., append = T, row.names = FALSE)
          
        } else if (user_spec_journeys %>% nrow() == 0) {
          
          user_spec_journeys <- 
            dbGetQuery(con, paste0("
            select 'gen_based_no_dir'::text as case, ",record_id.p,"::integer 
            as record_id, naptan_code 
            from
            afc_avl_tt_link_boarding_imputation.
            afc_avl_tt_lookup_all_imputation_general_based 
            where 
            isam_op_code = '",
                                   afc_transactions$isam_op_code[j],
                                   "' and etm_service_Number = '",
                                   afc_transactions$etm_service_number[j],
                                   "' and start_stage = '",
                                   afc_transactions$start_stage[j],
            "' and naptan_code is not null
            order by count desc limit 1
            "))
          
          # If the query does return usable information, the data are stored in 
          # the user_spec_journeys object.
          
          if (user_spec_journeys %>% nrow() > 0) {
            
            user_spec_journeys %>%
              mutate(record_id = record_id.p) %>% 
              select(case, record_id, naptan_code) %>%
              dbWriteTable(con, 
                           c("pre_avl_afc_boarding",
                             paste0("pre_avl_boardings_",
                                    gsub("-", "_", date_p))),
                           ., append = T, row.names = FALSE)
            
          } else if (user_spec_journeys %>% nrow() == 0) {
            
            data.frame(c("no match"),c(record_id.p),c(NA)) %>%
              dbWriteTable(con, 
                           c("pre_avl_afc_boarding",
                             paste0("pre_avl_boardings_",
                                    gsub("-", "_", date_p))), 
                           ., append = T, row.names = FALSE)
          }
        }
        
      } else {
        
        user_spec_journeys %>%
          mutate(case = 'tie_rank') %>%
          select(case, record_id, naptan_code) %>% 
          slice(1) %>%
          dbWriteTable(con, 
                       c("pre_avl_afc_boarding",
                         paste0("pre_avl_boardings_",
                                gsub("-", "_", date_p))),
                       ., append = T, row.names = FALSE)
        
      } 
      rm(user_spec_journeys)
    }
  }
})

