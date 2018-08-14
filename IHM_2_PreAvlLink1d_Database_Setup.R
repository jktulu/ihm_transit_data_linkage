# pre_avl_data_linakge_1_database_setup.R
# 
# @Description
# 
# Determination of boarding location and the inference of journeys prior to 
# commencement of the avl recording.
# 
# The goal of this script is to create the datasets required to create the 
# required reference data for completion of the boarding location allocation 
# task.


# Import required libraries ----------------------------------------------------

require(RPostgreSQL)
require(dplyr)
require(lubridate)


# Establish Database Connection ------------------------------------------------
 

#' Increasing the effective cache size may improve performance of the SQL 
#' interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")


# Create imputation datasets ---------------------------------------------------

# For each timetable period combine, combine all boarding data into a single 
# table to be used in the subsequent imputation. The loop is used to automate
# construction of the SQL union all query to combine each of the afc_avl 
# boarding datasets.

# Uncomment each set in turn to create the imputation tables. 

# 2015-Jul
dates <- seq.Date(as.Date("2015-08-11"), as.Date("2015-10-04"), by = "day");
year_p <- "2015"; 
month.p <- "jul" 

# 2015-Oct
#dates <- seq.Date(as.Date("2015-10-05"), as.Date("2016-01-17"), by = "day");  

# 2016-Jan
#dates <- seq.Date(as.Date("2016-01-18"), as.Date("2016-04-10"), by = "day"); 

# 2016-Apr - Note square brackets to omit days where AVL data were unavailable.
#dates <-  seq.Date(as.Date("2016-04-11"), 
#as.Date("2016-07-17"), by = "day")[c(1:74,79:98)]; 

# 2016-Jul
#dates <- seq.Date(as.Date("2016-07-18"), as.Date("2016-08-18"), by = "day"); 


# For each pair of the above, the following query should be run.
query <- 
  paste0("create table afc_avl_tt_link_boarding_imputation.
         afc_avl_tt_lookup_",year_p,"_",month.p,"_imputation_data as
         select * from afc_avl_tt_link.afc_avl_tt_link_",
         gsub("-", "_", dates[1]),"")

for (i in 2:length(dates)) {
  query <- paste0(query, " union all select * from afc_avl_tt_link.
                afc_avl_tt_link_",gsub("-", "_", dates[i]),
                  "")
}

# Submit the query to the database to combine the tables.
dbGetQuery(con, query)

# Index the table to improve future performance.
dbGetQuery(con, paste0("create index on afc_avl_tt_link_boarding_imputation.
         afc_avl_tt_lookup_",year_p,"_",month.p,"_imputation_data 
                       (card_isrn)"))

dbGetQuery(con, paste0("create index on afc_avl_tt_link_boarding_imputation.
         afc_avl_tt_lookup_",year_p,"_",month.p,"_imputation_data 
                       (record_id)"))


# Finally, create a complete table containing boardings for the full AFC_AVL 
# period.


### Update query to select only those transactions where a full match was achieved (1_1 or 2_1)

dbGetQuery(con, "create table afc_avl_tt_link_boarding_imputation.
afc_avl_tt_lookup_all_imputation_data_final as
           select * from (	
           select t1.*, t2.isam_op_code, start_stage from 
           afc_avl_tt_link_boarding_imputation.
           afc_avl_tt_lookup_2015_jul_imputation_data t1
           left join afc t2 using (record_id)
           ) u1 union all
           select * from (	
           select t1.*, t2.isam_op_code, start_stage from 
           afc_avl_tt_link_boarding_imputation.
           afc_avl_tt_lookup_2015_oct_imputation_data t1
           left join afc t2 using (record_id)
           ) u2 union all
           select * from (	
           select t1.*, t2.isam_op_code, start_stage from 
           afc_avl_tt_link_boarding_imputation.
           afc_avl_tt_lookup_2016_jan_imputation_data t1
           left join afc t2 using (record_id)
           ) u3 union all
           select * from (	
           select t1.*, t2.isam_op_code, start_stage from
           afc_avl_tt_link_boarding_imputation.
           afc_avl_tt_lookup_2016_apr_imputation_data t1
           left join afc t2 using (record_id)
           ) u4 union all
           select * from (	
           select t1.*, t2.isam_op_code, start_stage from 
           afc_avl_tt_link_boarding_imputation.
           afc_avl_tt_lookup_2016_jul_imputation_data t1
           left join afc t2 using (record_id)
           ) u5")


# Create general boarding database table ---------------------------------------

# Create a database table which contains the general behaviour of individuals.
dbGetQuery(
  con,
  "create table afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_general_based as
    select isam_op_code, etm_service_number, direction, start_stage,
    naptan_code, count(*) as count
    from
    afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_data_final
    group by isam_op_code, etm_service_number, direction, start_stage,
    naptan_code;
    "
)

# For the above create the required database indexs to maximise performance.
dbGetQuery(
  con, "
    -- first index includes direction
    create index on afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_general_based
    (isam_op_code, etm_service_number, direction, start_stage, naptan_code);

    -- second index excl. direction.
    create index on afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_general_based
    (isam_op_code, etm_service_number, start_stage, naptan_code);
    "
)


# Create user-based boarding database table ------------------------------------

# Create a database table which contains the individual behaviour of 
# individuals.
dbGetQuery(
  con,
  "create table afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_user_based as
    select t2.account, isam_op_code, etm_service_number, direction, start_stage,
    naptan_code, count(*) as count
    from
    afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_data_final t1 
    left join cch t2 using (card_isrn)
    where t1.avl_case in ('1_1', '2_1', '3_1') 
    group by t2.account, isam_op_code, etm_service_number, 
    direction, start_stage,
    naptan_code;
    "
)

# For the above create the required database indexs to maximise performance.
dbGetQuery(
  con, "
    -- first index includes direction
    create index on afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_user_based
    (account, isam_op_code, etm_service_number, 
    direction, start_stage, naptan_code);

    -- second index excl. direction.
    create index on afc_avl_tt_link_boarding_imputation.
    afc_avl_tt_lookup_all_imputation_user_based
    (account, isam_op_code, etm_service_number, start_stage, naptan_code);
    "
)

# Summary ----------------------------------------------------------------------

# Once complete, all of the required database tables have been constructed to 
# enable the effective identification of each users boarding locations. In the
# subsequent script the goal is to assign the processed locations based on 
# isam_op_code, etm_service_number, start_stage and direction of travel.

