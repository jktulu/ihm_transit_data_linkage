

require(RPostgreSQL)
require(dplyr)
require(lubridate)
require(stringr)

# Establish Database Connection ------------------------------------------------

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '256GB'")



# query all table names from data base and subset those associated with
# pre_avl_journeys.

tabs <- dbListTables(con) %>%  .[str_detect(., pattern = "pre_avl_journeys")]


# Based on the first table, create a new table to contain all pre-avl journey
# data.
dbGetQuery(con, 
           paste0("create table pre_avl_journeys_all as select *
                  from pre_avl_afc_journeys.",tabs[1]))


# Create a progress bar to monitor progress.
p <- progress_estimated(n = length(tabs))

# loop through remaing tables and insert the data into the new output table.
for (i in 2:length(tabs)) {
  p$tick()$print()
  dbGetQuery(con, paste0("insert into pre_avl_journeys_all select * 
                         from pre_avl_afc_journeys.",tabs[i]))
}

# Create key indexes such that the data may be efficiently queried.
dbGetQuery(con, "create index on pre_avl_journeys_all (record_id)")

dbGetQuery(con, "create index on pre_avl_journeys_all (transaction_datetime)")

dbGetQuery(con, "create index on pre_avl_journeys_all (account)")








