######### Load CSV files into database and add required geometry column

# This script imports all of the data into one very large table. I think that 
# this would be better if each table was imported as a single day. This would be 
# more practical in terms of day-to-day running. 

# If the new approach, where outputs from the previous script are piped to the
# database are employed, this script will not be explicitly necessary. Rather it
# will be necessary that the correct indexes be created.

library(RPostgreSQL)

# Script 1b

# Establish connection with the PostgreSQL Database


# Set the working directory based on where script 1a was performed. Should be the 
# same as filepath.

setwd()

# Identify all files to be imported. # May need to alter directory
month_files <- list.files(pattern = ".csv")

#loop through and import files.
for (i in 1:length(month_files)) {
  print(i)
  table <- read.csv(paste0(month_files[i]))
  dbWriteTable(conn, 
               "afc_avl_lookup_corrected",
               table[, c('case', 'record_id', 'avl_id', 'fleet_no_clean', 'lon', 'lat', 'time_diff')],
               append = T, row.names = F
  )
  rm(table)
}

# Once the data are imported, it is necessary that a geometry column be added. 
dbGetQuery(conn, "alter table afc_avl_lookup add column geom geometry;")
dbGetQuery(conn, "update afc_avl_lookup set geom = st_setsrid(st_point(lon, lat), 4326);")

# Create required database indexes.
dbGetQuery(conn, "create index on afc_avl_lookup (record_id, \"case\");")
dbGetQuery(conn, "create index on afc_avl_lookup (record_id, fleet_no_clean, lon, lat);")
dbGetQuery(conn, "create index on afc_avl_lookup (record_id, fleet_no_clean, geom);")



