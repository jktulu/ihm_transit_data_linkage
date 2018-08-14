# Code to transfer asset tracker data from raw files into a single table.

#I think that there may be better ways to handle the asset tracker data. The 
# present approach is quite simple. It would be better to juet keep the change 
# dates

library(readxl) # Excel workbook functions
library(stringr)
library(plyr)
library(dplyr)
library(DBI)

# Establish a connection with the database
con <- src_postgres(
  dbname = "",
  host = "",
  user = "",
  password = ""
)e


# Set the working directory where the asset tracker data is held.
setwd("//jdi-blue-fs1/cdrc017$/Analysis/WP3/Asset_Tracker_Processing_and_Import")

# Create function for reading in all sheets from excel workbook
read_excel_allsheets <- function(filename) {
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  names(x) <- sheets
  x
}

# list all files with the working directory.
file_list <- list.files(pattern = ".xlsm")

# Check that the file names are legitmate
file_list <- edit(file_list)

#Create a list to store dataframe for each worbook
finaldataframes <- list()

# set counter
j = 1 

# loop through each file
for (file in file_list) { 
  # read in workbook
  mysheets <- read_excel_allsheets(file) 
  
  # remove any sheets with 1.6 in name.
  names <- names(mysheets)[!grepl(1.6, names(mysheets))] 
  
  # empty list to compile each sheets dataframe
  dataframes <- list() 
  
  # Loop through each sheet
  
  for (i in 1:length(names)) { 
    
    # Select each sheet in turn
    data <- mysheets[[names[i]]]
    
    # select required columns
    data <- data[,c(1:9)] %>% 
      select(1,2,3,6,7) 
    
    # substring the names to remove 
    date <- str_sub(names[i],1,10) 
    
    # cbind to add date to each row based on sheet name
    dataframes[[i]] <- cbind(date, data)
  }
  
  
  # combine each sheets dataframe from current workbook
  final <- data.table::rbindlist(dataframes)
  
  # write the full worbook dataframe into main list.
  finaldataframes[[j]] <- final 
  
  # write constituent dataframe to csv.
  write.csv(final, paste0("file", j, ".csv"), row.names = F)
  
  # Increment the counter.
  j = j + 1
}

# combine each workbooks full dataframe into one table.
finaldataframe <- data.table::rbindlist(finaldataframes)

# Write out the final file.
finaldataframe %>% 
  write.csv("AssetTracker_2013-2017.csv", row.names = F)

dbWriteTable(con, c("public", "asset_tracker_final"), finaldataframe, row.names=F)
