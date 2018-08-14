
library(stringi)
library(stringr)
library(RPostgreSQL)

# Create a connection to the database using the RPostgreSQL syntax
conn <- dbConnect(dbDriver("PostgreSQL"), 
                  dbname = "", 
                  host = "", 
                  user = "", 
                  password = "")

# Set the year and month that the data are to be processed for. 

year_p <- "2015"
month_p <- "oct"


### Create table syntax ###

dbSendQuery(conn, str_c("
CREATE TABLE timetables.tt_",year_p,"_",month_p,"
(
  type text,
  operator text,
  start_date date,
  last_date date,
  route text,
  journey_scheduled time without time zone,
  running_board text,
  id text,
  vehicle_type text,
  reg_no text,
  direction text,
  naptan_code text,
  n numeric,
  bay_no text,
  fare_stage text,
  arrive time without time zone default NULL,
  depart time without time zone,
  dow text,
  term_time text,
  bank_hols text,
  timing_point text
)"))


### Select the Cif file to be imported.
test <- stri_read_lines(file.choose())

### Count how many significant rows within the cif file 
nrows <- str_sub(test,1,2)[which(str_sub(test,1,2) %in% c("QO", "QI", "QT"))] %>% 
  length()

#Estimate of Running Time
paste("Estimated running time is", ceiling(nrows/10000 * 16/60), "minutes.")

system.time({


for(i in 2:length(test)){
  if(i %% 10000==0) print(i)
  recordIdentity <- substr(test[i],0,2)
  if (recordIdentity == "QI"){
    counter=counter+1
    location <- str_trim(substr(test[i],3,14))
    arrTime <- paste0(substr(test[i],15,16),":",substr(test[i],17,18),":00")
    depTime <- paste0(substr(test[i],19,20),":",substr(test[i],21,22),":00")
    activityFlag <- substr(test[i],23,23)
    bayNo <- substr(test[i],24,26)
    timingPoint <- substr(test[i],27,28)
    dbSendQuery(conn, paste0("insert into timetables.tt_",year_p,"_",month_p," values('",recordIdentity, "','",
                             operator, "','", firstDate, "','", lastDate, "','", routeNo, "','", 
                             journey_scheduled, "','", runningBoard, "','", uniqueJourneyId, "','", 
                             VehicleType, "','", regNo, "','", direction, "','", location, "','", 
                             counter, "','", bayNo, "','", fareStage, "','", arrTime, "','", 
                             depTime, "','", DOW, "','", termTime, "','", bankHols, "','", timingPoint,"')"))
  } 
  else if(recordIdentity == "QS"){
    transactionType <- substr(test[i],3,3)
    operator <- substr(test[i],4,7)
    uniqueJourneyId <- str_trim(substr(test[i],8,13))
    firstDate <- substr(test[i],14,21)
    lastDate <- substr(test[i],22,29)
    DOW <- substr(test[i],30,36)
    termTime <- substr(test[i],37,37)
    bankHols <- substr(test[i],38,38)
    routeNo <- str_trim(substr(test[i],39,42))
    runningBoard <- substr(test[i],43,48)
    VehicleType <- substr(test[i],49,56)
    regNo <- substr(test[i],57,64)
    direction <- substr(test[i],65,65)
  } 
  else  if (recordIdentity == "QO"){
    counter=1
    location <- str_trim(substr(test[i],3,14))
    arrTime <- paste0(substr(test[i],15,16),":",substr(test[i],17,18),":00")
    depTime <- arrTime
    journey_scheduled <- arrTime
    bayNo <- substr(test[i],19,21)
    timingPoint <- substr(test[i],22,23)
    fareStage <- substr(test[i],24,25)
    dbSendQuery(conn, paste0("insert into timetables.tt_",year_p,"_",month_p," values('",recordIdentity, "','",
                             operator, "','", firstDate, "','", lastDate, "','", routeNo, "','", 
                             journey_scheduled, "','", runningBoard, "','", uniqueJourneyId, "','", 
                             VehicleType, "','", regNo, "','", direction, "','", location, "','", 
                             counter, "','", bayNo, "','", fareStage, "','", arrTime, "','", 
                             depTime, "','", DOW, "','", termTime, "','", bankHols, "','", timingPoint,"')"))
  } 
  else if (recordIdentity == "QT"){
    counter=counter+1
    location <- str_trim(substr(test[i],3,14))
    arrTime <- paste0(substr(test[i],15,16),":",substr(test[i],17,18),":00")
    depTime <- arrTime
    bayNo <- substr(test[i],19,21)
    timingPoint <- substr(test[i],22,23)
    fareStage <- substr(test[i],24,25)
    dbSendQuery(conn, paste0("insert into timetables.tt_",year_p,"_",month_p," values('",recordIdentity, "','", 
                             operator, "','", firstDate, "','", lastDate, "','", routeNo, "','", journey_scheduled, "','", 
                             runningBoard, "','", uniqueJourneyId, "','", VehicleType, "','", regNo, "','", direction, "','", 
                             location, "','", counter, "','", bayNo, "','", fareStage, "','", arrTime, "','", depTime, "','", 
                             DOW, "','", termTime, "','", bankHols, "','", timingPoint,"')"))
  } 
}

})

# Create any required indexes which might be useful later.

dbSendQuery(conn,str_c("CREATE INDEX tt_",year_p,"_oct_rney_scheduled_idx ON timetables.tt_",year_p,"_",month_p," (journey_scheduled, id);"))
dbSendQuery(conn, str_c("CREATE INDEX tt_",year_p,"_oct_naptan_code_id_idx ON timetables.tt_",year_p,"_",month_p," (naptan_code, id);"))
dbSendQuery(conn, str_c("CREATE INDEX tt_",year_p,"_oct_route_idx ON timetables.tt_",year_p,"_",month_p," (route);"))
dbSendQuery(conn, str_c("CREATE INDEX tt_",year_p,"_oct_type_idx ON timetables.tt_",year_p,"_",month_p," (type);"))
dbSendQuery(conn, str_c("CREATE INDEX ON timetables.tt_",year_p,"_",month_p," (operator, id, type);"))
dbSendQuery(conn, str_c("CREATE INDEX ON timetables.tt_",year_p,"_",month_p," (id);"))







