# pre_avl_data_linkage_5_Output Validation.R
# 
# @Description
# 
# This script seeks to assertain the degree to which the imputed journey data 
# are representative of the avl-based boarding and journey data. Three factors
# are explored. 1), the proportion of boardings on each service, 2) the average 
# distance travelled on each service and 3) the average journey duration on 
# service. The indicators provide a simple means by which the efficacy of the 
# stage-based imputation approach works. 


# Import required libraries ----------------------------------------------------

require(RPostgreSQL)
require(dplyr)
library(ggplot2)
library(glue)
library(cowplot)
library(tidyr)

# Establish Database Connection ------------------------------------------------

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")


# Specify dates to be processed ------------------------------------------------

dates_raw <- seq.Date(as.Date("2015-10-05"), as.Date("2015-10-31"), by = "day")

case_raw <- c(rep("Standard",length(dates_raw)), 
              rep("Comparative",length(dates_raw)))


candidates <- data.frame(rep(dates_raw,2), case_raw, stringsAsFactors = F)
#rm(case_raw, dates_raw)

colnames(candidates) <- c("date", "case")



# Create lists to hold outputs --------------------------------------------------

results2 <- c()
results3 <- c()
results4 <- c()
results5 <- c()


candidates <- candidates %>% 
  filter(case == "Comparative")

for (i in 1:nrow(candidates)) {
  
  date_p2 <- date_p <- candidates$date[i]
  
  etm_distance_afca <- 
    dbGetQuery(con, paste0("select 'AFC' as source, '",as.character(date_p2),"' as date, 
                    account, n , dest_n, transaction_datetime, extract('epoch' from dwell_time) as dwell_time, 
                    direction, record_id,isam_op_code, start_stage, etm_service_number,
                    trip_distance, trip_time
                     from pre_avl_afc_journeys.
                    pre_avl_journeys_",gsub("-", "_", date_p),"
                           where n  < dest_n 
                           ")) %>% 
    mutate(tt_operator = ifelse(isam_op_code == '????', "????", "????"))
  
  
    etm_distance_avla <- 
    dbGetQuery(con, 
               paste0("select  'AVL' as source, '",as.character(date_p2),"' as date, 
                      card_isrn, direction, orig_n, transaction_datetime, 
                      extract('epoch' from journey_duration) as trip_time, 
                      extract('epoch' from dwell_time) as dwell_time, dest_n, 
                      record_id, etm_service_number, isam_op_code,
                      st_length(st_transform(make_route, 27700)) as trip_distance, 
                      journey_duration--, 
                      --t2.\"case\" as afc_avl_lookup_case,
                      --t2.time_diff as afc_avl_lookup_time_diff
                      from
                      afc_avl_tt_journeys.full_user_journeys_2015_oct t1
                      --left join afc_avl_lookup_final_updated t2 using (record_id)
                      where transaction_datetime 
                      between '",date_p2," 00:00:00' and '",date_p2," 23:59:59' 
                      and orig_n < dest_n")) %>% distinct %>% mutate(tt_operator = ifelse(isam_op_code != '????', '????', '????'))

  
  # Where comparative analysis is being beformed we want to retain only those 
  # records which occur in both the AVl-based and pre-avl based datasets. In
  # addition, we create a table containing all of the comparative data such that
  # a week level analysis may be performed.
  print(i)
  if (candidates$case[i] == "Comparative") {
    
    etm_distance_afca <- etm_distance_afca %>% 
      filter(record_id %in% etm_distance_avla$record_id)
    
    etm_distance_avla <- etm_distance_avla %>% 
      filter(record_id %in% etm_distance_afca$record_id)
    
    results4[[length(results4) + 1]] <- etm_distance_afca  
    results5[[length(results5) + 1]] <- etm_distance_avla
    
  } else next()



  taba <- etm_distance_afca %>%
    group_by(tt_operator, etm_service_number) %>%
    filter( trip_distance > fivenum(trip_distance)[2] ,
            trip_distance < fivenum(trip_distance)[4] ) %>%
    ungroup() %>%
    group_by(tt_operator, etm_service_number) %>%
    summarise(trip_dist = mean(trip_distance))


  tabb <- etm_distance_avla %>%
    group_by(tt_operator,etm_service_number) %>%
    filter( trip_distance > fivenum(trip_distance)[2] ,
            trip_distance < fivenum(trip_distance)[4] ) %>%
    ungroup() %>%
    group_by(tt_operator,etm_service_number) %>%
    summarise(trip_dist = mean(trip_distance), count.x = n())


  tabc <- etm_distance_afca %>%
    group_by(tt_operator, etm_service_number) %>%
    filter( trip_distance > fivenum(trip_distance)[2] ,
            trip_distance < fivenum(trip_distance)[4] ,
            trip_time > 0,
            trip_time > fivenum(trip_time)[2] ,
            trip_time < fivenum(trip_time)[4] ) %>%
    ungroup() %>%
    group_by(tt_operator,etm_service_number) %>%
    summarise(trip_time = mean(trip_time,na.rm = T))

  tabd <- etm_distance_avla %>%
    filter(trip_time > 60 && trip_time < 7200) %>%
    group_by(tt_operator,etm_service_number) %>%
    filter( trip_distance > fivenum(trip_distance)[2] ,
            trip_distance < fivenum(trip_distance)[4],
            trip_time > 0,
            trip_time > fivenum(trip_time)[2] ,
            trip_time < fivenum(trip_time)[4] ) %>%
    ungroup() %>%
    group_by(tt_operator,etm_service_number) %>%
    summarise(trip_time = mean(trip_time, na.rm = T), count.x = n())





  taba %>%
    left_join(tabb, by = c("tt_operator","etm_service_number")) %>%
    mutate(case = ifelse(trip_dist.y > (trip_dist.x*0.9) &
                           trip_dist.y < (trip_dist.x*1.1),
                         "yes", "no")) %>%
    group_by(case) %>%
    summarise(sum = sum(count.x)/sum(.$count.x)) %>%
  print()


  taba %>% left_join(tabb, by = c("tt_operator","etm_service_number")) %>%
    mutate(case = ifelse(trip_dist.y > (trip_dist.x*0.85) &
                           trip_dist.y < (trip_dist.x*1.15)
                         , "yes", "no")) %>%
    group_by(case) %>%
    summarise(sum = sum(count.x)/sum(.$count.x)) %>%
    print()




  test3 <- tabc %>%
    left_join(tabd, by = c("tt_operator","etm_service_number"))
  colnames(test3) <- c("tt_operator",
                       "etm_service_number",
                       "average_time_afc",
                       "average_time_avl",
                       "count.x")


  test2 <- taba %>%
    left_join(tabb, by = c("tt_operator","etm_service_number"))

  colnames(test2) <- c("tt_operator",
                       "etm_service_number",
                       "average_dist_afc",
                       "average_dist_avl",
                       "count.x")

  test2$date <-  test3$date <- candidates$date[i]
  test2$case <- test3$case <- candidates$case[i]



  results2[[length(results2) + 1]] <- test2

  results3[[length(results3) + 1]] <- test3


}

output2 <- results2 %>% do.call("rbind", .)
output3 <- results3 %>% do.call("rbind", .)


output4 <- results4 %>% do.call("rbind", .)
output5 <- results5 %>% do.call("rbind", .)

output5$record_id <- as.integer(output5$record_id)
output4$record_id <- as.integer(output4$record_id)



output2 %>% head
output3 %>% head


dist_plot6 <- output2 %>%
  ggplot(aes(x = average_dist_afc,
             y = average_dist_avl,
             size = count.x)) + 
  geom_abline(size = 1, colour = "grey")  + 
  geom_abline(intercept = 0, slope = 0.9, size = 0.7, colour = "grey") +
  geom_abline(intercept = 0, slope = 1.1, size = 0.7, colour = "grey") + 
  geom_abline(intercept = 0, slope = 0.8, size = 0.4, colour = "grey") + 
  geom_abline(intercept = 0, slope = 1.2, size = 0.4, colour = "grey") + 
  geom_abline(intercept = 0, slope = 0.7, size = 0.2, colour = "grey") + 
  geom_abline(intercept = 0, slope = 1.3, size = 0.2, colour = "grey") +
  annotate("text", x = 5400, y = 4500, label = "10%") +
  annotate("text", x = 5400, y = 4000, label = "20%") +
  annotate("text", x = 5400, y = 3500, label = "30%") +

  geom_point(aes(colour = tt_operator), alpha = 0.4) +
  scale_size_area() +
  scale_x_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000), limits = c(0,5500)) +
  scale_y_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000), limits = c(0,5500)) +
  labs(x = "afc imputed (meters)",
       y = "avl-based (meters)",
       title = glue("AVL vs. AFC Imputed - Distance - {date_p} comp"),
       size = "Frequency") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy") + 
  facet_grid(case~date, scales = "free")

dist_plot6

output2 %>% head

output2 %>% 
  group_by(date, tt_operator, case) %>% 
  summarise(cor = cor(average_dist_afc, 
                      average_dist_avl, 
                      use = "comp")) %>%
  ungroup() %>% arrange(tt_operator, date) %>%
  spread(case, cor)



output2 %>%
  group_by(date, case) %>%
  summarise(cor = cor(average_dist_afc, average_dist_avl,"complete.obs")) %>% 
  spread(case, cor)



dist_plot7 <- output3 %>%
  ggplot(aes(x = average_time_afc,
             y = average_time_avl,
             size = count.x)) + 
  geom_abline(size = 1, colour = "grey")  + 
  geom_abline(intercept = 0, slope = 0.9, size = 0.7, colour = "grey") +
  geom_abline(intercept = 0, slope = 1.1, size = 0.7, colour = "grey") + 
  geom_abline(intercept = 0, slope = 0.8, size = 0.4, colour = "grey") + 
  geom_abline(intercept = 0, slope = 1.2, size = 0.4, colour = "grey") + 
  geom_abline(intercept = 0, slope = 0.7, size = 0.2, colour = "grey") + 
  geom_abline(intercept = 0, slope = 1.3, size = 0.2, colour = "grey") +
  geom_point(aes(colour = tt_operator), alpha = 0.4) +
  scale_size_area() +
  scale_x_continuous(breaks = c(0,300,600,900,1200,1500), limits = c(0,1500)) +
  scale_y_continuous(breaks = c(0,300,600,900,1200,1500), limits = c(0,1500)) +
  labs(x = "afc imputed (seconds)",
       y = "avl-based (seconds)",
       title = glue("AVL vs. AFC Imputed - Time - {date_p} comp"),
       size = "Frequency") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy") + 
  facet_grid(case~date, scales = "free")


dist_plot7





output3 %>%
  group_by(date, case) %>%
  summarise(cor = cor(average_time_afc, average_time_avl,use = "compl")) %>% 
  spread(case, cor)


output3 %>% 
  group_by(date, tt_operator, case) %>% 
  summarise(cor = cor(average_time_afc, 
                      average_time_avl, 
                      use = "comp")) %>%
  ungroup() %>% arrange(tt_operator, date) %>%
  spread(case, cor)



output3 %>% 
  filter(date == '2015-10-10') %>% 
  ggplot(aes(average_time_afc, average_time_avl)) + 
  geom_point()











# Part2: Weekly Trends ---------------------------------------------------------




head(output4)
head(output5)

output5 <- output5 %>% mutate(tt_operator = ifelse(isam_op_code != '????', '????', '????'))

# afc
tabe <- output4 %>%
  group_by(tt_operator, etm_service_number) %>% 
  filter( trip_distance > fivenum(trip_distance)[2] ,
          trip_distance < fivenum(trip_distance)[4] ,
          trip_time > 0,
          trip_time > fivenum(trip_time)[2] ,
          trip_time < fivenum(trip_time)[4] ) %>% 
  ungroup() %>% 
  group_by(tt_operator, etm_service_number) %>% 
  summarise(trip_time = mean(trip_time,na.rm = T))

# avl
tabf <- output5 %>% 
   filter(afc_avl_lookup_case == '1_1') %>% 
#  filter(trip_time > 60 && trip_time < 7200) %>% 
  group_by(tt_operator, etm_service_number) %>% 
  filter( trip_distance > fivenum(trip_distance)[2] ,
          trip_distance < fivenum(trip_distance)[4],
          trip_time > 0, 
          trip_time > fivenum(trip_time)[2] ,
          trip_time < fivenum(trip_time)[4] ) %>% 
  ungroup() %>% 
  group_by(tt_operator, etm_service_number) %>% 
  summarise(trip_time = mean(trip_time, na.rm = T), count.x = n())


test4 <- tabe %>% left_join(tabf, by = c("tt_operator","etm_service_number"))
colnames(test4) <- c("tt_operator","etm_service_number", "average_time_afc", "average_time_avl", "count.x")




test4 %>% 
  group_by(tt_operator) %>% 
  summarise(cor = cor(average_time_afc, 
                      average_time_avl, 
                      use = "comp"))


dist_plot8 <- test4 %>%
  ggplot(aes(x = average_time_afc,
             y = average_time_avl,
             size = count.x)) + 
  geom_abline(size = 1, colour = "grey")  + 
  geom_abline(intercept = -60, slope = 1, size = 0.5, colour = "grey") +
  geom_abline(intercept = 60, slope = 1, size = 0.5, colour = "grey") + 
  geom_abline(intercept = -120, slope = 1, size = 0.3, colour = "grey") + 
  geom_abline(intercept = 120, slope = 1, size = 0.3, colour = "grey") + 
  geom_abline(intercept = -180, slope = 1, size = 0.1, colour = "grey") + 
  geom_abline(intercept = 180, slope = 1, size = 0.1, colour = "grey") +
  geom_abline(intercept = -240, slope = 1, size = 0.1, colour = "grey") + 
  geom_abline(intercept = 240, slope = 1, size = 0.1, colour = "grey") +
  geom_abline(intercept = -300, slope = 1, size = 0.1, colour = "grey") + 
  geom_abline(intercept = 300, slope = 1, size = 0.1, colour = "grey") +
  geom_abline(intercept = -360, slope = 1, size = 0.1, colour = "grey") + 
  geom_abline(intercept = 360, slope = 1, size = 0.1, colour = "grey") +
  annotate(geom = "text", x = 1200, y = 1220, label = "0 mins", angle = 45) +
  annotate(geom = "text", x = 1260, y = 1160, label = "2 mins", angle = 45) +
  annotate(geom = "text", x = 1320, y = 1100, label = "4 mins", angle = 45) +
  coord_equal() +
  scale_x_continuous(breaks = c(0,300,600,900,1200,1500),
                     minor_breaks = seq(0,1500,60), limits = c(0,1500)) +
  scale_y_continuous(breaks = c(0,300,600,900,1200,1500),
                     minor_breaks = seq(0,1500,60), limits = c(0,1500)) +
  scale_size_area() +
  labs(x = "afc-based (seconds)",
       y = "avl-based (seconds)",
       title = glue("AVL vs. AFC Imputed - Time - {dates_raw[1]} > {rev(dates_raw)[1]} comp"),
       size = "Frequency",
       colour = "Operator") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy",
                  size.major = 0.5, size.minor = 0.1,
                  colour.minor = "lightgrey", colour.major = "grey")


dist_plot8




tabe %>% left_join(tabf, by = c("tt_operator", "etm_service_number")) %>%
  mutate(case = ifelse(trip_time.y > ((trip_time.x * 1) - 60) &
                         trip_time.y < ((trip_time.x * 1) + 60), 
                       "yes", "no")) %>%
  group_by(case) %>% 
  summarise(sum = sum(count.x, na.rm = T)/sum(.$count.x, na.rm = T)) %>% 
  print()


tabe %>% left_join(tabf, by = c("tt_operator", "etm_service_number")) %>%
  mutate(case = ifelse(trip_time.y > ((trip_time.x * 1) - 120) &
                         trip_time.y < ((trip_time.x * 1) + 120), 
                       "yes", "no")) %>%
  group_by(case) %>% 
  summarise(sum = sum(count.x, na.rm = T)/sum(.$count.x, na.rm = T)) %>% 
  print()

tabe %>% left_join(tabf, by = c("tt_operator", "etm_service_number")) %>%
  mutate(case = ifelse(trip_time.y > ((trip_time.x * 1) - 180) &
                         trip_time.y < ((trip_time.x * 1) + 180), 
                       "yes", "no")) %>%
  group_by(case) %>% 
  summarise(sum = sum(count.x, na.rm = T)/sum(.$count.x, na.rm = T)) %>% 
  print()










tabg <- output4 %>% 
  group_by(tt_operator, etm_service_number) %>% 
  #filter( trip_distance > fivenum(trip_distance)[2] ,
  #       trip_distance < fivenum(trip_distance)[4] ) %>% 
  ungroup() %>% 
  group_by(tt_operator, etm_service_number) %>% 
  summarise(trip_dist = mean(trip_distance))


tabh <- output5 %>% 
  group_by(tt_operator, etm_service_number) %>% 
  #filter( trip_distance > fivenum(trip_distance)[2] ,
  #        trip_distance < fivenum(trip_distance)[4] ) %>% 
  ungroup() %>% 
  group_by(tt_operator, etm_service_number) %>% 
  summarise(trip_dist = mean(trip_distance), count.x = n())




test5 <- tabg %>% left_join(tabh, by = c("tt_operator","etm_service_number"))

colnames(test5) <- c("tt_operator","etm_service_number", "average_dist_afc", "average_dist_avl", "count.x")




output5 %>%
  group_by(tt_operator) %>% 
  summarise(cor = cor(average_dist_afc, 
                      average_dist_avl, 
                      use = "comp"))


join6 <- output5 %>% inner_join(output4, by = c("record_id")) %>% group_by(isam_op_code.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup  

join6_sample <- join6 %>% sample_n(100, )


#ouput 5 contains orig_n which is from avl

join6 %>%
  #filter(etm_service_number.x %in% c("11", "11A", "11C")) %>%
  sample_frac(0.1) %>%
ggplot(aes(trip_distance.x, trip_distance.y)) + 
  geom_point(alpha = 0.15, size=0.2) + 
  geom_point(data = join6_sample, aes(trip_distance.x, trip_distance.y), color="green") +
  coord_equal() +
  scale_x_continuous(breaks = seq(0,120000,1000), labels = seq(0,120000,1000)/1000)+
  scale_y_continuous(breaks = seq(0,120000,1000), labels = seq(0,120000,1000)/1000)+
  geom_abline(intercept = -1000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = 1000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = -2000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = 2000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = -3000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = 3000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = -4000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = 4000, slope = 1, size = 1, colour = "darkgrey") + 
  geom_abline(intercept = 0, slope = 1.1, size = 1, colour = "red") + 
  geom_abline(intercept = 0, slope = .9, size = 1, colour = "red") +   
  geom_abline(intercept = 0, slope = 1.2, size = 1, colour = "red") + 
  geom_abline(intercept = 0, slope = .8, size = 1, colour = "red") +
  geom_abline(intercept = 0, slope = 1.3, size = 1, colour = "red") + 
  geom_abline(intercept = 0, slope = .7, size = 1, colour = "red") +
  #annotate("text", x = (7000+300)+15500, y=(8100+300)+16000, label="1000 m", angle = 45) +
  #annotate("text", x = (6500+300)+15500, y=(8600+300)+16000, label="2000 m", angle = 45) +
  #annotate("text", x = (6000+300)+15500, y=(9100+300)+16000, label="3000 m", angle = 45) +
  #annotate("text", x = (5500+300)+15500, y=(9600+300)+16000, label="4000 m", angle = 45) +
  annotate("text", x = (7000+300), y=(8100+300), label="1000 m", angle = 45) +
  annotate("text", x = (6500+300), y=(8600+300), label="2000 m", angle = 45) +
  annotate("text", x = (6000+300), y=(9100+300), label="3000 m", angle = 45) +
  annotate("text", x = (5500+300), y=(9600+300), label="4000 m", angle = 45) +
  #annotate("text", x = 3*(7780+300), y=3*(7150+300), label="10%", colour="red", angle = (180*atan(4500/5000))/pi) +
  #annotate("text", x = 3*(8180+300), y=3*(6690+300), label="20%", colour="red", angle = (180*atan(4000/5000))/pi) +
  #annotate("text", x = 3*(8550+300), y=3*(6120+300), label="30%", colour = "red", angle = (180*atan(3500/5000))/pi) +
  annotate("text", x = (7780+300), y=(7150+300), label="10%", colour="red", angle = (180*atan(4500/5000))/pi) +
  annotate("text", x = (8180+300), y=(6690+300), label="20%", colour="red", angle = (180*atan(4000/5000))/pi) +
  annotate("text", x = (8550+300), y=(6120+300), label="30%", colour = "red", angle = (180*atan(3500/5000))/pi) +
  #facet_wrap(~etm_service_number.x) + 
  labs(x = "AVL Distance (km)", y = "AFC Distance (km)") +
  background_grid(major = "xy", minor = "xy",
                  size.major = 0.5, size.minor = 0.0,
                  colour.minor = "lightgrey", colour.major = "grey")


ggsave(filename = "MyDocuments/sample_outputs/scatterplot_plus_100_n_sample.png", dpi = 300, width = 10, height = 10)




# Based on all journeys on the day being studied, what proportion of journeys fall within 10, 20 and 30% of the avl-based distance. 

output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>% 
 select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x*0.9 & trip_distance.y < trip_distance.x*1.1) %>% count(case) %>% mutate(prop = n/sum(n))

output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>%  
  inner_join(output4, by = c("record_id")) %>% select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x*0.8 & trip_distance.y < trip_distance.x*1.2) %>% count(case) %>% mutate(prop = n/sum(n))



output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>%  
  inner_join(output4, by = c("record_id")) %>% select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x*0.7 & trip_distance.y < trip_distance.x*1.3) %>% count(case) %>% mutate(prop = n/sum(n))




# Based on all journeys on the day being studied, what proportion of journeys fall within 1 minute, 2 minutes and three minutes of the avl-based distance. 


output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>% 
  select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x-100 & trip_distance.y < trip_distance.x+100) %>% count(case) %>% mutate(prop = n/sum(n))

output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>% 
  select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x-500 & trip_distance.y < trip_distance.x+500) %>% count(case) %>% mutate(prop = n/sum(n))

output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>% 
  select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x-1000 & trip_distance.y < trip_distance.x+1000) %>% count(case) %>% mutate(prop = n/sum(n))









output5 %>% 
  inner_join(output4, by = c("record_id"))  %>% 
  group_by(tt_operator.x, etm_service_number.x) %>%
  filter( trip_distance.x > fivenum(trip_distance.x)[2] ,
          trip_distance.x < fivenum(trip_distance.x)[4] ,
          trip_distance.y > fivenum(trip_distance.y)[2] ,
          trip_distance.y < fivenum(trip_distance.y)[4] ) %>% 
  ungroup %>%  
  inner_join(output4, by = c("record_id")) %>% select(tt_operator.x, etm_service_number.x, trip_distance.x, trip_distance.y) %>%
  mutate(case = trip_distance.y > trip_distance.x*0.7 & trip_distance.y < trip_distance.x*1.3) %>% count(case) %>% mutate(prop = n/sum(n))


























 #avl and y afc

dist_plot10 <- test5 %>%
  ggplot(aes(x = average_dist_afc,
             y = average_dist_avl,
             size = count.x)) + 
  geom_abline(size = 1, colour = "grey")  + 
  geom_abline(intercept = -100, slope = 0.9, size = 0.7, colour = "darkgrey") +
  geom_abline(intercept = 100, slope = 1.1, size = 0.7, colour = "darkgrey") + 
  geom_abline(intercept = -100, slope = 0.8, size = 0.4, colour = "darkgrey") + 
  geom_abline(intercept = 100, slope = 1.2, size = 0.4, colour = "darkgrey") + 
  geom_abline(intercept = -100, slope = 0.7, size = 0.2, colour = "darkgrey") + 
  geom_abline(intercept = 100, slope = 1.3, size = 0.2, colour = "darkgrey") +
  annotate("text", x = 5400, y = 4800, label = "10%") +
  annotate("text", x = 5400, y = 4400, label = "20%") +
  annotate("text", x = 5400, y = 4000, label = "30%") +
  geom_point(aes(colour = tt_operator), alpha = 0.2) +
  scale_size_area(max_size = 10) +
  scale_x_continuous(breaks = seq(0,8000, 1000), minor_breaks = seq(0,8000,200), limits = c(0,5500)) +
  scale_y_continuous(breaks = seq(0,8000, 1000), minor_breaks = seq(0,8000,200), limits = c(0,5500)) +
  labs(x = "afc imputed (meters)",
       y = "avl-based (meters)",
       title = glue("AVL vs. AFC Imputed - Distance - {dates_raw[1]} > {rev(dates_raw)[1]} comp"),
       size = "Frequency",
       color = "Operator") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy",
                  size.major = 0.5, size.minor = 0.05,
                  colour.minor = "lightgrey", colour.major = "grey")

dist_plot10

tabg %>% 
  left_join(tabh, by = c("tt_operator","etm_service_number")) %>%
  mutate(case = ifelse(trip_dist.y > (trip_dist.x * 0.9) &
                         trip_dist.y < (trip_dist.x * 1.1), "yes", "no")) %>%
  group_by(case) %>% 
  summarise(sum = sum(count.x, na.rm = T)/sum(.$count.x, na.rm = T)) %>% 
  print()


tabg %>% left_join(tabh, by = c("tt_operator", "etm_service_number")) %>%
  mutate(case = ifelse(trip_dist.y > (trip_dist.x*0.85) &
                         trip_dist.y < (trip_dist.x*1.15), "yes", "no")) %>%
  group_by(case) %>% 
  summarise(sum = sum(count.x, na.rm = T)/sum(.$count.x, na.rm = T)) %>% 
  print()



cowplot::plot_grid(dist_plot8, dist_plot10)






output4 %>% head


results5[[1]] %>% head

output4 %>% nrow

trend_plot_afc <-
output4 %>% 
  group_by(date, tt_operator, etm_service_number) %>% 
  filter( trip_distance > fivenum(trip_distance)[2] ,
          trip_distance < fivenum(trip_distance)[4] ) %>% 

#angle = # based on AFC
#utput4 %>% 
filter(etm_service_number %in% c("1")) %>% 
           group_by(date, tt_operator, etm_service_number) %>% summarise(avg_distance = mean(trip_distance), count = n()) %>% mutate(date_t = as.POSIXct(strftime(date, "%Y-%m-%d %H:%M:%S"))) %>% mutate(etm_service_number2 = paste(tt_operator, etm_service_number)) %>% 
  ggplot(aes(x = date_t, y = avg_distance, color = etm_service_number2)) + 
  geom_line(aes(size = count), alpha = 0.4, show.legend = F) +
  annotate(geom = "rect",
           xmin = as.POSIXct(strftime("2015-10-10", "%Y-%m-%d %H:%M:%S")), 
           xmax = as.POSIXct(strftime("2015-10-12", "%Y-%m-%d %H:%M:%S")),
           ymin= 0, ymax = 10000, fill = "red", alpha = 0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-17", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-10-19", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-24", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-10-26", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-31", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-11-01", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) + 
  theme(axis.text.x = element_text()) + 
  theme(axis.text.x = element_text()) +
  labs(title = "AFC based")

#unique(output4$etm_service_number)




trend_plot_avl <-
output5 %>% 
  group_by(date, tt_operator, etm_service_number) %>% 
  filter( trip_distance > fivenum(trip_distance)[2] ,
          trip_distance < fivenum(trip_distance)[4] ) %>% 
  
  #angle = # based on AFC
  #utput4 %>% 
  filter(etm_service_number %in% c("1")) %>% 
  group_by(date, tt_operator, etm_service_number) %>% summarise(avg_distance = mean(trip_distance), count = n()) %>% mutate(date_t = as.POSIXct(strftime(date, "%Y-%m-%d %H:%M:%S"))) %>% mutate(etm_service_number2 = paste(tt_operator, etm_service_number)) %>% 
  ggplot(aes(x = date_t, y = avg_distance, color = etm_service_number2)) + 
  geom_line(aes(size = count), alpha = 0.4, show.legend = F)+
  annotate(geom = "rect",
           xmin = as.POSIXct(strftime("2015-10-10", "%Y-%m-%d %H:%M:%S")), 
           xmax = as.POSIXct(strftime("2015-10-12", "%Y-%m-%d %H:%M:%S")),
           ymin= 0, ymax = 10000, fill = "red", alpha = 0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-17", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-10-19", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-24", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-10-26", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) +
  # annotate(geom = "rect",
  #          xmin = as.POSIXct(strftime("2015-10-31", "%Y-%m-%d %H:%M:%S")), 
  #          xmax = as.POSIXct(strftime("2015-11-01", "%Y-%m-%d %H:%M:%S")),
  #          ymin= 0, ymax = 10000, fill = "red", alpha=0.2) + 
  theme(axis.text.x = element_text()) +
  labs(title = "AVL based")

plot_grid(trend_plot_afc,trend_plot_avl, ncol = 1)































