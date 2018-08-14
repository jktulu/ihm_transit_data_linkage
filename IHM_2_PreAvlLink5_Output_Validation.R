# pre_avl_data_linkage_5_Output Validation.R
# 
# @Description
# 
# This script seeks to assertain the degree to which the imputed jounrey data 
# are representative of the avl-based boarding and journey data. Three factors
# are explored. 1), the proportion of boardings on each service, 2) the average 
# distance travelled on each service and 3) the average journey duration on 
# service. The indicators provide a simple means by which the efficacy of the 
# stage-based imputation approach works. 


# Import required libraries ----------------------------------------------------

require(RPostgreSQL)
require(dplyr)
library(ggplot2)
library(cowplot)


# Establish Database Connection ------------------------------------------------

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")




date.p <- "2015-10-05"


# Distance based validation ----------------------------------------------------

# etm_distance_afc <- 
#   dbGetQuery(con, paste0("select etm_service_number, 
#                         avg(trip_distance) as average_dist_afc,
#                         count(*)
#                         from pre_avl_afc_journeys.
#                         pre_avl_journeys_",gsub("-", "_", date.p),"
#                         where n != dest_n 
#                         and make_route is not null group by etm_service_number"))

etm_distance_afc <- 
  dbGetQuery(con, paste0("select etm_service_number, 
                         avg(trip_distance) as average_dist_afc,
                         count(*)
                         from pre_avl_afc_boarding.
                         pre_avl_journeys_",gsub("-", "_", date.p),"
                         where n +1 < dest_n
                         and make_route is not null group by etm_service_number"))



etm_distance_afc2 <- 
  dbGetQuery(con, paste0("select etm_service_number, account,
                        trip_distance
                        from pre_avl_afc_boarding.
                        pre_avl_journeys_",gsub("-", "_", date.p),"
                        where n+2 < dest_n and \"case\" in ('full match') 
                        "))

etm_distance_afc %>% left_join(etm_distance_afc2, by = "etm_service_number")



# what happens if a journey starts and finishes int he same place with a distance of 0. 

date.p2 <- "2015-10-12"
etm_distance_avl <- 
  dbGetQuery(con, paste0("select etm_service_number,
                          avg(st_length(st_transform(make_route, 27700)))
                          as average_dist_avl, 
                          count(*) from
                          afc_avl_tt_journeys.afc_avl_tt_link_2015_oct_journeys
                          where transaction_datetime 
                          between '",date.p2," 00:00:00' and '",date.p2," 23:59:59' 
                          and orig_n < dest_n and row_number = 1 group by etm_service_number "))

test1 <- etm_distance_afc %>% 
  left_join(etm_distance_avl, by = "etm_service_number")



test1 %>% 
  filter(!etm_service_number %in% c("11A", "11C")) %>%
  cor.test(.$count.x, .$count.y)


test1a <- test1 %>% filter(!etm_service_number %in% c("11A", "11C"))

cor.test(test1a$average_dist_afc,test1a$average_dist_avl)









etm_distance_afca <- 
  dbGetQuery(con, paste0("select record_id,etm_service_number,
                        trip_distance
                        from pre_avl_afc_boarding.
                        pre_avl_journeys_",gsub("-", "_", date.p),"
                        where n+2 < dest_n
                        "))


etm_distance_avla <- 
  dbGetQuery(con, paste0("select record_id, etm_service_number,
                         st_length(st_transform(make_route, 27700)) as trip_distance
                          from
                         afc_avl_tt_journeys.afc_avl_tt_link_2015_oct_journeys
                         where transaction_datetime 
                         between '",date.p," 00:00:00' and '",date.p," 23:59:59' 
                         and orig_n + 1 < dest_n and row_number = 1 "))

test1a <- etm_distance_afca %>% 
  inner_join(etm_distance_avla, by = "record_id") %>% filter(!etm_service_number.x %in% c("11A", "11C"))



cor.test(test1a$trip_distance.x, test1a$trip_distance.y)


test1 %>% View





dist_plot <- test1 %>%
  ggplot(aes(x = average_dist_afc,
                               y = average_dist_avl,
                               size = count.x)) + 
  geom_point(colour = "#d7191c", alpha = 0.4) + 
  scale_x_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000)) +
  scale_y_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000)) +
  labs(x = "afc imputed (meters)",
       y = "avl-based (meters)",
       title = "AVL vs. AFC Imputed - Distance",
       size = "Frequency") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy") + 
  geom_abline()  + 
  geom_abline(intercept = 37, slope = 0.9) + 
  geom_abline(intercept = 37, slope = 1.1) + 
  geom_abline(intercept = 37, slope = 0.8) + 
  geom_abline(intercept = 37, slope = 1.2)

dist_plot



etm_distance_2 <- 
  dbGetQuery(con, paste0("select t1.record_id, t1.etm_service_number, t1.direction, t2.direction as t2_direction,
                         st_length(st_transform(t1.make_route, 27700)) as avl_trip_distance, t2.trip_distance
                         from
                         afc_avl_tt_journeys.afc_avl_tt_link_2015_oct_journeys t1
                         inner join pre_avl_afc_boarding.
                         pre_avl_journeys_",gsub("-", "_", date.p)," t2 using (record_id)
                         where t1.transaction_datetime 
                         between '",date.p," 00:00:00' and '",date.p," 23:59:59' 
                         and t1.orig_n + 1 < t1.dest_n and 
                         t2.n +1 < t2.dest_n"))


etm_distance_2 %>% ggplot(aes(.$avl_trip_distance, .$trip_distance)) + geom_point()





etm_distance_2 %>% 
  group_by(etm_service_number) %>% 
  summarise(avg_trip_afc = mean(trip_distance),
            avg_trip_avl = mean(avl_trip_distance),
            count.x = n()) %>%
  ggplot(aes(x = avg_trip_afc,
             y = avg_trip_avl,
             size = count.x)) + 
  geom_point(colour = "#d7191c", alpha = 0.4) + 
  scale_x_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000,9000,10000), limits = c(0,10000)) +
  scale_y_continuous(breaks = c(0,1000,2000,3000,4000,5000,6000,7000,8000,9000,10000), limits = c(0,10000)) +
  scale_size_area() +
  labs(x = "afc imputed (meters)",
       y = "avl-based (meters)",
       title = "AVL vs. AFC Imputed - Distance",
       size = "Frequency") + 
  geom_text(aes(label = etm_service_number), size = 3) +
  background_grid(major = "xy", minor = "xy") + 
  geom_abline()  + 
  geom_abline(intercept = 37, slope = 0.9) + 
  geom_abline(intercept = 37, slope = 1.1) + 
  geom_abline(intercept = 37, slope = 0.8) + 
  geom_abline(intercept = 37, slope = 1.2)

  















# Journey duration based validation --------------------------------------------

etm_time_afc <- 
  dbGetQuery(con, paste("select etm_service_number,
                        avg(trip_time) as 
                        average_journey_duration_afc,
                        count(*) from
                        pre_avl_afc_boarding.pre_avl_journeys_2015_10_05 
                        where n +1 < dest_n and 
                        trip_time 
                        between 60 and 7200 
                        group by etm_service_number"))


etm_time_avl <- 
  dbGetQuery(con, paste0("select etm_service_number, 
                         avg(extract('epoch' from journey_duration)) as
                         average_journey_duration_avl, 
                         count(*) from 
                         afc_avl_tt_journeys.afc_avl_tt_link_2015_oct_journeys 
                         where transaction_datetime 
                         between '2015-10-05 00:00:00' and '2015-10-05 23:59:59' 
                         and orig_n != dest_n 
                         and extract('epoch' from journey_duration) 
                         between 60 and 7200 
                         group by etm_service_number "))

test2 <- etm_time_afc %>% left_join(etm_time_avl, by = "etm_service_number")

#weighted correlation

cor.test(test2$count.y,test2$count.x)


time_plot <- ggplot(test2, aes(x = average_journey_duration_afc,
                               y = average_journey_duration_avl,
                               size = count.x)) + 
  geom_point(colour = "#fdae61", alpha = 0.4) + 
  labs(x = "afc imputed (seconds)",
       y = "avl-based (seconds)",
       title = "AVL vs. AFC Imputed - Duration",
       size = "Frequency") + 
  background_grid(major = "xy", minor = "xy") + 
  geom_abline()




# Number of boardings ----------------------------------------------------------


etm_boardings_afc <- 
  dbGetQuery(con, paste("select etm_service_number,
                        count(*)
                        from pre_avl_afc_boarding.pre_avl_journeys_2015_10_05
                        group by etm_service_number"))


etm_boardings_avl <- 
  dbGetQuery(con, paste0("select etm_service_number,
                         count(*) from 
                         afc_avl_tt_journeys.afc_avl_tt_link_2015_oct_journeys 
                         where transaction_datetime 
                         between '2015-10-05 00:00:00' and '2015-10-05 23:59:59' 
                         group by etm_service_number "))


test3 <- etm_boardings_afc %>% 
  left_join(etm_boardings_avl, by = "etm_service_number") %>% 
  filter(!is.na(count.y) & !is.na(count.x))

test3$prop.x <- test3$count.x/sum(na.omit(test3$count.x))
test3$prop.y <- test3$count.y/sum(na.omit(test3$count.y))

cor.test(test3$count.y,test3$count.x)

sum(test3$prop.x)
sum(na.omit(test3$prop.y))

boardings_plot <- ggplot(test3, aes(x = prop.x, y = prop.y)) + 
  geom_point(colour = "#abdda4", alpha = 0.6) + 
  labs(x = "afc imputed (prop.)",
       y = "avl-based (prop.)",
       title = "AVL vs. AFC Imputed - Boardings",
       size = "Frequency") + 
  background_grid(major = "xy", minor = "xy") + 
  geom_abline()


# Diagnostic plot composition---------------------------------------------------

cowplot::plot_grid(dist_plot, time_plot,boardings_plot)



