



# PostGIStools provides an effcient tool to extract spatial data from the postgresql database
library(postGIStools)
# ggsn provides north arrows and scale bars
library(ggsn)

library(ggrepel)
library(grid)
library(gridExtra)
require(RPostgreSQL)
require(dplyr)
library(ggplot2)
library(cowplot)
library(stringr)
library(glue)

# Increasing the effective cache size may improve performance of the SQL 
# interprater
dbGetQuery(con, "set effective_cache_size = '128GB'")

# Here we extract the wm osm roads from the database. These are an effective backdrop for the data.
wm_roads <- get_postgis_query(con, "select type, st_transform(geom,27700) as geom from spatial_data.wm_osm_roads
                              where geom is not null and type 
                              in ('motorway', 'motorway_link',
                              'primary', 'primary_link',
                              'secondary', 'secondary_link',
                              'road', 
                              'tertiary', 'tertiary_link')", geom_name = "geom")


lookup <- data.frame(
  c("motorway", "motorway_link", "primary", "primary_link", "secondary", "secondary_link", "road", "tertiary","tertiary_link"),
  c(1,1,1,0.6,0.3,0.3,0.3,0.3,0.3)/10)
colnames(lookup) <- c("type", "width")


wm_roads@data$id <- as.character(1:nrow(wm_roads@data))
wm_roads@data <- wm_roads@data %>% left_join(lookup)
wm_roads.fort <- fortify(wm_roads, data = wm_roads@data)
wm_roads.fort <- wm_roads.fort %>% left_join(wm_roads@data)
wm_roads.fort$group <- as.character(wm_roads.fort$group)
wm_roads.fort <- wm_roads.fort %>% mutate(width = width) %>% arrange(as.integer(id), group, order)



accounts <- output4 %>% select(account, transaction_datetime) %>% unique() %>% sample_n(100)




# account <- "" This one is good.

library(stringr)


for(i in 1:length(accounts)){

account <- accounts$account[i]
date.p <- accounts$transaction_datetime[i] %>% str_sub(1,10)
# Extract boarding locations from the database AFC journey data




out2 <- dbGetQuery(con, glue("select row_number, st_transform(make_route,27700) as make_route from 
                             pre_avl_afc_journeys.pre_avl_journeys_{str_replace_all(date.p, '-','_')} 
                             where account = '{account}' and make_route is not null"))

if(nrow(out2)<1) next()






user_boarding_afc <- 
  get_postgis_query(con, glue("select record_id, row_number, \"case\", st_transform(geom,27700) as geom, transaction_datetime, direction, etm_service_number,
st_length(st_transform(make_route,27700)) as length from 
                              pre_avl_afc_journeys.pre_avl_journeys_{str_replace_all(date.p, '-','_')} 
                              where account = '{account}' and geom is not null"), 
                              geom_name = "geom")
user_boarding_afc <- cbind(user_boarding_afc@coords, user_boarding_afc@data)


# Extract boarding locations from the database AVL journey data
user_route_afc <- get_postgis_query(con, glue("select row_number, st_transform(make_route,27700) as make_route from 
                                pre_avl_afc_journeys.pre_avl_journeys_{str_replace_all(date.p, '-','_')} 
                                where account = '{account}' and make_route is not null"), 
                                geom_name = "make_route")
# Extract inferred routes from the database AFC journey data
user_route_afc@data$id <- as.character(1:nrow(user_route_afc@data))

user_route_afc.fort <- fortify(user_route_afc, data = user_route_afc@data) %>% 
  left_join(user_route_afc@data, by = "id") %>%
  mutate(group = as.character(group)) %>% 
  arrange(as.integer(id), group, order)



# Extract boarding locations from the database AVL journey data


out <- dbGetQuery(con, glue("select t2.row_number, st_transform(t2.make_route,27700) as make_route from cch t1 left join 
                                    afc_avl_tt_journeys.full_user_journeys_2015_oct_october_only t2 using (card_isrn)
                                    where t1.account = '{account}' and make_route is not null and transaction_datetime between
                                    '{date.p} 00:00:00' and '{date.p} 23:59:59'"))

if(nrow(out)<1) next()

user_boarding_avl <-  get_postgis_query(con, glue("select t2.row_number, t2.transaction_datetime, t2.direction, t2.etm_service_number, st_transform(t2.orig_geom,27700) as orig_geom, st_length(st_transform(t2.make_route,27700)) as length from cch t1 left join 
                                    afc_avl_tt_journeys.full_user_journeys_2015_oct_october_only t2 using (card_isrn)
                                    where t1.account = '{account}' and orig_geom is not null and transaction_datetime between
                                    '{date.p} 00:00:00' and '{date.p} 23:59:59'"), 
                                       geom_name = "orig_geom")


user_boarding_avl <- cbind(user_boarding_avl@coords, user_boarding_avl@data)




# Extract inferred routes from the database AVL journey data
user_route_avl <- get_postgis_query(con, glue("select t2.row_number, st_transform(t2.make_route,27700) as make_route from cch t1 left join 
                                    afc_avl_tt_journeys.full_user_journeys_2015_oct_october_only t2 using (card_isrn)
                                    where t1.account = '{account}' and make_route is not null and transaction_datetime between
                                    '{date.p} 00:00:00' and '{date.p} 23:59:59' and orig_n<dest_n"), 
                                    geom_name = "make_route")

user_route_avl@data$id <- as.character(1:nrow(user_route_avl@data))

user_route_avl.fort <- fortify(user_route_avl, data = user_route_avl@data) %>% 
  left_join(user_route_avl@data, by = "id") %>%
  mutate(group = as.character(group)) %>% 
  arrange(as.integer(id), group, order)


# Create bounding box criteria based on mins and maxs from each set of journeys.
bb_11 <- min(bbox(user_route_avl)[1,1],bbox(user_route_afc)[1,1])
bb_12 <- max(bbox(user_route_avl)[1,2],bbox(user_route_afc)[1,2])
bb_21 <- min(bbox(user_route_avl)[2,1],bbox(user_route_afc)[2,1])
bb_22 <- max(bbox(user_route_avl)[2,2],bbox(user_route_afc)[2,2])

# Determine the bounding box which encapsualtes both sets of data.
if((bb_22 - bb_21)<(bb_12 - bb_11)){
  bb_21 <- bb_21 - ((bb_12 - bb_11)-(bb_22 - bb_21))/2
  bb_22 <- bb_22 + ((bb_12 - bb_11)-(bb_22 - bb_21))/2
} else {
  bb_11 <- bb_11 - ((bb_22 - bb_21)-(bb_12 - bb_11))/2
  bb_12 <- bb_12 + ((bb_22 - bb_21)-(bb_12 - bb_11))/2
}


afc_only <- ggplot() + 
 # geom_path(data = wm_roads.fort, aes(x = long, y = lat, group = group, size = factor(width)), show.legend=FALSE, colour = "darkgrey", linejoin = "round", lineend = "round") +
  geom_path(data = user_route_afc.fort, aes(x = long, y = lat, group = group), size=2, colour = "red", alpha = 0.5, linejoin = "round", lineend = "round") +
  geom_point(data = user_boarding_afc, aes(coords.x1, coords.x2)) + 
  geom_text(data = user_boarding_afc, aes(coords.x1, coords.x2, label = row_number), size =6) + 
  coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  theme(panel.background = element_rect(fill="grey"), 
        panel.border=element_rect(size=1, linetype = 1, colour = "black")) +
  labs(title = "AFC based", x = "Easting", y = "Northing")+
  scalebar(x.min = bb_11,x.max = bb_12, y.min = bb_21,y.max = bb_22, dist = 0.5, location = "bottomleft")

grob_tab <- tableGrob(user_boarding_afc %>% 
                        select(record_id, `Journey no` = row_number,
                                                   `Transaction Time` = transaction_datetime,
                                                   direction,
                                                   Route = etm_service_number, length, case,
                      `Distance` = length), rows=NULL , theme = ttheme_default(base_size =15))
                      
library(ggrepel)

avl_only <- ggplot() + 
#  geom_path(data = wm_roads.fort, aes(x = long, y = lat, group = group, size = factor(width)), show.legend=FALSE, colour = "darkgrey", linejoin = "round", lineend = "round") +
  geom_path(data = user_route_avl.fort, aes(x = long, y = lat, group = group), size = 2, alpha = 0.5, colour = "green", linejoin = "round", lineend = "round") +
  geom_point(data = user_boarding_avl, aes(coords.x1, coords.x2)) + 
  geom_text_repel(data = user_boarding_avl, aes(coords.x1, coords.x2, label = row_number), size =6) + 
  coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  theme(panel.background = element_rect(fill="grey"), 
        panel.border=element_rect(size=1, linetype = 1, colour = "black")) +
  labs(title = "AVL based", x = "Easting", y = "Northing")+
  scalebar(x.min = bb_11,x.max = bb_12, y.min = bb_21,y.max = bb_22, dist = 0.5, location = "bottomleft")

grob_tab2 <- tableGrob(user_boarding_avl %>% 
                        select(`Journey no` = row_number,
                               `Transaction Time` = transaction_datetime,
                               direction,
                               Route = etm_service_number,
                               `Distance` = length),rows=NULL , theme = ttheme_default(base_size =15)
)


out <- cowplot::plot_grid(afc_only, avl_only, grob_tab, grob_tab2, nrow = 2, ncol = 2)
ggsave(glue("MyDocuments/sample_outputs/image_account_{i}.png"), plot = out, width = 12.6, height =11.4, scale = 2)

}




