# In this script the objective is to be able to compare alternative journeys appraoches.


library(lubridate)


# Import the OSM data from the database based on the criteria specified in the query. 

g_roads <- get_postgis_query(con, "select type, st_transform(geom,27700) as geom from spatial_data.g_osm_roads
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


g_roads@data$id <- as.character(1:nrow(g_roads@data))
g_roads@data <- g_roads@data %>% left_join(lookup)
g_roads.fort <- fortify(g_roads, data = g_roads@data)
g_roads.fort <- g_roads.fort %>% left_join(g_roads@data)
g_roads.fort$group <- as.character(g_roads.fort$group)
g_roads.fort <- g_roads.fort %>% mutate(width = width) %>% arrange(as.integer(id), group, order)


output4%>% head(1254) %>% filter(record_id %in% output5$record_id)%>% select(record_id) %>% unique %>% tail


sample_ids <- join6_sample$record_id

sample_ids <- output4 %>% 
  head(40000) %>% 
  select(record_id) %>% 
  sample_n(100) %>%
  .[[1]]


for(i in 16:nrow(join6_sample)) {


record_id <- sample_ids[i]
date.p <- stringr::str_sub(join6_sample$date.x[i],1,10)

# Extract boarding locations from the database AVL journey data
user_route_afc <- get_postgis_query(con, glue("select \"case\", direction, account, isam_op_code, etm_service_number, start_stage, round((st_length(st_transform(make_route,27700))::numeric/1000.0),2) as length,st_transform(make_route,27700) as make_route from 
                                              pre_avl_afc_journeys.pre_avl_journeys_{gsub('-', '_', date.p)} 
                                              where record_id = '{record_id}' and make_route is not null"), 
                                    geom_name = "make_route")



# Extract boarding locations from the database AVL journey data
user_boarding_afc <- get_postgis_query(con, glue("select st_transform(geom,27700) as geom from pre_avl_afc_journeys.pre_avl_journeys_{gsub('-', '_', date.p)} 
                                              where record_id = '{record_id}' and make_route is not null"), 
                                    geom_name = "geom")
user_boarding_afc <- cbind(user_boarding_afc@coords, user_boarding_afc@data)


# Extract boarding locations from the database AVL journey data
user_boarding_avl <- get_postgis_query(con, glue("select st_transform(orig_geom,27700) as geom from afc_avl_tt_journeys.full_user_journeys_2015_oct 
                                              where record_id = '{record_id}' and make_route is not null"), 
                                       geom_name = "geom")
user_boarding_avl <- cbind(user_boarding_avl@coords, user_boarding_avl@data)


# Extract inferred routes from the database AFC journey data
user_route_afc@data$id <- as.character(1:nrow(user_route_afc@data))

user_route_afc.fort <- fortify(user_route_afc, data = user_route_afc@data) %>% 
  left_join(user_route_afc@data, by = "id") %>%
  mutate(group = as.character(group)) %>% 
  arrange(as.integer(id), group, order)


# Extract inferred routes from the database AVL journey data
user_route_avl <- get_postgis_query(con, glue("select t2.row_number, t2.transaction_datetime, t2.isam_op_code, direction, t2.etm_service_number, t1.start_stage, round(st_length(st_transform(make_route,27700))::numeric/1000,2) as length, st_transform(t2.make_route,27700) as make_route from 
                                    afc t1 right join afc_avl_tt_journeys.full_user_journeys_2015_oct t2 using(record_id) where t2.record_id = '{record_id}' and make_route is not null and t2.transaction_datetime between
                                    '{date.p} 00:00:00' and '{date.p} 23:59:59'"), 
                                    geom_name = "make_route")
user_route_avl@data$id <- as.character(1:nrow(user_route_avl@data))




# Extract inferred routes from the database AVL journey data
#user_route_avl <- get_postgis_query(con, glue("select direction, st_transform(t2.make_route,27700) as make_route from 
#                                    afc_avl_tt_journeys.full_user_journeys_2015_oct t2 where record_id = '{record_id}' and make_route is not null and transaction_datetime between
#                                    '2015-10-05 00:00:00' and '2015-10-05 23:59:59'"), 
#                                    geom_name = "make_route")

user_route_avl@data$id <- as.character(1:nrow(user_route_avl@data))


user_route_avl.fort <- fortify(user_route_avl, data = user_route_avl@data) %>% 
  left_join(user_route_avl@data, by = "id") %>%
  mutate(group = as.character(group)) %>% 
  arrange(as.integer(id), group, order)

stage_counts <- get_postgis_query(con, glue("select t1.isam_op_code, t1.etm_service_number, t1.start_stage, t1.naptan_code, st_transform(t2.geom,27700) as geom, count(*) from 
afc_avl_tt_link_boarding_imputation.afc_avl_tt_lookup_all_imputation_data_final t1 left join naptan_stops t2 using (naptan_code)
where t1.isam_op_code = '{user_route_afc@data$isam_op_code[1]}' and t1.etm_service_number = '{user_route_afc@data$etm_service_number[1]}'and t1.start_stage = '{user_route_afc@data$start_stage[1]}'
group by t1.isam_op_code, t1.etm_service_number, t1.start_stage, t1.naptan_code, t2.geom having geom is not null order by count(*) desc"), geom_name = "geom"
)
bb_stages <- bbox(stage_counts)
stage_counts <- cbind(stage_counts@coords, stage_counts@data)









# Create bounding box criteria based on mins and maxs from each set of journeys.
bb_11 <- min(bbox(user_route_avl)[1,1],bbox(user_route_afc)[1,1])
bb_12 <- max(bbox(user_route_avl)[1,2],bbox(user_route_afc)[1,2])
bb_21 <- min(bbox(user_route_avl)[2,1],bbox(user_route_afc)[2,1])
bb_22 <- max(bbox(user_route_avl)[2,2],bbox(user_route_afc)[2,2])

if((bb_22 - bb_21)<(bb_12 - bb_11)){
  bb_21 <- bb_21 - ((bb_12 - bb_11)-(bb_22 - bb_21))/2
  bb_22 <- bb_22 + ((bb_12 - bb_11)-(bb_22 - bb_21))/2
} else {
  bb_11 <- bb_11 - ((bb_22 - bb_21)-(bb_12 - bb_11))/2
  bb_12 <- bb_12 + ((bb_22 - bb_21)-(bb_12 - bb_11))/2
}

# Create bounding box criteria based on mins and maxs from each set of journeys.
js_11 <- min(stage_counts$coords.x1)
js_12 <- max(stage_counts$coords.x1)
js_21 <- min(stage_counts$coords.x2)
js_22 <- max(stage_counts$coords.x2)

if((js_22 - js_21)<(js_12 - js_11)){
  js_21 <- js_21 - ((js_12 - js_11)-(js_22 - js_21))/2
  js_22 <- js_22 + ((js_12 - js_11)-(js_22 - js_21))/2
} else {
  js_11 <- js_11 - ((js_22 - js_21)-(js_12 - js_11))/2
  js_12 <- js_12 + ((js_22 - js_21)-(js_12 - js_11))/2
}


afc_only <- ggplot() + 
  #geom_path(data = g_roads.fort, aes(x = long, y = lat, group = group, size = width), show.legend=FALSE, colour = "darkgrey", linejoin = "round", lineend = "round") +
  geom_point(data = stage_counts, aes(x=coords.x1, y=coords.x2), size=2) +
  geom_path(data = user_route_afc.fort, aes(x = long, y = lat, group = group), size=2, colour = "red", alpha = 0.5, linejoin = "round", lineend = "round") +
  geom_point(data = user_boarding_avl, aes(coords.x1, coords.x2), size=3, color = "green") + 
  #geom_text(data = user_boarding_afc, aes(coords.x1, coords.x2, label = row_number), size =6) + 
  coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  theme(panel.background = element_rect(fill="lightgrey"), 
        panel.border=element_rect(size=1, linetype = 1, colour = "black")) +
  labs(title = "AFC based", x = "Easting", y = "Northing")+
  scalebar(x.min = bb_11,x.max = bb_12, y.min = bb_21,y.max = bb_22, dist = 0.25, location = "bottomright")

afc_only_stages <- ggplot() + 
  #geom_path(data = g_roads.fort, aes(x = long, y = lat, group = group, size = width), show.legend=FALSE, colour = "darkgrey", linejoin = "round", lineend = "round") +
  geom_path(data = user_route_afc.fort, aes(x = long, y = lat, group = group), size=2, colour = "red", alpha = 0.5, linejoin = "round", lineend = "round") +
  #geom_point(data = user_boarding_afc, aes(coords.x1, coords.x2)) + 
  #geom_text(data = user_boarding_afc, aes(coords.x1, coords.x2, label = row_number), size =6) + 
  geom_point(data = stage_counts, aes(x=coords.x1, y=coords.x2, size=count), alpha=0.5) +
  coord_equal(xlim = c(js_11, js_12), ylim = c(js_21, js_22), ratio = 1) +
  #coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  #coord_() +
  annotate(geom = "rect", xmin = bb_11-(x_dist*0.05), xmax = bb_12+(x_dist*0.05), ymin = bb_21-(y_dist*0.05),ymax = bb_22+(y_dist*0.05), fill = NA, color = "red") +
  theme(panel.background = element_rect(fill="lightgrey"), 
        panel.border=element_rect(size=1, linetype = 1, colour = "black"),legend.justification = c(0,0), legend.position = c(0,0)) +
  labs(title = "AFC based Stage Imputation", x = "Easting", y = "Northing")#+
#  scalebar(x.min = bb_11,x.max = bb_12, y.min = bb_21,y.max = bb_22, dist = 0.25, location = "bottomright")


avl_only <- ggplot() + 
  #geom_path(data = g_roads.fort, aes(x = long, y = lat, group = group, size = width), show.legend=FALSE, colour = "darkgrey", linejoin = "round", lineend = "round") +
  geom_point(data = stage_counts, aes(x=coords.x1, y=coords.x2), size=2) +
  geom_path(data = user_route_avl.fort, aes(x = long, y = lat, group = group), size = 2, alpha = 0.5, colour = "green", linejoin = "round", lineend = "round") +
  #geom_point(data = user_boarding_avl, aes(coords.x1, coords.x2)) + 
  geom_point(data = user_boarding_afc, aes(coords.x1, coords.x2), size=3, color = "red") +   coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  coord_equal(xlim = c(bb_11,bb_12), ylim = c(bb_21,bb_22), ratio = 1) +
  theme(panel.background = element_rect(fill="lightgrey"), 
        panel.border=element_rect(size=1, linetype = 1, colour = "black")) +
  labs(title = "AVL based", x = "Easting", y = "Northing")+
  scalebar(x.min = bb_11,x.max = bb_12, y.min = bb_21,y.max = bb_22, dist = 0.25, location = "bottomright")

diagnostics <- data.frame(
c("Record id",
  "Datetime",
  "Account",
  "Case",
  "Route",
  "Stage Stage",
  "Direction AFC",
  "Direction AVL",
  "Distance AFC",
  "Distance AVL")
,
c("##########",
  "##-##-## ##:##:##",
  "########",
  user_route_afc.fort$case[1],
  user_route_afc.fort$etm_service_number[1],
  user_route_afc.fort$start_stage[1],
  user_route_afc.fort$direction[1],
  user_route_avl.fort$direction[1],
  user_route_afc.fort$length[1],
  user_route_avl.fort$length[1])
# c(record_id,
#   as.character(user_route_avl$transaction_datetime[1]),
#   user_route_afc.fort$account[1],
#   user_route_afc.fort$case[1],
#   user_route_afc.fort$etm_service_number[1],
#   user_route_afc.fort$start_stage[1],
#   user_route_afc.fort$direction[1],
#   user_route_avl.fort$direction[1],
#   user_route_afc.fort$length[1],
#   user_route_avl.fort$length[1])
)
 
colnames(diagnostics) <- c("parameter", "value")

diag_grob <- tableGrob(diagnostics, rows=NULL , theme = ttheme_default(base_size = 20))

cowplot::plot_grid(afc_only, avl_only, afc_only_stages, diag_grob)


ggsave(glue("MyDocuments/sample_outputs/sample_image_{i}.png"), width = 12.6, height =11.4, scale = 2)

}
