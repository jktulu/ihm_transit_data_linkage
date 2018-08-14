
-- Timetable post-proccessing script to address duplciation and seasonal scheduling.
-- Alistair Leak
-- 05-09-2017
/* 
The objective in this script is to identify any occurence of duplication in the timetable created as a consequence of seasonal alterations to scheduled bus services. 

The key challenge in achieving the above is a lack of attribution pertaining to wheather a route is consistent or, if a route is altered based on other factors. 

A key assumption in the analysis is that we are differenting between term and non-term times. During the term time, it is expected that a greater amount of congestion on 
routes around school drop off and pick-up times results in route required a greater period of time for completion. Thus, where we believe that two scheduled routes are an term/non-term pair, 
we assign the journey with the shortest interval to non-term and visa versa.

Where scheduled journeys are identical in all but timetable id, these journeys are merged into 1 and reprot just one of the possible timetable ids. In each case, the possible timetable ids
are recorded in a seperate field (tt_array).
*/


/*
Goal of first table function to identify duplicate journeys within the timetable. A journey is consider duplicate where it shared the same
operator, days of operation (dow), route, start location (orig_naptan), end location (dest_naptan), direction, start time (orig_arrival_time) 
and end time (dest_arrive_time). At this stage, it is not possible to differentiate between seasonal paired journeys.
*/

--drop materialized view timetables.tt_2015_jul_p1;
create materialized view timetables.tt_2015_jul_p1 as
select operator, dow,  route, orig_naptan, dest_naptan, direction, orig_arrive_time, dest_arrive_time, array_agg(id) as id_array, count(*) from 
(
select distinct * from 
(
select type, t1.operator, start_date, last_date, t1.id, route, journey_scheduled, t1.arrive as orig_arrive_time, t2.arrive-t1.arrive as duration, vehicle_type, 
direction, t1.naptan_code as orig_naptan, dow, t2.arrive as dest_arrive_time, t2.naptan_code as dest_naptan from timetables.tt_2015_jul t1 left join
(
select id, operator, arrive, naptan_code from timetables.tt_2015_jul t1a where t1a.type = 'QT'
) t2 on t1.operator = t2.operator and t1.id = t2.id where t1.type = 'QO' and t1.operator in ('????', '????') order by operator,journey_scheduled, t2.arrive-t1.arrive
) t3 where type = 'QO' order by operator, route, dow, orig_arrive_time
) t4 group by operator, dow,  route, orig_naptan, dest_naptan, direction,  orig_arrive_time, dest_arrive_time order by count(*) desc;
--

/*
Identifying paired seasonally adjusted journeys part 1. Here we find journeys which are identical but with the exception that the journey completion time varies. 
*/

--drop materialized view timetables.tt_2015_jul_p2;
create materialized view timetables.tt_2015_jul_p2 as
select row_number() over(partition by "operator", dow, route, orig_naptan, dest_naptan, orig_arrive_time order by "operator", dow, route, orig_naptan, dest_naptan, orig_arrive_time) as summer_id,
operator, route, dow, id_array, orig_arrive_time, dest_arrive_time , case when dest_arrive_time <= orig_arrive_time then dest_arrive_time-orig_arrive_time +'24:00:00' else dest_arrive_time-orig_arrive_time end as interval, orig_naptan, dest_naptan from 
(
select distinct operator, route, dow, id_array, orig_naptan, dest_naptan, orig_arrive_time, dest_arrive_time , case when dest_arrive_time <= orig_arrive_time then dest_arrive_time-orig_arrive_time +'24:00:00' else dest_arrive_time-orig_arrive_time end as interval from 
(
select t1.*, t2.id_array, char_length(t2.id_array::text) as char_length, t2.orig_naptan, t2.dest_naptan, t2.orig_arrive_time, t2.dest_arrive_time from timetables.tt_2015_jul t1 left join timetables.tt_2015_jul_p1 t2 on t1.operator = t2.operator and t1.dow = t2.dow and t1.route=t2.route and
t1.direction = t2.direction and t1.journey_scheduled=t2.orig_arrive_time where type = 'QO'
) t4 where id_array is not null  order by dest_arrive_time
) t5;
--

/*
Finally, the original timetable is reconstructed and affixed with info on seasonablity and duplicates. 
cases: std = consistent across the timetable; trm = term time service (sometimes slower); and hol = faster services during school holidays.
*/


-- drop table timetables.tt_2015_jul_processed
create table timetables.tt_2015_jul_processed  as
select t6.tt_id, t6.id_array, t6."case", t7.* from 
(
select t1.*, array_length(t1.id_array,1), t1.id_array[1] as tt_id, t2.summer_id as t2_summer_id, case when t2.summer_id is null then 'std' else (case when t1.summer_id = 1 and t2.summer_id is not null then 'hol' else 'trm' end) end 
from timetables.tt_2015_jul_p2 t1 left join (select * from timetables.tt_2015_jul_p2 s1 where s1.summer_id != 1) t2 
on t1.operator = t2.operator and t1.route = t2.route and t1.dow = t2.dow and t1.orig_arrive_time=t2.orig_arrive_time and t1.orig_naptan=t1.orig_naptan and t1.dest_naptan=t2.dest_naptan 
) t6 left join timetables.tt_2015_jul t7 on t6.operator = t7.operator and t6.tt_id = t7.id;

/*
Required indexs created to boost subsequent analysis performance.
*/

create index on timetables.tt_2016_apr_processed (id, n);
create index on timetables.tt_2016_apr_processed (operator, id, n);
create index on timetables.tt_2016_apr_processed (operator, id, naptan_code);
create index on timetables.tt_2016_apr_processed (operator, route, dow);
create index on timetables.tt_2016_apr_processed (operator, route, id, naptan_code);
