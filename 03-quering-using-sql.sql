-- Databricks notebook source
create database taxidata

-- COMMAND ----------

use taxidata

-- COMMAND ----------

select * from taxidata.yellowcab_tripdata_2019_06;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06;

-- COMMAND ----------

select VendorID, Trip_Distance from yellowcab_tripdata_2019_06;

-- COMMAND ----------

select VendorID, Trip_Distance from yellowcab_tripdata_2019_06 limit 10;

-- COMMAND ----------

select tpep_pickup_datetime as pickup, tpep_dropoff_datetime dropoff from yellowcab_tripdata_2019_06 as T

-- COMMAND ----------

select * from taxi_zone_lookup where borough = 'Queens'

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where vendorid in (1,2)

-- COMMAND ----------

select * from taxi_zone_lookup where borough like 'Staten%'

-- COMMAND ----------

select * from taxi_zone_lookup where borough like '%een%'

-- COMMAND ----------

select * from taxi_zone_lookup where borough like 'M%nha%n'

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where vendorid between 1 and 5

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where tpep_pickup_datetime between '2019-06-02' and '2019-06-03'

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where tpep_pickup_datetime between to_date('06/02/2019','MM/dd/yyyy') and to_date('2019/06/03','yyyy/MM/dd')

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where pulocationid in (select locationid from taxi_zone_lookup where zone = 'Flatlands')

-- COMMAND ----------

select * from yellowcab_tripdata_2019_06 where vendorid not between 1 and 3

-- COMMAND ----------

select * from taxi_zone_lookup where borough = 'Queens' or (borough = 'Staten Island' and zone = 'Arrochar/Fort Wadsworth')

-- COMMAND ----------

create table nulltest (a string, b int);
insert into nulltest values ('r1', 1);
insert into nulltest values ('r2', NULL);

-- COMMAND ----------

select * from nulltest where b = 1

-- COMMAND ----------

select * from nulltest where b != 1

-- COMMAND ----------

select * from nulltest where b is NULL

-- COMMAND ----------

select * from nulltest where b is NOT NULL

-- COMMAND ----------

select * from yellowcab_tripdata_2019_05 
union 
select * from yellowcab_tripdata_2019_06;

-- COMMAND ----------

select 
 tz.borough, 
 tz.zone, 
 yt.tpep_pickup_datetime, 
 yt.tpep_dropoff_datetime 
from 
 yellowcab_tripdata_2019_06 yt 
 left join taxi_zone_lookup tz   
  on (yt.pulocationid = tz.locationid)

-- COMMAND ----------

select * from taxi_zone_lookup order by borough, zone

-- COMMAND ----------

select * from taxi_zone_lookup order by borough desc, zone asc

-- COMMAND ----------

select round(sum(trip_distance), 2) from yellowcab_tripdata_2019_06

-- COMMAND ----------

select vendorid, sum(fare_amount) total_fare_amount from yellowcab_tripdata_2019_06 group by vendorid order by total_fare_amount

-- COMMAND ----------

select min(fare_amount), max(fare_amount), avg(fare_amount) from yellowcab_tripdata_2019_06

-- COMMAND ----------

select pulocationid, min(fare_amount), max(fare_amount), avg(fare_amount), count(fare_amount) from yellowcab_tripdata_2019_06 group by pulocationid having count(fare_amount) > 200000

-- COMMAND ----------

select md5('Advanced Analytics')

-- COMMAND ----------

create table taxi_june_day_sum as select dayofmonth(tpep_pickup_datetime) day, passenger_count, sum(fare_amount) total_fare_amount from yellowcab_tripdata_2019_06 group by dayofmonth(tpep_pickup_datetime), passenger_count

-- COMMAND ----------

select 
 day, 
 passenger_count, 
 round(total_fare_amount,0) total_fare_amount, 
 round(sum(total_fare_amount) over (partition by passenger_count),2) passenger_total,
 round(total_fare_amount/sum(total_fare_amount) over (partition by passenger_count) * 100,2) pct
from
 taxi_june_day_sum
order by 
 day, passenger_count

-- COMMAND ----------

select 
 day, 
 passenger_count, 
 total_fare_amount, 
 sum(total_fare_amount) over (order by day, passenger_count rows between unbounded preceding and current row) accumulated
from 
 taxi_june_day_sum 
order by 
 day, 
 passenger_count

-- COMMAND ----------

select 
 vendorid, 
 tpep_pickup_datetime, 
 lag(tpep_pickup_datetime,1) over (order by tpep_pickup_datetime) lag1 
from 
 yellowcab_tripdata_2019_06

-- COMMAND ----------

select * from (select day, total_fare_amount, dense_rank() over (order by total_fare_amount desc) ranking from (select day, sum(total_fare_amount) total_fare_amount from taxi_june_day_sum group by day)) where ranking <= 10 order by ranking

-- COMMAND ----------

with q_table as 
(select day, sum(total_fare_amount) total_fare_amount from taxi_june_day_sum group by day)
select * from (select day, total_fare_amount, dense_rank() over (order by total_fare_amount desc) ranking from q_table) where ranking <= 10 order by ranking

-- COMMAND ----------

create view borough_timespan_view as select tz.borough, min(yt.tpep_pickup_datetime) first_ride, max(yt.tpep_pickup_datetime) last_ride from yellowcab_tripdata_2019_06 yt left join taxi_zone_lookup tz on (yt.pulocationid = tz.locationid) group by tz.borough

-- COMMAND ----------

select * from borough_timespan_view

-- COMMAND ----------

create or replace temporary view number_of_rows_view as select count(*) from yellowcab_tripdata_2019_06

-- COMMAND ----------

select * from number_of_rows_view

-- COMMAND ----------

create table taxi_drivers (taxi_driver_id bigint not null, first_name string, last_name string)

-- COMMAND ----------

create table if not exists taxi_drivers (taxi_driver_id bigint not null, first_name string, last_name string)

-- COMMAND ----------

desc extended taxi_drivers

-- COMMAND ----------

alter table taxi_drivers add columns (start_date timestamp comment 'first day of driving' after taxi_driver_id)

-- COMMAND ----------

insert into taxi_drivers values (1, current_date(), 'john', 'doe');
insert into taxi_drivers values (2, null, 'jane', 'doe');
select * from taxi_drivers;

-- COMMAND ----------

drop table taxi_drivers

-- COMMAND ----------

create table tzl_delta using delta as select locationid, borough, zone, service_zone from taxi_zone_lookup

-- COMMAND ----------

update tzl_delta set zone = 'Unknown' where borough = 'Unknown'

-- COMMAND ----------

delete from tzl_delta where locationid = 265

-- COMMAND ----------

create table taxi_zone_update as select * from taxi_zone_lookup where 1=0;
insert into taxi_zone_update values (264, 'unknown', 'not applicable', 'not applicable');
insert into taxi_zone_update values (265, 'upcoming', 'not applicable', 'not applicable');

-- COMMAND ----------

merge into tzl_delta tz
using taxi_zone_update tzupdate
on tz.locationid = tzupdate.locationid
when matched then
  update set borough = tzupdate.borough, zone = tzupdate.zone, service_zone = tzupdate.service_zone
when not matched
  then insert (locationid, borough, zone, service_zone) values (tzupdate.locationid, tzupdate.borough, tzupdate.zone, tzupdate.service_zone)
