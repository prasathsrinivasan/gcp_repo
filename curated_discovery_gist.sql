create schema if not exists discoveryds;
--CREATE TABLE if not exists curatedds.trans_mobile_autopart_2021
--(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
--PARTITION BY
  --_PARTITIONDATE;
--create table  if not exists curatedds.trans_pos_part_cluster
--(txnno numeric,txndt date,custno int64,amount float64,category string,product string,city string, state string, spendby string,loadts timestamp,loaddt date) 
--partition by loaddt
--cluster by custno,txnno,loaddt
--OPTIONS (description = 'point of sale table with partition and clusters');
--create or replace table curatedds.consumer_full_load(custno INT64, fullname STRING,age INT64,yearofbirth INT64,profession STRING,loadts timestamp,loaddt date);
--create table if not exists curatedds.trans_online_part
--(transsk numeric,customerid int64,productname string,productcategory string,productprice int64,productidint int64,prodidstr string ,loadts timestamp,loaddt date)
--partition by loaddt
--OPTIONS (require_partition_filter = FALSE);

create table if not exists discoveryds.consumer_trans_pos_mob_online
(txnno numeric,txndt date,custno int64,fullname string,age int64,profession string,
trans_day string,trans_type string,net string,online_pos_amount float64,
geo_coordinate geography,provider string,activity string,spendby string,city string,
state string,online_pos_category string,product string,loadts timestamp,loaddt date);


create table if not exists discoveryds.trans_aggr
(state string,city string,category string,product string,max_amt float64,min_amt float64,
sum_amt float64,approx_cnt_cust int64,states_cnt int64,mid_amt_cnt int64,high_amt_cnt int64);

insert into discoveryds.consumer_trans_pos_mob_online
with pos_mob_trans as(select mob.txnno as mob_txnno,pos.txnno as pos_txnno,
case when mob.dt=pos.txndt then 'Same Day Trans' else 'Multi Day Trans' end as trans_day,
coalesce(mob.dt,pos.txndt)as txndt,
case when mob.txnno is null then 'MOB' else 'POS_MOB' end as trans_type,
mob.net,pos.amount,mob.geo_coordinate,mob.provider,
case when mob.activity='STILL' then 'In Store Pickup' else mob.activity end as activity,
pos.spendby,pos.city,pos.state,pos.product,pos.category,pos.loadts as pos_loadts,pos.loaddt as pos_loaddt,mob.loadts as mob_loadts,mob.loaddt as mob_loaddt,pos.custno 
from `curatedds.trans_mobile_autopart_20*` mob full join `curatedds.trans_pos_part_cluster` pos
on mob.txnno=pos.txnno),

cust_online_trans as(select *,case when transsk is not null then 'online' else null end as trans_type from curatedds.consumer_full_load cust left join curatedds.trans_online_part trans on cust.custno=trans.customerid)

select coalesce(mob_txnno,pos_txnno)as txnno,trans.txndt,trans.custno,fullname,age,profession,trans_day,coalesce(cust.trans_type,trans.trans_type)as trans_type,coalesce(net,'na')as net,
coalesce(productprice,coalesce(amount,0.0))as online_pos_amount,geo_coordinate,coalesce(provider,'na')as provider,coalesce(activity,'na')as activity,coalesce(spendby,'na')as spendby,coalesce(city,'unknown')as city,coalesce(state,'unknown')as state,coalesce(productcategory,category)as online_pos_category,product,coalesce(pos_loadts,mob_loadts)as loadts,coalesce(pos_loaddt,mob_loaddt)as loaddt from cust_online_trans cust left outer join
pos_mob_trans trans on trans.custno=cust.custno;

truncate table discoveryds.trans_aggr;

insert into discoveryds.trans_aggr select state,city,category,product,max(amount)as max_amt,min(amount)as min_amt,sum(amount)as sum_amt,
approx_count_distinct(t.custno)as approx_cnt_cust,
countif(state in('Nevada','Texas','Oregon'))as states_cnt,
countif(amount<100)as mid_amt_cnt,countif(amount>=100)as high_amt_cnt from(select t.txnno,
c.custno,age,yearofbirth,profession,amount,category,product,city,state,spendby,net,provider,activity,t.txndt from curatedds.consumer_full_load c 
inner join curatedds.trans_pos_part_cluster t on c.custno=t.custno 
inner join curatedds.trans_mobile_autopart_2023 t23 on t.txnno=t23.txnno)as t group by state,city,category,product;