declare loadts timestamp;
set loadts=(current_timestamp());
create schema if not exists curatedds;

create or replace table curatedds.consumer_full_load(
custno int64,fullname string,age int64,yearofbirth int64,profession string,loadts timestamp,loaddt date);

create table if not exists curatedds.trans_online_part(
transsk numeric,customerid int64,productname string,productcategory string,productprice int64,productidint int64,prodidstr string,loadts timestamp,loaddt date)
partition by loaddt
options(require_partition_filter=FALSE);

create table if not exists curatedds.trans_pos_part_cluster(
txnno numeric,txndt date,custno int64,amount float64,category string,product string,city string,state string,spendby string,loadts timestamp,loaddt date)
partition by loaddt
cluster by custno,txnno,loaddt
options(description='point of sale table with partition and clusters'); 

insert into curatedds.consumer_full_load 
select custid,concat(firstname,' ',lastname),age,extract(year from current_date)-age as yearofbirth,coalesce(profession,'not provided')as profession,loadts,date(loadts)as loaddt
from(select custid,firstname,lastname,age,profession from(select custid,firstname,lastname,age,profession,row_number()over(partition by custid order by age desc)as rnk from rawds.consumer)where rnk=1);

create or replace temp table online_trans_view as 
select abs(farm_fingerprint(generate_uuid()))as transk,
customerid,prod_exploded.productname,prod_exploded.productcategory,
prod_exploded.productprice,
cast(regexp_replace(prod_exploded.productid,'[^0-9]','')as int64)as prodidint,
case when coalesce(trim(regexp_replace(lower(prod_exploded.productid),'[^a-z]','')),'')='' then 'na'
else regexp_replace(lower(prod_exploded.productid),'[^a-z]','')end as prodidstr,
loadts,date(loadts)as loaddt from rawds.trans_online,
unnest(products)as prod_exploded;

insert into curatedds.trans_online_part select * from online_trans_view;

create table if not exists curatedds.trans_online_part_furniture as 
select * from online_trans_view where 1=2;

truncate table curatedds.trans_online_part_furniture;
insert into curatedds.trans_online_part_furniture select * from online_trans_view where 
productcategory='Furniture';

insert into curatedds.trans_pos_part_cluster select txnno,
parse_date('%m-%d-%Y',txndt)as txndt,custno,amt,
coalesce(product,'na'),* except(txnno,txndt,custno,amt,product),
loadts,date(loadts)as loaddt from rawds.trans_pos;

create table if not exists curatedds.trans_mobile_autopart_2021(txnno numeric,
dt date,ts timestamp,geo_coordinate geography,net string,provider string,
activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
partition by _partitiondate;

insert into curatedds.trans_mobile_autopart_2021(txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt) 
select txnno,cast(dt as date)as dt,
timestamp(concat(dt,' ',hour))as ts,st_geogpoint(long,lat)as geo_coordinate,net,provider,
activity,postal_code,town_name,loadts,date(loadts)as loaddt from rawds.trans_mobile_channel
where extract(year from cast(dt as date))=2021;

create table if not exists curatedds.trans_mobile_autopart_2022(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,
provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
partition by _partitiondate;

insert into curatedds.trans_mobile_autopart_2022(txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date)as dt,timestamp(concat(dt,' ',hour))as ts,st_geogpoint(long,lat)as geo_coordinate,net,provider,activity,
postal_code,town_name,loadts,date(loadts)as loaddt from rawds.trans_mobile_channel 
where extract(year from cast(dt as date))=2022;

create table if not exists curatedds.trans_mobile_autopart_2023(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,
provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
partition by _partitiondate;

insert into curatedds.trans_mobile_autopart_2023 (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date)as dt,timestamp(concat(dt,' ',hour))as ts,st_geogpoint(long,lat)as geo_coordinate,net,provider,activity,
postal_code,town_name,loadts,date(loadts)as loaddt from rawds.trans_mobile_channel
where extract(year from cast(dt as date))=2023;
