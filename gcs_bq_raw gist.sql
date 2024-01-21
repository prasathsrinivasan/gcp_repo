--drop table rawds.trans_online;
--drop table rawds.consumer;
--drop table rawds.trans_pos;
--drop table rawds.trans_mobile_channel;
--drop table curatedds.consumer_full_load;
--drop table curatedds.trans_online_part;
--drop table curatedds.trans_pos_part_cluster;
--drop table `curatedds.trans_mobile_autopart_2021`;
--drop table `curatedds.trans_mobile_autopart_2022`;
--drop table `curatedds.trans_mobile_autopart_2023`;
create schema if not exists rawds;
select current_timestamp,"load started";
select current_timestamp,"create and load CSV data into BQ managed table with defined schema";
--complete Load(Delete/Truncate and Load)
load data overwrite `rawds.trans_pos`(txnno numeric,txndt string,custno int64,amt float64,category string,product string,city string,state string,spendby string)
from files(format='csv',uris=['gs://gcs_bigquery_usecase/store_pos_product_trans.csv'],field_delimiter=',');

select current_timestamp,"create and load json data into BQ managed table using auto detect schema";
load data overwrite rawds.trans_online
from files(format='json',uris=['gs://gcs_bigquery_usecase/online_products_trans.json']);

select current_timestamp,"create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";
create table if not exists rawds.consumer(custno int64,firstname string,lastname string,age int64,profession string);
load data overwrite rawds.consumer 
from files(format='csv',uris=['gs://gcs_bigquery_usecase/custs_header'],skip_leading_rows=1,field_delimiter=',');

select current_timestamp,"Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";
load data overwrite rawds.trans_mobile_channel
from files(format='csv',uris=['gs://gcs_bigquery_usecase/mobile_trans.csv'],field_delimiter=',');

select current_timestamp,"load completed successfully"