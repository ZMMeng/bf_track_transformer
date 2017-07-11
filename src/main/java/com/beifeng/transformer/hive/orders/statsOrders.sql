--1. 在hive中创建hbase的event_logs对应表
drop table event_logs;

create external table event_logs(
row string,
pl string,
en string,
s_time bigint,
p_url string,
u_uid string,
u_sid string,
ca string,
ac string,
oid string,
`on` string,
cua bigint,
cut string,
pt string
)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,info:pl,info:en,info:s_time,info:p_url,info:u_uid,info:u_sid,info:ca,info:ac,info:oid,info:on,info:cua,info:cut,info:pt")
tblproperties("hbase.table.name"="event_logs");

--2. 自定义UDF

--3. 将jar包上传到HDFS上

--4. 创建function
create function currency_type_convert as 'com.beifeng.transformer.hive.orders.CurrencyTypeDimensionUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.3.jar';
create function payment_type_convert as 'com.beifeng.transformer.hive.orders.PaymentTypeDimensionUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.3.jar';

--5. 创建临时表
create table stats_order_tmp1(
pl string,
date string,
cut string,
pt string,
values bigint
);

create table stats_order_tmp2(
platform_dimension_id bigint,
date_dimension_id bigint,
currency_type_dimension_id bigint,
payment_type_dimension_id bigint,
values bigint,
created string
);

--6.编写hql订单数量(总)
from(
select
pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
cut,
pt,
count(distinct oid) as cnt
from event_logs
where en='e_cre' and
pl is not null and
s_time>=unix_timestamp('2017-06-30','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-01','yyyy-MM-dd')*1000
group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt)
as tmp
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,cnt
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cnt,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as values,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as values,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as values,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as values,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as values,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as values,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as values,
date
group by date;

--7.sqoop脚本(订单数量)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,data_dimension_id,currency_type_dimension_id,payment_type_dimension_id
--columns platform_dimension_id,data_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created