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
add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar

--4. 创建function
create function currency_type_convert as 'com.beifeng.transformer.hive.orders.CurrencyTypeDimensionUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.3.jar';
create function payment_type_convert as 'com.beifeng.transformer.hive.orders.PaymentTypeDimensionUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.3.jar';
create function order_info as 'com.beifeng.transformer.hive.orders.OrderInfoUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.4.jar';
create function order_total_amount as 'com.beifeng.transformer.hive.orders.OrderTotalAmountUDF' using jar 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.4.jar';

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

--6. 保存订单数据到mysql中
create table order_info(
order_id string,
platform string,
s_time bigint,
currency_type string,
payment_type string,
amount bigint
);

--hql插入Hive表中
from event_logs
insert overwrite table order_info
select oid,pl,s_time,cut,pt,cua
where en='e_cre' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000;

--或者
from(
select oid,pl,s_time,cut,pt,cua
where en='e_cre' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000;
) as tmp
insert overwrite table order_info select oid,pl,s_time,cut,pt,cua;

--sqoop脚本
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table order_info \
--export-dir /user/hive/warehouse/order_info/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key order_id

--7.编写hql订单数量(总)
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
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
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
sum(values) as orders,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as orders,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as orders,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as orders,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as orders,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as orders,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as orders,
date
group by date;

--8.sqoop脚本(订单数量)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created

--9.编写hql(总订单金额)
from(
from(
select
pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
cut,
pt,
oid,
max(cua) as amount
from event_logs
where en='e_cre' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt,oid
) as tmp1
select
pl,
date,
cut,
pt,
sum(amount) as amount
group by pl,date,cut,pt
) as tmp2
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,amount
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by date;

--10.sqoop脚本(订单总金额)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,order_amount,created

--11.编写hql(支付成功订单数量)
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
count(distinct oid) as orders
from event_logs
where en='e_cs' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt')
) as tmp
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,orders
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by date;

--12.sqoop脚本(支付成功订单数量)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,success_orders,created

--13.编写hql(支付成功订单金额)
from(
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
oid,
max(order_info(oid)) as amount
from event_logs
where en='e_cs' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid
) as tmp1
select
pl,
date,
cut,
pt,
sum(amount) as amount
group by pl,date,cut,pt
) as tmp2
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,amount
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by date;

--14.sqoop脚本(支付成功订单金额)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,revenue_amount,created

--15.编写hql(迄今为止支付成功订单总金额)
from(
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
oid,
max(order_info(oid)) as amount
from event_logs
where en='e_cs' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid
) as tmp1
select
pl,
date,
cut,
pt,
sum(amount) as amount
group by pl,date,cut,pt
) as tmp2
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,amount
insert overwrite table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'revenue') as amount,
date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),'revenue') as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert
(pt),cast(sum(values) as int),'revenue') as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert
('all'),cast(sum(values) as int),'revenue') as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),
payment_type_convert(pt),cast(sum(values) as int),'revenue') as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),
payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),
payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),
payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,
date
group by date;

--16.sqoop脚本(迄今为止支付成功订单总金额)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,total_revenue_amount,created

--17.编写hql(退款订单数量)
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
count(distinct oid) as orders
from event_logs
where en='e_cr' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt')
) as tmp
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,orders
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by date;

--18.sqoop脚本(退款订单数量)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_orders,created

--19.编写hql(退款订单金额)
from(
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
oid,
max(order_info(oid)) as amount
from event_logs
where en='e_cr' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid
) as tmp1
select
pl,
date,
cut,
pt,
sum(amount) as amount
group by pl,date,cut,pt
) as tmp2
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,amount
insert overwrite table stats_order_tmp2
select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
sum(values) as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
sum(values) as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
sum(values) as amount,
date
group by date;

--20.sqoop脚本(退款订单金额)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_amount,created

--21.编写hql(迄今为止退款订单总金额)
from(
from(
select
order_info(oid,'pl') as pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
order_info(oid,'cut') as cut,
order_info(oid,'pt') as pt,
oid,
max(order_info(oid)) as amount
from event_logs
where en='e_cr' and
pl is not null and
s_time>=unix_timestamp('2017-07-05','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-06','yyyy-MM-dd')*1000
group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid
) as tmp1
select
pl,
date,
cut,
pt,
sum(amount) as amount
group by pl,date,cut,pt
) as tmp2
insert overwrite table stats_order_tmp1
select pl,date,cut,pt,amount
insert overwrite table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'refund') as amount,
date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert(pt),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,
date
group by date,cut,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert
(pt),cast(sum(values) as int),'refund') as amount,
date
group by pl,date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert
('all'),cast(sum(values) as int),'refund') as amount,
date
group by pl,date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert(pt),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),
payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,
date
group by date,pt;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert(cut),
payment_type_convert('all'),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),
payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,
date
group by date,cut;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert(pl),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),
payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,
date
group by pl,date;

from stats_order_tmp1
insert into table stats_order_tmp2
select
platform_convert('all'),
date_convert(date),
currency_type_convert('all'),
payment_type_convert('all'),
order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),
payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,
date
group by date;

--22.sqoop脚本(迄今为止支付成功订单总金额)
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,
-- total_refund_amount,created