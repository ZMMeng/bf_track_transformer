--1. 在Hive创建HBase的event_logs表
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
ac string
)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,info:pl,info:en,info:s_time,info:p_url,info:u_uid,info:u_sid,info:ca,info:ac")
tblproperties("hbase.table.name"="event_logs");

--2. 创建MySQL在Hive中的对应表
create table stats_event(
platform_dimension_id bigint,
date_dimension_id bigint,
event_dimension_id bigint,
times bigint,
created string
);

--3. 创建临时表

--4. 编写UDF

--5. 上传transformer-0.0.2.jar到HDFS上

--6. 创建hive的函数
create function event_convert as 'com.beifeng.transformer.hive.event.EventDimensionUDF' using jar
 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.2.jar';

--7. 编写hql
with tmp1 as (
select
pl,
from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,
ca,
ac
from event_logs
where en='ede' and
pl is not null and
s_time>unix_timestamp('2017-06-30','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-01','yyyy-MM-dd')*1000
) from (
select
pl as pl,
date,
ca as ca,
ac as ac,
count(1) as times
from tmp1
group by pl,date,ca,ac
union all
select
'all' as pl,
date,
ca as ca,
ac as ac,
count(1) as times
from tmp1
group by date,ca,ac
union all
select
pl as pl,
date,
ca as ca,
'all' as ac,
count(1) as times
from tmp1
group by pl,date,ca
union all
select
'all' as pl,
date,
ca as ca,
'all' as ac,
count(1) as times
from tmp1
group by date,ca
union all
select
pl as pl,
date,
'all' as ca,
'all' as ac,
count(1) as times
from tmp1
group by pl,date
union all
select
'all' as pl,
date,
'all' as ca,
'all' as ac,
count(1) as times
from tmp1
group by date
) as tmp2
insert overwrite table stats_event
select
platform_convert(pl),
date_convert(date),
event_convert(ca,ac),
sum(times),
'2017-06-30'
group by pl,date,ca,ac;

--8. sqoop脚本
sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_event \
--export-dir /user/hive/warehouse/stats_event/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,data_dimension_id,event_dimension_id