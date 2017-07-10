--1. 在Hive创建HBase的event_logs表
create external table event_logs(
row string,
pl string,
en string,
s_time bigint,
p_url string,
u_uid string,
u_sid string
)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,info:pl,info:en,info:s_time,info:p_url,info:u_uid,info:u_sid")
tblproperties("hbase.table.name"="event_logs");

--2. 创建MySQL在Hive中的对应表
create table stats_view_depth(
platform_dimension_id bigint,
date_dimension_id bigint,
kpi_dimension_id bigint,
pv1 bigint,
pv2 bigint,
pv3 bigint,
pv4 bigint,
pv5_10 bigint,
pv10_30 bigint,
pv30_60 bigint,
pv60plus bigint,
created string
);

--3. 创建临时表
create table stats_view_depth_tmp(
pl string,
date string,
col string,
cnt bigint
);

--4.编写UDF(platformId & dateId)

--5.上传jar包到HDFS

--6.创建hive的函数
create function platform_convert as 'com.beifeng.transformer.hive.viewdepth.PlatformDimensionUDF' using jar
 'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.1.jar';
create function date_convert as 'com.beifeng.transformer.hive.viewdepth.DateDimensionUDF' using jar
  'hdfs://hadoop:8020/user/mycentos/hive/jars/transformer-0.0.1.jar';

--5. hql编写(统计用户角度的浏览深度) 查询结果为窄表 注意时间为外部给定
from(
select
pl,
from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd') as day,
u_uid,
(case
    when count(p_url)=1 then "pv1"
    when count(p_url)=2 then "pv2"
    when count(p_url)=3 then "pv3"
    when count(p_url)=4 then "pv4"
    when count(p_url)>=5 and count(p_url)<10 then "pv5_10"
    when count(p_url)>=10 and count(p_url)<30 then "pv10_30"
    when count(p_url)>=30 and count(p_url)<60 then "pv30_60"
    else "pv60plus"
end
) as pv
from event_logs
where
en='e_pv' and
p_url is not null and
pl is not null and
s_time>unix_timestamp('2017-06-30','yyyy-MM-dd')*1000 and
s_time<unix_timestamp('2017-07-01','yyyy-MM-dd')*1000
group by pl,from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd'),u_uid) as tmp
insert overwrite table stats_view_depth_tmp
select pl,day,pv,count(distinct u_uid) cnt
where u_uid is not null
group by pl,day,pv;

with tmp as (
select pl,date,cnt as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv1'
union all
select pl,date,0 as pv1,cnt as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv2'
union all
select pl,date,0 as pv1,0 as pv2,cnt as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv3'
union all
select pl,date,0 as pv1,0 as pv2,0 as pv3,cnt as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv4'
union all
select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,cnt as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv5_10'
union all
select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,cnt as pv10_30,0 as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv10_30'
union all
select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,cnt as pv30_60,0 as pv60plus
from stats_view_depth_tmp
where col='pv30_60'
union all
select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,cnt as pv60plus
from stats_view_depth_tmp
where col='pv60plus'
)
from tmp
insert overwrite table stats_view_depth
select platform_convert(pl),date_convert(date),4,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30)
,sum(pv30_60),sum(pv60plus),'2017-06-30'
group by pl,date;