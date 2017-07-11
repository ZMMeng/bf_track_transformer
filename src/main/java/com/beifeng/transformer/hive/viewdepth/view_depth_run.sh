#!/bin/bash

# Hive和Sqoop的目录
HIVE_HOME=/home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6
SQOOP_HOME=/home/mycentos/opt/cdh/sqoop-1.4.5-cdh5.3.6

#起始时间和结束时间
startDate=''
endDate=''

# 判断是否已给定参数，
until [ $# -eq 0 ]
do
    if [ $1'x' = '-sdx' ]; then
        shift
        startDate=$1
    elif [ $1'x' = '-edx' ]; then
        shift
        endDate=$1
    fi
    shift
done

# 若已给定，则分别给startDate和endDate赋值，否则使用默认值
if [ -n "$startDate" ] && [ -n "endDate" ]; then
    echo "use the arguments of the date"
else
    echo "use the default date"
    startDate=$(date -d last-day +%Y-%m-%d)
    endDate=$(date +%Y-%m-%d)
fi

echo "run of arguments: start date is:$startDate, end date is:$endDate"
echo "start run of view depth job "

## insert overwrite
echo "start insert user data to hive tmp table"
$HIVE_HOME/bin/hive --database default -e "from(select pl, from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd') as day, u_uid, (case when count(p_url)=1 then 'pv1' when count(p_url)=2 then 'pv2' when count(p_url)=3 then 'pv3' when count(p_url)=4 then 'pv4' when count(p_url)>=5 and count(p_url)<10 then 'pv5_10' when count(p_url)>=10 and count(p_url)<30 then 'pv10_30' when count(p_url)>=30 and count(p_url)<60 then 'pv30_60' else 'pv60plus' end) as pv from event_logs where en='e_pv' and p_url is not null and pl is not null and s_time>unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by pl,from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd'),u_uid) as tmp insert overwrite table stats_view_depth_tmp select pl,day,pv,count(distinct u_uid) cnt where u_uid is not null group by pl,day,pv"

echo "start insert user data to hive table"
$HIVE_HOME/bin/hive --database default -e "with tmp as (select pl,date,cnt as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv1' union all select pl,date,0 as pv1,cnt as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv2' union all select pl,date,0 as pv1,0 as pv2,cnt as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv3' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,cnt as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv4' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,cnt as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv5_10' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,cnt as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv10_30' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,cnt as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv30_60' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,cnt as pv60plus from stats_view_depth_tmp where col='pv60plus') from tmp insert overwrite table stats_view_depth select platform_convert(pl),date_convert(date),4,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),date group by pl,date"

echo "start insert session date to hive tmp table"
$HIVE_HOME/bin/hive -database default -e "from(select pl,from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd') as day,u_sid,(case when count(p_url)=1 then 'pv1' when count(p_url)=2 then 'pv2' when count(p_url)=3 then 'pv3' when count(p_url)=4 then 'pv4' when count(p_url)>=5 and count(p_url)<10 then 'pv5_10' when count(p_url)>=10 and count(p_url)<30 then 'pv10_30' when count(p_url)>=30 and count(p_url)<60 then 'pv30_60' else 'pv60plus' end) as pv from event_logs where en='e_pv' and p_url is not null and pl is not null and s_time>unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by pl,from_unixtime(cast(s_time/1000 as bigint), 'yyyy-MM-dd'),u_sid) as tmp insert overwrite table stats_view_depth_tmp select pl,day,pv,count(distinct u_sid) cnt where u_sid is not null group by pl,day,pv"

## insert into
echo "start insert session data to hive table"
$HIVE_HOME/bin/hive --datebase default -e "with tmp as (select pl,date,cnt as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv1' union all select pl,date,0 as pv1,cnt as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv2' union all select pl,date,0 as pv1,0 as pv2,cnt as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv3' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,cnt as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv4' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,cnt as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv5_10' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,cnt as pv10_30,0 as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv10_30' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,cnt as pv30_60,0 as pv60plus from stats_view_depth_tmp where col='pv30_60' union all select pl,date,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,cnt as pv60plus from stats_view_depth_tmp where col='pv60plus') from tmp insert into table stats_view_depth select platform_convert(pl),date_convert(date),5,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),date group by pl,date"

## sqoop
echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_view_depth \
--export-dir /user/hive/warehouse/stats_view_depth/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,data_dimension_id,kpi_dimension_id
echo "complete run the view depth job"





