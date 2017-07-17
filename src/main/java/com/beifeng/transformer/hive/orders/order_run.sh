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
echo "start run of order job "

echo "start run order request job "
echo "start order numbers"
$HIVE_HOME/bin/hive --database default -e "from(select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,cut,pt,count(distinct oid) as cnt from event_logs where en='e_cre' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,cnt insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cnt,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as orders,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created

echo "start order amount "
$HIVE_HOME/bin/hive --database default -e "from(from(select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,cut,pt,oid,max(cua) as amount from event_logs where en='e_cre' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt,oid) as tmp1 select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,order_amount,created

echo "start order info job "
$HIVE_HOME/bin/hive --database default -e "from event_logs insert overwrite table order_info select oid,pl,s_time,cut,pt,cua where en='e_cre' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table order_info \
--export-dir /user/hive/warehouse/order_info/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key order_id

echo "start success order job "
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(select order_info(oid,'pl') as pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,count(distinct oid) as orders from event_logs where en='e_cs' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt')) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,orders insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,success_orders,created

echo "start success order amount "
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(from(select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cs' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp1 select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,revenue_amount,created

echo "start total order amount to this day "
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(from(select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cs' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp1 select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'revenue') as amount,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),'revenue') as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'revenue') as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'revenue') as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'revenue') as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,total_revenue_amount,created

echo "start refund order job"
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,count(distinct oid) as orders from event_logs where en='e_cr' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt')) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,orders insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_orders,created

echo "start refund order amount job "
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(from(select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cr' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp1 select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_amount,created

echo "start total refund order amount to this day "
$HIVE_HOME/bin/hive --database default -e "add jar /home/mycentos/opt/cdh/hive-0.13.1-cdh5.3.6/lib/mysql-connector-java-5.1.42-bin.jar;from(from(select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cr' and pl is not null and s_time>=unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time<unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp1 select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'refund') as amount,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,date group by date,cut,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,date group by pl,date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,date group by pl,date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,date group by date,pt;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,date group by date,cut;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,date group by pl,date;from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(date),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,date group by date;exit;"

echo "run the sqoop script,insert hive data to mysql table"
$SQOOP_HOME/bin/sqoop export --connect jdbc:mysql://hadoop:3306/report?useSSL=false \
--username root \
--password root \
--table stats_order \
--export-dir /user/hive/warehouse/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,total_revenue_amount,created