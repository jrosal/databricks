// Databricks notebook source
// MAGIC %sql
// MAGIC /*Initialize*/
// MAGIC set hive.input.dir.recursive=true;
// MAGIC set hive.mapred.supports.subdirectories=true;
// MAGIC set hive.supports.subdirectories=true;
// MAGIC set mapred.input.dir.recursive=true

// COMMAND ----------

// MAGIC %md ###Establish directory connections

// COMMAND ----------

// MAGIC %sql
// MAGIC /*connect to web event directory*/
// MAGIC DROP TABLE IF EXISTS lgwebevent_jan;
// MAGIC CREATE EXTERNAL TABLE lgwebevent_jan(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/01';
// MAGIC 
// MAGIC /*connect to web event directory*/
// MAGIC DROP TABLE IF EXISTS lgwebevent_feb2;
// MAGIC CREATE EXTERNAL TABLE lgwebevent_feb2(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/02';
// MAGIC 
// MAGIC /*connect to web event directory*/
// MAGIC DROP TABLE IF EXISTS lgwebevent_mar;
// MAGIC CREATE EXTERNAL TABLE lgwebevent_mar(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/03';
// MAGIC 
// MAGIC /*connect to basket directory*/
// MAGIC DROP TABLE IF EXISTS lngbasket_jan;
// MAGIC CREATE EXTERNAL TABLE lngbasket_jan(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/01';
// MAGIC 
// MAGIC /*connect to basket directory*/
// MAGIC DROP TABLE IF EXISTS lngbasket_feb;
// MAGIC CREATE EXTERNAL TABLE lngbasket_feb(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/02';
// MAGIC 
// MAGIC /*connect to basket directory*/
// MAGIC DROP TABLE IF EXISTS lngbasket_mar;
// MAGIC CREATE EXTERNAL TABLE lngbasket_mar(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/03';
// MAGIC 
// MAGIC /*connect to app analytics directory*/
// MAGIC DROP TABLE IF EXISTS lgapp_jan;
// MAGIC CREATE EXTERNAL TABLE lgapp_jan(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/01';
// MAGIC 
// MAGIC /*connect to app analytics directory*/
// MAGIC DROP TABLE IF EXISTS lgapp_feb;
// MAGIC CREATE EXTERNAL TABLE lgapp_feb(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/02';
// MAGIC 
// MAGIC /*connect to app analytics directory*/
// MAGIC DROP TABLE IF EXISTS lgapp_mar;
// MAGIC CREATE EXTERNAL TABLE lgapp_mar(log STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY '\t'
// MAGIC LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/03';

// COMMAND ----------

// MAGIC %md ###Cache tables

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from lgwebevent_jan limit 10

// COMMAND ----------

// MAGIC %sql /*cache web events*/
// MAGIC drop table if exists lgwebevent_cache_prod;
// MAGIC cache table lgwebevent_cache_prod 
// MAGIC (select * from lgwebevent_feb2 union
// MAGIC  select * from lgwebevent_mar
// MAGIC )

// COMMAND ----------

// MAGIC %sql /*cache basket events*/
// MAGIC drop table if exists lngbasket_cache_prod;
// MAGIC cache table lngbasket_cache_prod 
// MAGIC (select * from lngbasket_feb union
// MAGIC  select * from lngbasket_mar
// MAGIC )

// COMMAND ----------

// MAGIC %sql /*cache app events*/
// MAGIC drop table if exists lgapp_cache_prod;
// MAGIC cache table lgapp_cache_prod 
// MAGIC (select * from lgapp_feb union
// MAGIC  select * from lgapp_mar
// MAGIC )

// COMMAND ----------

// MAGIC %md ###Define Schemas

// COMMAND ----------

// MAGIC %sql
// MAGIC /*define schemas*/
// MAGIC drop table if exists lgwebevent_cache_dtl_all;
// MAGIC cache table lgwebevent_cache_dtl_all as 
// MAGIC select 
// MAGIC get_json_object(log,'$.eventValues.collectorNumber') as member_id, 
// MAGIC get_json_object(log,'$.eventValues.domain') as domain, 
// MAGIC get_json_object(log,'$.eventValues.language') as language, 
// MAGIC get_json_object(log,'$.eventValues.region') as region, 
// MAGIC get_json_object(log,'$.eventValues.collectorTier') as tier,  
// MAGIC get_json_object(log,'$.eventValues.responsiveScreenMode') as screenMode,  
// MAGIC get_json_object(log,'$.eventValues.offerId') as offer_id, 
// MAGIC get_json_object(log,'$.eventValues.date') as activity_date, 
// MAGIC from_unixtime(substring((get_json_object(log,'$.eventValues.timestamp')),1,10)-(5*60*60)) as activity_datetime,
// MAGIC get_json_object(log,'$.eventValues.offerContext') as offerContext,
// MAGIC get_json_object(log,'$.eventValues.offerPosition') as offerPosition, 
// MAGIC get_json_object(log,'$.eventValues.sponsorName') as sponsorName, 
// MAGIC get_json_object(log,'$.eventValues.sponsorId') as sponsor_id,
// MAGIC get_json_object(log,'$.eventValues.offerTarget') as OfferTarget, 
// MAGIC get_json_object(log,'$.tagType') as tag_type, 
// MAGIC get_json_object(log,'$.categoryId') as category_id,
// MAGIC get_json_object(log,'$.pageId') as page_id,
// MAGIC get_json_object(log,'$.eventTarget') as eventTarget,
// MAGIC get_json_object(log,'$.eventDsi') as event_dsi,
// MAGIC split(get_json_object(log,'$.pageId'),'_')[2] as activityType,
// MAGIC substr(get_json_object(log,'$.eventDsi'),-4) as InventoryCode,
// MAGIC reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) as docket
// MAGIC from lgwebevent_cache_prod
// MAGIC order by activity_datetime desc

// COMMAND ----------

// MAGIC %sql
// MAGIC /*L+G App ELEMENT schema - Android and iOS components in*/
// MAGIC drop table if exists lgappevent_cache_dtl_all;
// MAGIC cache table lgappevent_cache_dtl_all as 
// MAGIC select 
// MAGIC if(split(get_json_object(log,'$.valueTargetId'),'-')[0] is null, get_json_object(log,'$.collectorNumber'), split(get_json_object(log,'$.valueTargetId'),'-')[0]) as member_id_unified,
// MAGIC get_json_object(log,'$.eventType') as eventType,
// MAGIC get_json_object(log,'$.loginLevel') as loginLevel, 
// MAGIC get_json_object(log,'$.deviceModel') as deviceModel,
// MAGIC get_json_object(log,'$.deviceOS') as deviceOS,
// MAGIC get_json_object(log,'$.appName') as appName,
// MAGIC get_json_object(log,'$.appVersion') as appVersion,
// MAGIC get_json_object(log,'$.date') as date, 
// MAGIC if(from_unixtime(substring(get_json_object(log,'$.timeStamp'),1,10)-(5*60*60)) is null, from_unixtime(substring(get_json_object(log,'$.timestamp'),1,10)-(5*60*60)), from_unixtime(substring(get_json_object(log,'$.timeStamp'),1,10)-(5*60*60))) as activity_timestamp_unified,
// MAGIC get_json_object(log,'$.valueOfferPosition') as offerPosition,
// MAGIC get_json_object(log,'$.valueOfferId') as offer_id,
// MAGIC split(get_json_object(log,'$.eventTarget'),'_')[2] as activityType,
// MAGIC get_json_object(log,'$.valueOfferInteractionContext') as interactionContext,
// MAGIC get_json_object(log,'$.valuePartnerName') as partnerName
// MAGIC from lgapp_cache_prod
// MAGIC where get_json_object(log,'$.eventType') = 'element'
// MAGIC order by date desc

// COMMAND ----------

// MAGIC %sql
// MAGIC /*break out result of useTransaction - RESULTS*/
// MAGIC DROP TABLE IF EXISTS lngbasket_usetrans_cache_all;
// MAGIC cache table lngbasket_usetrans_cache_all as 
// MAGIC /*UseTransaction Split - RESULTS*/
// MAGIC   select eventType, store_id, transNumber, datetime, member_id, 
// MAGIC     get_json_object(c.info_object,'$.offerId') as offer_id, 
// MAGIC     get_json_object(c.info_object,'$.issuanceCode') as issuanceCode, 
// MAGIC     get_json_object(c.info_object,'$.miles') as miles, 
// MAGIC     get_json_object(c.info_object,'$.offerDescription') as offerDesc, 
// MAGIC     get_json_object(c.info_object,'$.contributingProductIds') as includedSKUs 
// MAGIC   from
// MAGIC   (
// MAGIC     select eventType, store_id, transNumber, datetime, member_id, info_object
// MAGIC     from
// MAGIC     (
// MAGIC       select eventType, store_id, transNumber, datetime, member_id, 
// MAGIC         split(regexp_replace(regexp_replace(a.info_array_string,'\\}\\,\\{','\\}\\;\\{'),'^\\[|\\]$',''),'\\;') as info_array
// MAGIC       from
// MAGIC       (
// MAGIC         select 
// MAGIC           get_json_object(log,'$.eventType') as eventType, 
// MAGIC           concat(regexp_replace(substring(get_json_object(log,'$.message.transaction.date'),1,11),'T',' '), substring(get_json_object(log,'$.message.transaction.date'),12,8)) as datetime,
// MAGIC           get_json_object(log,'$.message.transaction.storeId') as store_id,
// MAGIC           get_json_object(log,'$.message.transaction.number') as transNumber,
// MAGIC           get_json_object(log,'$.message.transaction.memberId') as member_id,
// MAGIC           get_json_object(log,'$.message.result') as info_array_string
// MAGIC         from lngbasket_cache_prod
// MAGIC         where get_json_object(log,'$.eventType') = 'UseTransaction'
// MAGIC           and get_json_object(log,'$.message.transaction.memberId') is not null 
// MAGIC       ) a
// MAGIC     ) b lateral view explode(b.info_array) info_array_exploded as info_object
// MAGIC   ) c
// MAGIC   order by 2 desc

// COMMAND ----------

// MAGIC %sql
// MAGIC /*break out result of useTransaction - ITEMS - Basket spend summary*/
// MAGIC DROP TABLE IF EXISTS usetransitems_cache_all;
// MAGIC cache table usetransitems_cache_all as 
// MAGIC   select eventType, transType, transNumber, store_id, datetime, member_id, 
// MAGIC     get_json_object(c.info_object,'$.productId') as product_id,
// MAGIC     get_json_object(c.info_object,'$.name') as product_name, 
// MAGIC     get_json_object(c.info_object,'$.tags') as tags, 
// MAGIC     get_json_object(c.info_object,'$.quantity') as quantity, 
// MAGIC     get_json_object(c.info_object,'$.netPrice') as netPrice,
// MAGIC     get_json_object(c.info_object,'$.listPrice') as listPrice
// MAGIC   from
// MAGIC   (
// MAGIC     select eventType, transType, transNumber, store_id, datetime, member_id, info_object
// MAGIC     from
// MAGIC     (
// MAGIC       select eventType, transType, transNumber, store_id, datetime, member_id, 
// MAGIC           split(regexp_replace(regexp_replace(a.info_array_string,'\\}\\,\\{','\\}\\;\\{'),'^\\[|\\]$',''),'\\;') as info_array
// MAGIC       from
// MAGIC       (
// MAGIC         select 
// MAGIC           get_json_object(log,'$.eventType') as eventType, 
// MAGIC           get_json_object(log,'$.message.transaction.type') as transType,
// MAGIC           get_json_object(log,'$.message.transaction.number') as transNumber,
// MAGIC           get_json_object(log,'$.message.transaction.storeId') as store_id,
// MAGIC           concat(regexp_replace(substring(get_json_object(log,'$.message.transaction.date'),1,11),'T',' '), substring(get_json_object(log,'$.message.transaction.date'),12,8)) as datetime,
// MAGIC           get_json_object(log,'$.message.transaction.memberId') as member_id, 
// MAGIC           get_json_object(log,'$.message.transaction.items') as info_array_string
// MAGIC         from lngbasket_cache_prod
// MAGIC         where get_json_object(log,'$.eventType') = 'UseTransaction' /*restricting only to Uses (no Calculates)*/
// MAGIC       ) a
// MAGIC     ) b lateral view explode(b.info_array) info_array_exploded as info_object
// MAGIC   ) c
// MAGIC   order by 3 desc

// COMMAND ----------

// MAGIC %md ###Data summarization for visualization

// COMMAND ----------

// MAGIC %sql
// MAGIC /*show previous day APP activity breakdown hour by hour 24hrs*/
// MAGIC drop table if exists app_activity_summary;
// MAGIC cache table app_activity_summary as 
// MAGIC select 
// MAGIC date_format(activity_timestamp_unified, 'dd-MMM HH') as event_time, 
// MAGIC if(activityType = 'IMPRESSION', 'IMPRESSION', if(activityType = 'DETAILS', 'DETAILS', if(activityType = 'ADDED', 'ADD', ''))) as event_type,
// MAGIC 'APP' as event_source,
// MAGIC count(*) as event_count
// MAGIC from lgappevent_cache_dtl_all
// MAGIC where activityType in ('IMPRESSION','DETAILS','ADDED','ERROR')
// MAGIC   and substring(activity_timestamp_unified,1,10) >= substring(from_unixtime(unix_timestamp()-(18000+(60*60*24))),1,10)
// MAGIC   and substring(activity_timestamp_unified,1,10) <  substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC   /*18000 until Spring, 18000 after daylight savings*/
// MAGIC group by 1,2,3
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC /*show previous day WEB activity breakdown hour by hour 24hrs*/
// MAGIC DROP TABLE IF EXISTS web_activity_summary;
// MAGIC cache table web_activity_summary as 
// MAGIC select 
// MAGIC date_format(activity_datetime, 'dd-MMM HH') as event_time, 
// MAGIC activityType as event_type, 
// MAGIC if(screenMode='PC','WEB-DESKTOP',if(screenMode='Phone','WEB-MOBILE','')) as event_source,
// MAGIC count(*) as event_count
// MAGIC from lgwebevent_cache_dtl_all
// MAGIC where activityType in ('IMPRESSION','DETAILS','ADD')
// MAGIC   and substring(activity_datetime,1,10) >= substring(from_unixtime(unix_timestamp()-(18000+(60*60*24))),1,10)
// MAGIC   and substring(activity_datetime,1,10) <  substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC   /*18000 until Spring, 18000 after daylight savings*/
// MAGIC group by 1,2,3
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC /*web + app activity summary*/
// MAGIC DROP TABLE IF EXISTS web_app_activity_summary;
// MAGIC cache table web_app_activity_summary as 
// MAGIC select event_time, event_type, event_source, sum(event_count) as num_events
// MAGIC from (select * from app_activity_summary union select * from web_activity_summary)
// MAGIC group by 1,2,3
// MAGIC order by 1,2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from web_app_activity_summary where event_type = 'IMPRESSION' order by 1,3

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from web_app_activity_summary where event_type = 'ADD' order by 1,3

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC if(deviceModel like 'iPhone%','iOS',if(deviceModel like 'iPad%','iOS',if(deviceModel like 'iPod%','iOS','Android'))) as deviceSplit, 
// MAGIC count(distinct member_id_unified) as num_collectors, 
// MAGIC count(*) as num_activities 
// MAGIC from lgappevent_cache_dtl_all 
// MAGIC where deviceModel not in ('Simulator')
// MAGIC group by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC   drop table if exists allevents_summary;
// MAGIC   cache table allevents_summary  
// MAGIC   
// MAGIC   /*summarize web-desktop and web-mobile activities...*/
// MAGIC   (select 
// MAGIC   substring(activity_datetime,1,10) as activity_date, 
// MAGIC   if(screenMode = 'PC','3_Web_Desktop','2_Web_Mobile') as screenMode,  
// MAGIC   sum(if(activityType = 'IMPRESSION', 1, 0)) as impressions,
// MAGIC   sum(if(activityType = 'DETAILS', 1, 0)) as detail_views,
// MAGIC   sum(if(activityType = 'ADD', 1, 0)) as offers_added
// MAGIC   from lgwebevent_cache_dtl_all
// MAGIC   where substring(activity_datetime,1,10) <  substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC   group by 1,2
// MAGIC   order by 1,2)
// MAGIC   
// MAGIC   union
// MAGIC   
// MAGIC   /*...and add mobile app activity*/
// MAGIC   (select 
// MAGIC substring(activity_timestamp_unified,1,10) as activity_date,
// MAGIC '1_Mobile_App' as screenMode, 
// MAGIC sum(if(activityType in ('IMPRESSION','IMPRESS'),1,0)) as impressions,
// MAGIC sum(if(activityType = 'DETAILS',1,0)) as detail_views,
// MAGIC sum(if(activityType = 'ADDED',1,0)) as offers_added
// MAGIC /* count(*) as total_activity */
// MAGIC from lgappevent_cache_dtl_all
// MAGIC where member_id_unified > 0
// MAGIC   and offer_id is not null
// MAGIC   and substring(activity_timestamp_unified,1,10) < substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC group by 1,2
// MAGIC order by 1 desc)
// MAGIC 
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC /*Lifetime activity splits*/
// MAGIC select * from allevents_summary order by screenMode

// COMMAND ----------

// MAGIC %sql
// MAGIC /*summary of offer loads - daily by channel*/
// MAGIC select * from allevents_summary order by screenMode, activity_date

// COMMAND ----------

// MAGIC %sql
// MAGIC /*basket summary daily*/ 
// MAGIC drop table if exists use_summary_all;
// MAGIC cache table use_summary_all as 
// MAGIC select substring(datetime,1,10) as date, count(distinct member_id) as num_unique_collectors, 
// MAGIC count(distinct transNumber) as num_lg_transactions, 
// MAGIC count(*) as num_offers_used
// MAGIC from lngbasket_usetrans_cache_all
// MAGIC where eventType = 'UseTransaction'
// MAGIC   and offer_id is not null
// MAGIC   and substring(datetime,1,10) < substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC group by 1
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists date_dim;
// MAGIC cache table date_dim as 
// MAGIC select distinct event_time from web_app_activity_summary order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC /*show previous day USE activity breakdown hour by hour 24hrs*/
// MAGIC drop table if exists use_activity_summary;
// MAGIC cache table use_activity_summary as 
// MAGIC 
// MAGIC select a.event_time, b.event_type, b.event_count
// MAGIC from date_dim a left join 
// MAGIC (
// MAGIC select 
// MAGIC date_format(datetime, 'dd-MMM HH') as event_time, 
// MAGIC 'USE' as event_type,
// MAGIC count(*) as event_count
// MAGIC from lngbasket_usetrans_cache_all
// MAGIC where eventType = 'UseTransaction' 
// MAGIC   and offer_id is not null
// MAGIC   and substring(datetime,1,10) >= substring(from_unixtime(unix_timestamp()-(18000+(60*60*24))),1,10)
// MAGIC   and substring(datetime,1,10) <  substring(from_unixtime(unix_timestamp()-18000),1,10) 
// MAGIC   /*18000 until Spring, 18000 after daylight savings*/
// MAGIC group by 1,2
// MAGIC   ) b on a.event_time = b.event_time
// MAGIC 
// MAGIC order by 1

// COMMAND ----------

// MAGIC %md end of daily update

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave, 
// MAGIC unique_coll_web_viewed,
// MAGIC unique_coll_web_detail_viewed,
// MAGIC unique_coll_web_loaded,
// MAGIC unique_coll_app_viewed,
// MAGIC unique_coll_app_detail_viewed,
// MAGIC unique_coll_app_loaded,
// MAGIC unique_coll_viewed,
// MAGIC unique_coll_loaded,
// MAGIC unique_coll_used
// MAGIC from lg_wave_summary2 
// MAGIC where wave like 'W%'
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave, 
// MAGIC web_impressions,
// MAGIC web_detail_views,
// MAGIC web_offers_loaded,
// MAGIC app_impressions,
// MAGIC app_detail_views,
// MAGIC app_offers_loaded,
// MAGIC total_impressions,
// MAGIC total_detail_views,
// MAGIC num_offers_loaded,
// MAGIC num_offers_used,
// MAGIC total_miles_earned
// MAGIC from lg_wave_summary2 
// MAGIC where wave like 'W%'
// MAGIC order by 1

// COMMAND ----------

// MAGIC %md 
// MAGIC - Collector Load Rate: Loaded/Viewed
// MAGIC - Collector Use Rate: Used/Loaded
// MAGIC - Offer Usage Rate: OffersUsed/OffersLoaded

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave, 
// MAGIC round((unique_coll_loaded/unique_coll_viewed)*100,1) as pct_coll_loaded, 
// MAGIC round((unique_coll_used/unique_coll_loaded)*100,1) as pct_coll_used, 
// MAGIC round((web_offers_loaded/num_offers_loaded)*100,1) as pct_coll_web_loaded, 
// MAGIC round((app_offers_loaded/num_offers_loaded)*100,1) as pct_coll_app_loaded, 
// MAGIC round((num_offers_used/num_offers_loaded)*100,1) as offer_usage_rate
// MAGIC from lg_wave_summary2
// MAGIC where wave like 'W%' 
// MAGIC order by 1

// COMMAND ----------

// MAGIC %md ###Appendix

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave,
// MAGIC web_impressions, 
// MAGIC web_detail_views, 
// MAGIC web_offers_loaded, 
// MAGIC app_impressions, 
// MAGIC app_detail_views, 
// MAGIC app_offers_loaded, 
// MAGIC total_impressions, 
// MAGIC total_detail_views, 
// MAGIC num_offers_loaded, 
// MAGIC num_offers_used, 
// MAGIC total_miles_earned
// MAGIC from lg_wave_summary2 order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave,
// MAGIC unique_coll_web_viewed, 
// MAGIC unique_coll_web_detail_viewed, 
// MAGIC unique_coll_web_loaded, 
// MAGIC unique_coll_app_viewed, 
// MAGIC unique_coll_app_detail_viewed, 
// MAGIC unique_coll_app_loaded, 
// MAGIC unique_coll_viewed, 
// MAGIC unique_coll_detail_viewed, 
// MAGIC unique_coll_loaded, 
// MAGIC unique_coll_used
// MAGIC from lg_wave_summary2 order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave, 
// MAGIC round((unique_coll_loaded/unique_coll_viewed)*100,1) as coll_load_penetration, 
// MAGIC round((unique_coll_used/unique_coll_loaded)*100,1) as coll_used_penetration, 
// MAGIC round((unique_coll_web_loaded/unique_coll_web_viewed)*100,1) as coll_web_load_penetration, 
// MAGIC round((unique_coll_app_loaded/unique_coll_app_viewed)*100,1) as coll_app_load_penetration
// MAGIC from lg_wave_summary2 
// MAGIC order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select wave, 
// MAGIC round((unique_coll_loaded/unique_coll_viewed)*100,1) as pct_coll_loaded, 
// MAGIC round((unique_coll_used/unique_coll_loaded)*100,1) as pct_coll_used, 
// MAGIC round((web_offers_loaded/num_offers_loaded)*100,1) as pct_coll_web_loaded, 
// MAGIC round((app_offers_loaded/num_offers_loaded)*100,1) as pct_coll_app_loaded, 
// MAGIC round((num_offers_used/num_offers_loaded)*100,1) as offer_usage_rate
// MAGIC from lg_wave_summary2
// MAGIC where wave like 'W%' 
// MAGIC order by 1