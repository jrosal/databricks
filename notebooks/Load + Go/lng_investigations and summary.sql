-- Databricks notebook source
/*Initialize*/
set hive.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.supports.subdirectories=true;
set mapred.input.dir.recursive=true

-- COMMAND ----------

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_oct;
CREATE EXTERNAL TABLE lgwebevent_oct(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2016/10';

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_nov;
CREATE EXTERNAL TABLE lgwebevent_nov(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2016/11';

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_dec;
CREATE EXTERNAL TABLE lgwebevent_dec(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2016/12';

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_jan;
CREATE EXTERNAL TABLE lgwebevent_jan(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/01';

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_feb2;
CREATE EXTERNAL TABLE lgwebevent_feb2(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/02';

/*connect to web event directory*/
DROP TABLE IF EXISTS lgwebevent_mar;
CREATE EXTERNAL TABLE lgwebevent_mar(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/loadandgo-web-event/2017/03';

-- COMMAND ----------

-- MAGIC %md ####start to cache data to memory - approximate run time: 1h 10mins (compute optimized, 120GB)

-- COMMAND ----------

/*join data together - all months*/
drop table if exists lgwebevent_cache_all;
cache table lgwebevent_cache_all 
(
 select * from lgwebevent_oct union 
 select * from lgwebevent_nov union 
 select * from lgwebevent_dec union 
 select * from lgwebevent_jan union
 select * from lgwebevent_feb2 union
select * from lgwebevent_mar
)

-- COMMAND ----------

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_oct;
CREATE EXTERNAL TABLE lgapp_oct(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2016/10';

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_nov;
CREATE EXTERNAL TABLE lgapp_nov(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2016/11';

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_dec;
CREATE EXTERNAL TABLE lgapp_dec(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2016/12';

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_jan;
CREATE EXTERNAL TABLE lgapp_jan(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/01';

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_feb;
CREATE EXTERNAL TABLE lgapp_feb(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/02';

/*connect to app analytics directory*/
DROP TABLE IF EXISTS lgapp_mar;
CREATE EXTERNAL TABLE lgapp_mar(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/load-and-go-app-analytics/2017/03';

-- COMMAND ----------

/*join data together - all months*/
drop table if exists lgapp_cache_all;
cache table lgapp_cache_all 
(
 select log from lgapp_oct union 
 select log from lgapp_nov union
 select log from lgapp_dec union 
 select log from lgapp_jan union
 select log from lgapp_feb union
 select log from lgapp_mar
)

-- COMMAND ----------

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_oct;
CREATE EXTERNAL TABLE lngbasket_oct(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2016/10';

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_nov;
CREATE EXTERNAL TABLE lngbasket_nov(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2016/11';

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_dec;
CREATE EXTERNAL TABLE lngbasket_dec(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2016/12';

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_jan;
CREATE EXTERNAL TABLE lngbasket_jan(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/01';

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_feb;
CREATE EXTERNAL TABLE lngbasket_feb(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/02';

/*connect to basket directory*/
DROP TABLE IF EXISTS lngbasket_mar;
CREATE EXTERNAL TABLE lngbasket_mar(log STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3a://AKIAJAAZXWPTD27QBWSQ:XQuxTQXWApe4yowHe0y8ixc7ZJ2SFWvkhHWYSkTH@prod-dh01-storage/basket/2017/03';

-- COMMAND ----------

/*join data together - all months*/
drop table if exists lngbasket_cache_all;
cache table lngbasket_cache_all 
(
 select log from lngbasket_oct union 
 select log from lngbasket_nov union
 select log from lngbasket_dec union 
 select log from lngbasket_jan union 
 select log from lngbasket_feb union 
 select log from lngbasket_mar
)

-- COMMAND ----------

-- MAGIC %md ####start of schema detailing

-- COMMAND ----------

/*inventory code dim*/
drop table if exists web_inventory_codes;
cache table web_inventory_codes as
select 
get_json_object(log,'$.eventValues.offerId') as offer_id, 
max(if(
  reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564a','W01: Oct07-Oct20',if(
    reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564b','W02: Oct21-Nov03',if(
      reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564c','W03: Nov04-Nov17',if(
        reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564d','W04: Nov18-Dec01',if(
          reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564e','W05: Dec02-Dec15',if(
            reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564f','W06: Dec16-Dec29',if(
              reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564g','W07: Dec30-Jan12',if(
                reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '31564h','W08: Jan13-Jan26',if(
                  reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '33108','W09: Jan27-Feb09',if(
                    reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '33158','W10: Feb10-Feb23',if(
                      reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6)) = '33160','W11: Feb24-Mar09','')))))))))))) as wave, 
max(reverse(substring(reverse(get_json_object(log,'$.eventDsi')),9,6))) as docket,
max(substr(get_json_object(log,'$.eventDsi'),-4)) as InventoryCode
from lgwebevent_cache_all
group by 1
order by 2,1
/*null inventoryCode = global offer*/

-- COMMAND ----------

/*define schemas*/
/*lgwebevent_cache_dtl_all*/
drop table if exists lgwebevent_cache_dtl_all;
cache table lgwebevent_cache_dtl_all as 
select 
get_json_object(log,'$.eventValues.collectorNumber') as member_id, 
get_json_object(log,'$.eventValues.domain') as domain, 
get_json_object(log,'$.eventValues.language') as language, 
get_json_object(log,'$.eventValues.region') as region, 
get_json_object(log,'$.eventValues.collectorTier') as tier,  
get_json_object(log,'$.eventValues.responsiveScreenMode') as screenMode,  
get_json_object(log,'$.eventValues.offerId') as offer_id, 
get_json_object(log,'$.eventValues.date') as activity_date, 
from_unixtime(substring((get_json_object(log,'$.eventValues.timestamp')),1,10)-(5*60*60)) as activity_datetime,
get_json_object(log,'$.eventValues.offerContext') as offerContext,
get_json_object(log,'$.eventValues.offerPosition') as offerPosition, 
get_json_object(log,'$.eventValues.sponsorName') as sponsorName, 
get_json_object(log,'$.eventValues.sponsorId') as sponsor_id,
get_json_object(log,'$.eventValues.offerTarget') as OfferTarget, 
get_json_object(log,'$.tagType') as tag_type, 
get_json_object(log,'$.categoryId') as category_id,
get_json_object(log,'$.pageId') as page_id,
get_json_object(log,'$.eventTarget') as eventTarget,
get_json_object(log,'$.eventDsi') as event_dsi,
split(get_json_object(log,'$.pageId'),'_')[2] as activityType
from lgwebevent_cache_all
order by activity_datetime desc

-- COMMAND ----------

drop table if exists lgwebevent_cache_dtl_all_inv;
cache table lgwebevent_cache_dtl_all_inv as  
select a.*, b.wave, b.docket, b.inventoryCode 
from lgwebevent_cache_dtl_all a left join web_inventory_codes b on a.offer_id = b.offer_id
/*null inventoryCode = global offer*/

-- COMMAND ----------

/*L+G App ELEMENT schema - Android and iOS components in*/
drop table if exists lgappevent_cache_dtl_all;
cache table lgappevent_cache_dtl_all as 
select 
if(split(get_json_object(log,'$.valueTargetId'),'-')[0] is null, get_json_object(log,'$.collectorNumber'), split(get_json_object(log,'$.valueTargetId'),'-')[0]) as member_id_unified,
get_json_object(log,'$.eventType') as eventType,
get_json_object(log,'$.loginLevel') as loginLevel, 
get_json_object(log,'$.deviceModel') as deviceModel,
get_json_object(log,'$.deviceOS') as deviceOS,
get_json_object(log,'$.appName') as appName,
get_json_object(log,'$.appVersion') as appVersion,
get_json_object(log,'$.date') as date, 
if(from_unixtime(substring(get_json_object(log,'$.timeStamp'),1,10)-(5*60*60)) is null, from_unixtime(substring(get_json_object(log,'$.timestamp'),1,10)-(5*60*60)), from_unixtime(substring(get_json_object(log,'$.timeStamp'),1,10)-(5*60*60))) as activity_timestamp_unified,
get_json_object(log,'$.valueOfferPosition') as offerPosition,
get_json_object(log,'$.valueOfferId') as offer_id,
split(get_json_object(log,'$.eventTarget'),'_')[2] as activityType,
get_json_object(log,'$.valueOfferInteractionContext') as interactionContext,
get_json_object(log,'$.valuePartnerName') as partnerName
from lgapp_cache_all
where get_json_object(log,'$.eventType') = 'element'
order by date desc

-- COMMAND ----------

drop table if exists lgappevent_cache_dtl_all_inv;
cache table lgappevent_cache_dtl_all_inv as  
select a.*, b.docket, b.wave, b.inventoryCode 
from lgappevent_cache_dtl_all a left join web_inventory_codes b on a.offer_id = b.offer_id
/*null inventoryCode = global offer*/

-- COMMAND ----------

/*break out result of useTransaction - RESULTS*/
DROP TABLE IF EXISTS lngbasket_usetrans_cache_all;
cache table lngbasket_usetrans_cache_all as 
/*UseTransaction Split - RESULTS*/
  select eventType, store_id, transNumber, datetime, member_id, 
    get_json_object(c.info_object,'$.offerId') as offer_id, 
    get_json_object(c.info_object,'$.issuanceCode') as issuanceCode, 
    get_json_object(c.info_object,'$.miles') as miles, 
    get_json_object(c.info_object,'$.offerDescription') as offerDesc, 
    get_json_object(c.info_object,'$.contributingProductIds') as includedSKUs 
  from
  (
    select eventType, store_id, transNumber, datetime, member_id, info_object
    from
    (
      select eventType, store_id, transNumber, datetime, member_id, 
        split(regexp_replace(regexp_replace(a.info_array_string,'\\}\\,\\{','\\}\\;\\{'),'^\\[|\\]$',''),'\\;') as info_array
      from
      (
        select 
          get_json_object(log,'$.eventType') as eventType, 
          concat(regexp_replace(substring(get_json_object(log,'$.message.transaction.date'),1,11),'T',' '), substring(get_json_object(log,'$.message.transaction.date'),12,8)) as datetime,
          get_json_object(log,'$.message.transaction.storeId') as store_id,
          get_json_object(log,'$.message.transaction.number') as transNumber,
          get_json_object(log,'$.message.transaction.memberId') as member_id,
          get_json_object(log,'$.message.result') as info_array_string
        from lngbasket_cache_all
        where get_json_object(log,'$.eventType') = 'UseTransaction'
          and get_json_object(log,'$.message.transaction.memberId') is not null 
      ) a
    ) b lateral view explode(b.info_array) info_array_exploded as info_object
  ) c
  order by 2 desc

-- COMMAND ----------

/*break out result of useTransaction - ITEMS - Basket spend summary*/
DROP TABLE IF EXISTS usetransitems_cache_all;
cache table usetransitems_cache_all as 
  select eventType, transType, transNumber, store_id, datetime, member_id, 
    get_json_object(c.info_object,'$.productId') as product_id,
    get_json_object(c.info_object,'$.name') as product_name, 
    get_json_object(c.info_object,'$.tags') as tags, 
    get_json_object(c.info_object,'$.quantity') as quantity, 
    get_json_object(c.info_object,'$.netPrice') as netPrice,
    get_json_object(c.info_object,'$.listPrice') as listPrice
  from
  (
    select eventType, transType, transNumber, store_id, datetime, member_id, info_object
    from
    (
      select eventType, transType, transNumber, store_id, datetime, member_id, 
          split(regexp_replace(regexp_replace(a.info_array_string,'\\}\\,\\{','\\}\\;\\{'),'^\\[|\\]$',''),'\\;') as info_array
      from
      (
        select 
          get_json_object(log,'$.eventType') as eventType, 
          get_json_object(log,'$.message.transaction.type') as transType,
          get_json_object(log,'$.message.transaction.number') as transNumber,
          get_json_object(log,'$.message.transaction.storeId') as store_id,
          concat(regexp_replace(substring(get_json_object(log,'$.message.transaction.date'),1,11),'T',' '), substring(get_json_object(log,'$.message.transaction.date'),12,8)) as datetime,
          get_json_object(log,'$.message.transaction.memberId') as member_id, 
          get_json_object(log,'$.message.transaction.items') as info_array_string
        from lngbasket_cache_all
/*         where get_json_object(log,'$.eventType') = 'UseTransaction'  -- opening to Calculates*/
      ) a
    ) b lateral view explode(b.info_array) info_array_exploded as info_object
  ) c
  order by 3 desc

-- COMMAND ----------

-- MAGIC %md ####start of summary tables - investigation > lookup

-- COMMAND ----------

/*activity summary prelim*/
drop table if exists lngwebevent_summary2;
cache table lngwebevent_summary2 as  
select 
wave, docket, inventoryCode, member_id, offer_id, 'WEB' as screenMode, 
sum(if(activityType = 'IMPRESSION', 1, 0)) as impressions,
sum(if(activityType = 'DETAILS', 1, 0)) as detail_views,
sum(if(activityType = 'ADD', 1, 0)) as added_offer,
min(if(activityType = 'IMPRESSION', activity_datetime, 'null')) as min_view_time,
max(activity_datetime) as max_view_time,
min(if(activityType = 'ADD', activity_datetime, 'null')) as offer_load_time
from lgwebevent_cache_dtl_all_inv
where member_id is not null
group by 1,2,3,4,5,6
order by 1 desc

-- COMMAND ----------

/*activity summary prelim*/
drop table if exists lngappevent_summary2;
cache table lngappevent_summary2 as  
select 
wave, docket, inventoryCode, member_id_unified as member_id, offer_id, 'APP' as screenMode, 
sum(if(activityType in ('IMPRESSION','IMPRESS'), 1, 0)) as impressions,
sum(if(activityType = 'DETAILS', 1, 0)) as detail_views,
sum(if(activityType = 'ADDED', 1, 0)) as added_offer,
min(if(activityType in ('IMPRESSION','IMPRESS'), activity_timestamp_unified, 'null')) as min_view_time,
max(activity_timestamp_unified) as max_view_time,
min(if(activityType = 'ADDED', activity_timestamp_unified, 'null')) as offer_load_time
from lgappevent_cache_dtl_all_inv
where eventType = 'element'
  and member_id_unified > 0
  and offer_id is not null
group by 1,2,3,4,5,6
order by 1,2,3 desc

-- COMMAND ----------

/*allevents - web + app, grouped by collector, offer*/
drop table if exists allevent_summary2;
cache table allevent_summary2 as 
select wave, docket, inventoryCode, offer_id, member_id, screenMode, 
min(min_view_time) as min_view_time,
max(max_view_time) as max_view_time,
min(offer_load_time) as offer_load_time,  
sum(impressions) as impressions, 
sum(detail_views) as detail_views, 
sum(added_offer) as offers_added
from (
  select * from lngwebevent_summary2 union 
  select * from lngappevent_summary2
) 
group by 1,2,3,4,5,6
order by 4,5,6

-- COMMAND ----------

/* offer usage summary */
drop table if exists use_summary_cache;
cache table use_summary_cache as 
select offer_id, member_id, store_id, transNumber, offerDesc, issuancecode, datetime as trans_datetime, count(offer_id) as offers_used, sum(miles) as offer_miles_earned
from lngbasket_usetrans_cache_all
  where offer_id is not null
/*   and datetime >= '2016-12-31 00:00:00.000' */
group by 1,2,3,4,5,6,7
order by 2,1

-- COMMAND ----------

drop table if exists use_summary_cache_inv;
cache table use_summary_cache_inv as 
select b.wave, b.docket, b.inventoryCode, a.*
from use_summary_cache a left join web_inventory_codes b on a.offer_id = b.offer_id
order by a.member_id, a.trans_datetime

-- COMMAND ----------

/*basket spend summary*/
drop table if exists spend_summary_cache;
cache table spend_summary_cache as 
select eventType, transType, member_id, store_id, transNumber, datetime as trans_datetime, sum(quantity) as total_items, sum(netPrice) as total_spend
from usetransitems_cache_all
/* where datetime >= '2016-12-31 00:00:00.000' */
group by 1,2,3,4,5,6
order by 3,4,5,6

-- COMMAND ----------

/*dedupe use and calculates*/
drop table if exists spend_summary_cache2; 
cache table spend_summary_cache2 as 
select distinct member_id, store_id, transNumber, trans_datetime, total_items, total_spend
from spend_summary_cache
order by 1,2,3,4

-- COMMAND ----------

/*persist tables for lookups - write to Hive Metastore*/
drop table if exists lg_lookup_allevent_summary;
create table lg_lookup_allevent_summary as 
select * from allevent_summary2;

drop table if exists lg_lookup_spend_summary;
create table lg_lookup_spend_summary as 
select * from spend_summary_cache;

drop table if exists lg_lookup_items_summary;
create table lg_lookup_items_summary as 
select * from usetransitems_cache_all;

drop table if exists lg_lookup_offeruse_summary;
create table lg_lookup_offeruse_summary as 
select * from use_summary_cache_inv;

refresh lg_lookup_allevent_summary;
refresh lg_lookup_spend_summary;
refresh lg_lookup_items_summary;
refresh lg_lookup_offeruse_summary;

-- COMMAND ----------

/*test query for latest records*/
select * from lg_lookup_allevent_summary order by 8 desc limit 10

-- COMMAND ----------

-- MAGIC %md ####start of LG wave summaries for Rexall A&I

-- COMMAND ----------

/*activity summary prelim*/
drop table if exists lngwebevent_summary;
cache table lngwebevent_summary as  
select 
wave, docket, inventoryCode, member_id, offer_id, 
sum(if(activityType = 'IMPRESSION' and offerContext = 'MyOffers', 1, 0)) as ViewOn_OfferPage,
sum(if(activityType = 'IMPRESSION' and offerContext = 'LoadedOffers', 1, 0)) as ViewOn_LoadedPage,
sum(if(activityType = 'IMPRESSION' and offerContext = 'RedeemedOffers', 1, 0)) as ViewOn_RedeemedPage,
sum(if(activityType = 'IMPRESSION' and offerContext = 'PartnerList', 1, 0)) as ViewOn_PartnerList,
sum(if(activityType = 'IMPRESSION' and offerContext = 'ExpiredOffers', 1, 0)) as ViewOn_ExpiredPage,
sum(if(activityType = 'IMPRESSION' and offerContext = 'HowItWorks', 1, 0)) as ViewOn_HowItWorks,
sum(if(activityType = 'IMPRESSION', 1, 0)) as web_Impressions,
sum(if(activityType = 'DETAILS', 1, 0)) as web_Detail_Views,
sum(if(activityType = 'ADD', 1, 0)) as web_Added_Offer,
min(if(activityType = 'ADD', activity_datetime, 'null')) as web_Added_Time
from lgwebevent_cache_dtl_all_inv
group by 1,2,3,4,5
order by 4 desc

-- COMMAND ----------

drop table if exists lngappevent_summary;
cache table lngappevent_summary as 
select 
wave, docket, inventoryCode, member_id_unified as member_id, offer_id, 
sum(if(activityType in ('IMPRESSION','IMPRESS') and interactionContext = 'HOME SCREEN', 1, 0)) as appViewOn_HomeScreen,
sum(if(activityType in ('IMPRESSION','IMPRESS') and interactionContext = 'CARD SCREEN', 1, 0)) as appViewOn_CardScreen,
sum(if(activityType in ('IMPRESSION','IMPRESS') and interactionContext = 'USED EXPIRED', 1, 0)) as appViewOn_UsedExpired,
sum(if(activityType in ('IMPRESSION','IMPRESS'), 1, 0)) as app_impressions,
sum(if(activityType = 'DETAILS', 1, 0)) as app_Detail_Views,
sum(if(activityType = 'ADDED', 1, 0)) as app_Added_Offer,
min(if(activityType = 'ADDED', activity_timestamp_unified, 'null')) as app_Added_Time
from lgappevent_cache_dtl_all_inv
where eventType = 'element'
  and member_id_unified > 0
  and offer_id is not null
group by 1,2,3,4,5
order by member_id, offer_id

-- COMMAND ----------

/*allevents web+app, grouped by wave, collector, offer with page columns*/
drop table if exists allevent_summary;
cache table allevent_summary as 
select 
if(b.wave is not null, b.wave, d.wave) as wave, 
if(b.docket is not null, b.docket, d.docket) as docket, 
if(b.member_id is not null, b.member_id, d.member_id) as account_number,
if(b.inventoryCode is not null, b.inventoryCode, d.inventoryCode) as inventoryCode, 
if(b.offer_id is not null, b.offer_id,d.offer_id) as offer_id,
b.viewOn_OfferPage, b.viewOn_LoadedPage, b.viewOn_RedeemedPage, b.web_impressions, b.web_detail_views, b.web_added_offer, if(b.web_added_time = 0,'null',b.web_added_time) as web_added_time, d.appViewOn_HomeScreen, d.appViewOn_CardScreen, d.appViewOn_UsedExpired, d.app_impressions, d.app_detail_views, d.app_added_offer, if(d.app_added_time = 0,'null',d.app_added_time) as app_added_time
from lngwebevent_summary b full outer join lngappevent_summary d on b.member_id = d.member_id and b.offer_id = d.offer_id and b.wave = d.wave
order by 1,3,5

-- COMMAND ----------

/*join event data with usage data*/
drop table if exists add_use;
cache table add_use as 
select 
if(b.wave is not null, b.wave, d.wave) as wave, 
if(b.docket is not null, b.docket, d.docket) as docket,
if(b.account_number > 0, b.account_number, d.member_id) as account_number, 
if(b.inventoryCode is not null, b.inventoryCode, d.inventoryCode) as inventoryCode,
if(b.offer_id is not null, b.offer_id, d.offer_id) as offer_id,
b.viewOn_OfferPage, b.viewOn_LoadedPage, b.viewOn_RedeemedPage, 
if(b.web_impressions is not null,b.web_impressions,0) as web_impressions, 
if(b.web_detail_views is not null,b.web_detail_views,0) as web_detail_views, 
if(b.web_added_offer is not null,b.web_added_offer,0) as web_added_offer, b.web_added_time, b.appViewOn_HomeScreen, b.appViewOn_CardScreen, b.appViewOn_UsedExpired, 
if(b.app_impressions is not null,b.app_impressions,0) as app_impressions, 
if(b.app_detail_views is not null,b.app_detail_views,0) as app_detail_views, 
if(b.app_added_offer is not null,b.app_added_offer,0) as app_added_offer, b.app_added_time, d.trans_datetime as offer_use_time, 
if(d.offer_miles_earned is not null,d.offer_miles_earned,0) as miles, d.offerDesc, d.issuanceCode
from allevent_summary b
    full outer join use_summary_cache_inv d on b.account_number = d.member_id and b.offer_id = d.offer_id and b.wave = d.wave
order by 1,3,5

-- COMMAND ----------

/*create exportable final output*/
drop table if exists lg_final_output;
create table lg_final_output as 
select * from add_use
/* choose wave for output */

-- COMMAND ----------

-- MAGIC %md ####start of creating/updating persistent wave summary table

-- COMMAND ----------

/*Summarize Waves - total activity*/
drop table if exists lg_activity_summary;
cache table lg_activity_summary as 
select wave, 
sum(web_impressions) as web_impressions,
sum(web_detail_views) as web_detail_views,
sum(web_added_offer) as web_offers_loaded,
sum(app_impressions) as app_impressions,
sum(app_detail_views) as app_detail_views,
sum(app_added_offer) as app_offers_loaded,
sum(web_impressions+app_impressions) as total_impressions,
sum(web_detail_views+app_detail_views) as total_detail_views,
sum(web_added_offer+app_added_offer) as num_offers_loaded,
sum(if(offer_use_time is not null,1,0)) as num_offers_used,
sum(miles) as total_miles_earned
from add_use
where wave like 'W%' /*or specify wave(s) to include*/
group by 1
order by 1

-- COMMAND ----------

/*Summarize Waves - collector-level metrics*/
drop table if exists lg_coll_summary;
cache table lg_coll_summary as 
select wave,  
sum(web_viewed) as unique_coll_web_viewed, 
sum(web_detail_viewed) as unique_coll_web_detail_viewed, 
sum(web_loaded_offer) as unique_coll_web_loaded, 
sum(app_viewed) as unique_coll_app_viewed, 
sum(app_detail_viewed) as unique_coll_app_detail_viewed, 
sum(app_loaded_offer) as unique_coll_app_loaded, 
sum(viewed) as unique_coll_viewed, 
sum(detail_viewed) as unique_coll_detail_viewed, 
sum(loaded_offer) as unique_coll_loaded, 
sum(used_offer) as unique_coll_used
from (
  select wave, account_number, 
  if(sum(web_impressions)>0,1,0) as web_viewed, 
  if(sum(web_detail_views)>0,1,0) as web_detail_viewed,
  if(sum(web_added_offer)>0,1,0) as web_loaded_offer,
  if(sum(app_impressions)>0,1,0) as app_viewed, 
  if(sum(app_detail_views)>0,1,0) as app_detail_viewed,
  if(sum(app_added_offer)>0,1,0) as app_loaded_offer,
  if(sum(web_impressions)+sum(app_impressions)>0,1,0) as viewed, 
  if(sum(web_detail_views)+sum(app_detail_views)>0,1,0) as detail_viewed,
  if(sum(web_added_offer)+sum(app_added_offer)>0,1,0) as loaded_offer,
  if(sum(if(offer_use_time is not null,1,0))>0,1,0) as used_offer
  from add_use
  where wave like 'W%' /*or specify wave(s) to include*/
group by 1,2
)
group by 1
order by 1

-- COMMAND ----------

/*Summarize Waves - total activity*/
drop table if exists lg_activity_summary_ltd;
cache table lg_activity_summary_ltd as 
select 'All Waves' as wave, 
sum(web_impressions) as web_impressions,
sum(web_detail_views) as web_detail_views,
sum(web_added_offer) as web_offers_loaded,
sum(app_impressions) as app_impressions,
sum(app_detail_views) as app_detail_views,
sum(app_added_offer) as app_offers_loaded,
sum(web_impressions+app_impressions) as total_impressions,
sum(web_detail_views+app_detail_views) as total_detail_views,
sum(web_added_offer+app_added_offer) as num_offers_loaded,
sum(if(offer_use_time is not null,1,0)) as num_offers_used,
sum(miles) as total_miles_earned
from add_use
where wave like 'W%' /*or specify wave(s) to include*/
group by 1
order by 1

-- COMMAND ----------

/*Summarize Waves - collector-level metrics*/
drop table if exists lg_coll_summary_ltd;
cache table lg_coll_summary_ltd as 
select wave, 
sum(web_viewed) as unique_coll_web_viewed, 
sum(web_detail_viewed) as unique_coll_web_detail_viewed, 
sum(web_loaded_offer) as unique_coll_web_loaded, 
sum(app_viewed) as unique_coll_app_viewed, 
sum(app_detail_viewed) as unique_coll_app_detail_viewed, 
sum(app_loaded_offer) as unique_coll_app_loaded, 
sum(viewed) as unique_coll_viewed, 
sum(detail_viewed) as unique_coll_detail_viewed, 
sum(loaded_offer) as unique_coll_loaded, 
sum(used_offer) as unique_coll_used
from (
  select 'All Waves' as wave, account_number, 
  if(sum(web_impressions)>0,1,0) as web_viewed, 
  if(sum(web_detail_views)>0,1,0) as web_detail_viewed,
  if(sum(web_added_offer)>0,1,0) as web_loaded_offer,
  if(sum(app_impressions)>0,1,0) as app_viewed, 
  if(sum(app_detail_views)>0,1,0) as app_detail_viewed,
  if(sum(app_added_offer)>0,1,0) as app_loaded_offer,
  if(sum(web_impressions)+sum(app_impressions)>0,1,0) as viewed, 
  if(sum(web_detail_views)+sum(app_detail_views)>0,1,0) as detail_viewed,
  if(sum(web_added_offer)+sum(app_added_offer)>0,1,0) as loaded_offer,
  if(sum(if(offer_use_time is not null,1,0))>0,1,0) as used_offer
  from add_use
  where wave like 'W%' /*or specify wave(s) to include*/
group by 1,2
)
group by 1
order by 1

-- COMMAND ----------

-- MAGIC %md ####let lg_wave_summaries represent archived fixed wave summaries
-- MAGIC ####let lg_wave_summary2 be a the table above plus the current wave running
-- MAGIC 
-- MAGIC ####update lg_wave_summaries when we want to fix a completed wave to it

-- COMMAND ----------

/*L+G COLLECTOR AND ACTIVITY SUMMARY BY WAVE - write to Hive table*/
drop table if exists lg_wave_summary2;
create table lg_wave_summary2 as 
( select a.*, b.web_impressions, b.web_detail_views, b.web_offers_loaded, b.app_impressions, b.app_detail_views, b.app_offers_loaded, 
  b.total_impressions, b.total_detail_views, b.num_offers_loaded, b.num_offers_used, b.total_miles_earned
  from lg_coll_summary a join lg_activity_summary b on a.wave = b.wave
 union 
  select a.*, b.web_impressions, b.web_detail_views, b.web_offers_loaded, b.app_impressions, b.app_detail_views, b.app_offers_loaded, 
  b.total_impressions, b.total_detail_views, b.num_offers_loaded, b.num_offers_used, b.total_miles_earned
  from lg_coll_summary_ltd a join lg_activity_summary_ltd b on a.wave = b.wave /*LTD*/
) 

-- COMMAND ----------

refresh table lg_wave_summary2;
select * from lg_wave_summary2 order by wave

-- COMMAND ----------

-- MAGIC %md library of calculations to waves - TBD

-- COMMAND ----------

/*rates of activity*/
select wave, round((unique_coll_loaded/unique_coll_viewed)*100,1) as pct_coll_loaded, round((unique_coll_used/unique_coll_viewed)*100,1) as pct_coll_used, 
round((web_offers_loaded/num_offers_loaded)*100,1) as pct_coll_web_loaded, round((app_offers_loaded/num_offers_loaded)*100,1) as pct_coll_app_loaded, 
round((num_offers_used/num_offers_loaded)*100,1) as offer_usage_rate
from lg_wave_summary2
order by 1