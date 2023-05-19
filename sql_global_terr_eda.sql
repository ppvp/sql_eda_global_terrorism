/*
 * 
 * Parsing, normalizing, cleaning
 * 
 */

--- Finding, replacing 0 values in iday & imonth columns with 1

select * from "global_terr".globalterrorismdb
where iday < 1

update "global_terr".globalterrorismdb
set iday = 1
where iday = 0

update "global_terr".globalterrorismdb
set imonth = 1
where imonth = 0

alter table "global_terr".globalterrorismdb
add column date date

alter table "global_terr".globalterrorismdb 
alter column iday type text

alter table "global_terr".globalterrorismdb 
alter column imonth type text

alter table "global_terr".globalterrorismdb 
alter column iyear type text

UPDATE "global_terr".globalterrorismdb 
SET date = CAST(iday || '/' || imonth || '/' || iyear AS DATE)


--- No duplicate entries found

with duplicates as (
select eventid, date, nperps, motive, target1, attacktype1_txt, success, suicide, multiple, latitude, longitude, city, country,
row_number() over (partition by eventid, date, nperps, motive, target1, attacktype1_txt, success, suicide, multiple, latitude, longitude, city, country)
from "global_terr".globalterrorismdb
)
select * from duplicates
where row_number > 1

/*
 * 
 * EDA
 * 
 */

--- Querying for GEODATA

select  eventid, 
		date, 
		latitude, 
		longitude, 
		city, 
		country_txt,
		region_txt,
		case when region_txt = 'Central Asia'
			 or region_txt = 'East Asia'
			 or region_txt = 'South Asia'
			 or region_txt = 'Southeast Asia'
			 then 'Asia'
			 when region_txt = 'Eastern Europe'
			 or region_txt = 'Western Europe'
			 then 'Europe'
			 when region_txt = 'Australasia & Oceania'
			 then 'Australia'
			 when region_txt = 'North America'
			 	or region_txt = 'Central America & Caribbean'
			 then 'North America'
			 when region_txt = 'South America'
			 then 'South America'
			 when region_txt = 'Middle East & North Africa'
			 	or region_txt = 'Sub-Saharan Africa'
			 then 'Africa'
		else '' end as continent
from "global_terr".globalterrorismdb


--- Number of terrorist attacks per year [1970-2017] NOTE: 1993 data not present

select COUNT(eventid) as num_of_terrorist_attacks,
	   extract(year from date) as dateyear
from "global_terr".globalterrorismdb
group by dateyear
order by dateyear;


--- Number of terrorist attacks per year per region [timeseries]

select COUNT(eventid) as num_of_terrorist_attacks,
	   extract(year from date) as dateyear,
	   region_txt
from "global_terr".globalterrorismdb
group by dateyear,  region_txt
order by dateyear;


--- Year to Year difference in number of terrorist attacks

with tt as (
select COUNT(eventid) as num_of_terrorist_attacks, 
	   extract(year from date) as dateyear
	   from "global_terr".globalterrorismdb
group by dateyear
order by dateyear
)
select num_of_terrorist_attacks, 
	   dateyear, 
	   num_of_terrorist_attacks - LAG(num_of_terrorist_attacks) over (order by dateyear) as previous_year_difference
from tt;


--- Decade to Decade difference in the number of terrorist attacks

WITH tt AS (
    SELECT COUNT(eventid) AS num_of_terrorist_attacks, 
           FLOOR(extract(year FROM date) / 10) * 10 AS decade
    FROM "global_terr".globalterrorismdb
    GROUP BY decade
    ORDER BY decade
)
SELECT decade, 
       num_of_terrorist_attacks,
       num_of_terrorist_attacks - LAG(num_of_terrorist_attacks) OVER (ORDER BY decade) AS previous_decade_difference
FROM tt;


---- Attacks per Month 

select extract(month from date) as d_month, count(eventid)
from "global_terr".globalterrorismdb
group by d_month
order by d_month asc, count desc

--- Cities and Countries with the highest number of terrorist attacks 

select count(eventid) num_of_attacks, city, country_txt
from "global_terr".globalterrorismdb
group by city, country_txt
having city is not null and city <> 'Unknown'
order by num_of_attacks desc, city 


--- Highest ransom demanded

with hrd as (
	select eventid, date, city, country_txt, MAX(ransomamt) as max_ransom_usd, coalesce(ransomnote, 'Unknown') as ransomnote
	from "global_terr".globalterrorismdb
	group by eventid, ransomnote, date, city, country_txt
)
select *
from hrd
where max_ransom_usd is not null and max_ransom_usd <> -99 and max_ransom_usd <> -9 and max_ransom_usd <> 0 and max_ransom_usd <> 1
order by max_ransom_usd desc


--- Highest ransom paid

with hrp as (
	select eventid, date, city, country_txt, MAX(ransompaid) max_ransompaid, coalesce(ransomnote, 'Unknown') as ransomnote
	from "global_terr".globalterrorismdb
	where ransomamt is not null
	group by eventid, ransomnote, date, city, country_txt
)
select * 
from hrp
where max_ransompaid is not null and max_ransompaid <> -99
order by max_ransompaid desc


--- Motives 

select motive, count(*) as cnt
from  "global_terr".globalterrorismdb
group by motive
having motive is not null
order by cnt desc

--- Attack Types

with cte as (
    SELECT COALESCE(attacktype1_txt, '') AS attacktype_one
    FROM "global_terr".globalterrorismdb
    UNION ALL
    SELECT COALESCE(attacktype2_txt, '') AS attacktype_one
    FROM "global_terr".globalterrorismdb
    UNION ALL
    SELECT COALESCE(attacktype3_txt, '') AS attacktype_one
    FROM "global_terr".globalterrorismdb
)
select attacktype_one, COUNT(*) as cnt
from cte
where attacktype_one != ''
group by attacktype_one
order by cnt desc;

--- Terrorist attacks with highest property damage

select eventid, date, city, country_txt, propvalue, propextent_txt, propcomment
from "global_terr".globalterrorismdb
where propvalue is not null and propvalue <> -99
group by eventid, date, city, country_txt, propvalue, propextent_txt, propcomment
order by propvalue desc


--- Deadliest Terrorist Attacks

select sum(nkill) as number_of_casualties, date, country_txt, city
from "global_terr".globalterrorismdb
where nkill is not null
group by country_txt, date, city
order by number_of_casualties desc 


--- Groups with the highest count of terrorist attacks

select gname, count(*) as cnt
from "global_terr".globalterrorismdb
-- where gname <> 'Unknown'
group by gname
order by cnt desc


--- Most Common Targets

with cte as (
    SELECT targtype1_txt as targ_comb
    FROM "global_terr".globalterrorismdb
    UNION ALL
    SELECT targtype2_txt as targ_comb
    FROM "global_terr".globalterrorismdb
    UNION ALL
    SELECT targtype3_txt as targ_comb
    FROM "global_terr".globalterrorismdb
)
select targ_comb, count(*) as cnt
from cte
where targ_comb is not null
group by targ_comb
order by cnt desc


--- Most popular weapon

with cte as (
select weaptype1_txt as weap_type
from "global_terr".globalterrorismdb
union all
select weaptype2_txt as weap_type
from "global_terr".globalterrorismdb
union all
select weaptype3_txt as weap_type
from "global_terr".globalterrorismdb
)
select weap_type, count(*) as cnt
from cte
where weap_type is not null 
group by weap_type
order by cnt desc


--- Number of killings per year

select extract(year from date) as yeardate, sum(nkill) as number_of_casualties
from "global_terr".globalterrorismdb
where nkill is not null
group by yeardate
order by yeardate desc


--- Number of causalties per region

select sum(nkill) as number_of_casualties, extract(year from date) as dateyear, region_txt -- nkillter, nwound, nwoundus, nwoundte
from "global_terr".globalterrorismdb
where nkill is not null
group by region_txt, dateyear
order by number_of_casualties desc


--- Number of causalties per country

select sum(nkill) as number_of_casualties, extract(year from date) as dateyear, country_txt -- nkillter, nwound, nwoundus, nwoundte
from "global_terr".globalterrorismdb
where nkill is not null
group by country_txt, dateyear
order by number_of_casualties desc


--- Cumulative sum of casualties

with cte as (
	select sum(nkill) as number_of_casualties, 
	extract (year from date) as yeardate
	from "global_terr".globalterrorismdb
	where nkill is not null
	group by yeardate
)
select yeardate, sum(number_of_casualties) over(order by yeardate) as cum_sum
from cte
order by yeardate


