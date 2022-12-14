## Compiled SQL code base for BC2402 Question 1 to 11

## Data Cleaning:
## Creating a common cleaning method
SET SQL_SAFE_UPDATES = 0;
## Will be closing this at the end of the ENTIRE codebase

## Modifying table contents

## Altering Table For owid_energy_data
update owid_energy_data
set gdp = NULL
where gdp = "";
#To not affect calculations of the average GDP as well as GDP per capita below

update owid_energy_data
set oil_consumption = NULL
where oil_consumption = ""; #Updating of blank values to NULL
#To not affect calculations of average_oil_consumption below


## Altering Table For householdelectricityconsumption

alter table householdelectricityconsumption 
modify column year int;
## modify column month int; 
## Seems like the month column cannot be recoded as int because they have the "annual" summary count.
## Should remove it from the Database
## Checking for string or text values now
select count(kwh_per_acc) 
from householdelectricityconsumption
where NOT REGEXP_LIKE(kwh_per_acc, '^-?[0-9.]+$');
## There are 906 instances where there are non-numeric values
-- Okay try and count how many rows there are in total first
select count(kwh_per_acc)
from householdelectricityconsumption;
-- There is only a 1.15% of data which has the non numeric value. Could drop it
## Deleting the data given that it is only a 1.15% loss in data
delete from householdelectricityconsumption
where NOT REGEXP_LIKE(kwh_per_acc, '^-?[0-9.]+$');
## Altering Table for importsofenergyproduct
#Checking for NULL values
SELECT *
FROM importsofenergyproducts
WHERE value_ktoe IS NULL OR value_ktoe = "";
# No empty values found

## Altering Table for exportsofenergyproduct
SELECT *
FROM exportsofenergyproducts
WHERE value_ktoe IS NULL OR value_ktoe = "";
# No empty values found









## Question 1:
## Will be counting the countries by ISO_code
select count(distinct(iso_code))
from owid_energy_data
where length(iso_code) = 3;
## Checked and returned 217 countries

-- Checking individual countries
select distinct(iso_code)
from owid_energy_data
where length(iso_code) = 3;

## Checking for countries that are not in the list:
select distinct(iso_code), country
from owid_energy_data
where length(iso_code) !=3;

select distinct(country)
from owid_energy_data
where iso_code like "";

## Greenland is a country, just that the country itself has two records, one with ISO_code and the other without ISO_code
## Kosovo was recognized as a country by Singapore on 1 Dec 2016

## If we count by ISO_Code, we will have a total of 217 countries with ISO_code =3 and 1 country that were excluded but recognized by Singapore
## Therefore total = 218 countries after adding kosovo;


## Question 2
select min(year) from owid_energy_data
; -- min:1900
select max(year) from owid_energy_data
; -- max:2021
-- total number of years from 1900 to 2021 is 122

select country, count(country) as records from owid_energy_data
where country in 
(select distinct(country) from owid_energy_data
where length(iso_code) = 3
or country like "Kosovo")		#additional countries identified from q1 that were omitted from using iso_code = 3
group by country
having count(country) = (select max(year) - min(year) + 1 from owid_energy_data)		#difference between year 2021 and year 1900 is 2021-1900+1 = 122
;



## Question 3
select country, year, fossil_share_energy
from owid_energy_data
where country = "Singapore" and cast(fossil_share_energy as float) < 100
order by year
limit 1; 

select iso_code, country, year, fossil_share_energy, oil_share_energy, coal_share_energy, gas_share_energy, low_carbon_share_energy, nuclear_share_energy, renewables_share_energy, other_renewables_share_energy, biofuel_share_energy, hydro_share_energy, solar_share_energy, wind_share_energy
from owid_energy_data
where country = "Singapore" and year in (1985,1986);

## Question 4
SELECT country, (SUM(CAST(gdp AS FLOAT)) / COUNT(DISTINCT(year))) AS avgGdp
FROM owid_energy_data
WHERE year BETWEEN 2000 AND 2021
AND country IN ("Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam")
GROUP BY country
ORDER BY avgGdp DESC;

## Question 5
UPDATE owid_energy_data
SET oil_consumption = NULL
WHERE oil_consumption = ""; #Updating of blank values to NULL
#To not affect calculations of average_oil_consumption below

#First Part: 3-year Moving Averages
SELECT country, year, AVG(oil_consumption+0.0) #oil_consumption is text column
OVER(
PARTITION BY country
ORDER BY year
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS average_oil_consumption
FROM owid_energy_data
WHERE country IN ("Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore",
"Thailand", "Vietnam") AND (year BETWEEN 2002 AND 2021) #Do I start from 2002 or 2000? --> 2002 if using Partition
ORDER BY year, country;
#Brunei, Cambodia, Laos and Myanmar is 0 throughout --> Discuss how to clean during meeting --> Make NULL
#Second Part: Identifying instances of negative changes and corresponding 3-year moving averages in GDP
#Can compute country by country instead - Prof's Comment
SELECT *
FROM
(SELECT country, year, average_oil_consumption - LAG(average_oil_consumption,1)
OVER(
PARTITION BY country
ORDER BY year) AS change_in_moving_average, AVG(gdp+0.0)
OVER(
PARTITION BY country
ORDER BY year
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS average_gdp
FROM
(SELECT country, year, AVG(oil_consumption+0.0)
OVER(
PARTITION BY country
ORDER BY year
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS average_oil_consumption, gdp
FROM owid_energy_data
WHERE country IN ("Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore",
"Thailand", "Vietnam") AND (year BETWEEN 2002 AND 2021) #Do I start from 2002 or 2000? --> 2002 if using Partition
ORDER BY year, country) AS table1) AS table2
WHERE change_in_moving_average < 0;

## Question 6
#Overall Average for importsofenergyproducts
SELECT energy_products, sub_products, AVG(value_ktoe+0.0) AS average_value
FROM importsofenergyproducts
GROUP BY sub_products, energy_products;

#Overall Average for exportsofenergyproducts
SELECT energy_products, sub_products, AVG(value_ktoe+0.0) AS average_value
FROM exportsofenergyproducts
GROUP BY sub_products, energy_products;

## Question 7

SELECT x.year, x.energy_products, x.sub_products, CAST(x.value_ktoe AS FLOAT) - CAST(m.value_ktoe AS FLOAT) AS yearlyDiff
FROM exportsofenergyproducts x
INNER JOIN importsofenergyproducts m
ON x.year = m.year
WHERE x.energy_products = m.energy_products
AND x.sub_products = m.sub_products;


SELECT x.year
FROM exportsofenergyproducts x
INNER JOIN importsofenergyproducts m
ON x.year = m.year
WHERE x.energy_products = m.energy_products
AND x.sub_products = m.sub_products
AND CAST(x.value_ktoe AS FLOAT) > CAST(m.value_ktoe AS FLOAT)
GROUP BY x.year
HAVING COUNT(x.year) > 4;

## Question 8
select region, year, avg(cast(kwh_per_acc as float)) as yearly_avg_kwh
from householdelectricityconsumption
where dwelling_type = "Overall" and month = "Annual" and Region not in ("Overall") and Description not like '%Region%' #use overall to avoid overlapping
group by Region, year
order by Region, year;

## Question 9
select region, count(kwh_per_acc_MA) as count 
from 
(select year, region,
case when convert(year, signed int) = 2005 then 0 -- since 2005 is the first year, no 2 year MA diff
else avg_kwh_per_acc - lag(avg_kwh_per_acc) over(order by region, convert(year, signed int))
end as kwh_per_acc_MA
from 
	(select year, region, avg(convert(kwh_per_acc, double)) avg_kwh_per_acc
	from householdelectricityconsumption
	where dwelling_type = "Overall"
	and month = "Annual" 
	and region != "Overall"
	and description not like "%Region%"
	group by year, region
	order by year, region) as a) as b
where kwh_per_acc_MA < 0
group by region
order by -count
limit 3;

## Question 10

select * from householdelectricityconsumption;

delete from householdelectricityconsumption
where month like "%annual%";

## Altering Table 
alter table householdelectricityconsumption 
modify column month int; 

-- Next step is to group them up quarterly
(select year, "Quarter 1" as month, round(sum(kwh_per_acc),3) as totalquartervalue
    from householdelectricityconsumption
    where month IN ("1", "2", "3")
    group by year)
union
 (select year, "Quarter 2" as month, round(sum(kwh_per_acc),3) as totalquartervalue
    from householdelectricityconsumption
    where month IN ("4", "5", "6")
    group by year)
union
(select year, "Quarter 3" as month, round(sum(kwh_per_acc),3) as totalquartervalue
    from householdelectricityconsumption
    where month IN ("7", "8", "9")
    group by year)
union
(select year, "Quarter 4" as month, round(sum(kwh_per_acc),3) as totalquartervalue
    from householdelectricityconsumption
    where month IN ("10", "11", "12")
    group by year)
ORDER BY year, month;

## Ordering the data by year ascending

## Grouping by QuarterlyValue
(select year, region, "Quarter 1" as quarter, avg(kwh_per_acc) as avg_kwh
	from householdelectricityconsumption
    where month in("1","2","3")
    group by year,region)
union
(select year, region, "Quarter 2" as quarter, avg(kwh_per_acc) as avg_kwh
	from householdelectricityconsumption
    where month in("4","5","6")
    group by year,region)
union
(select year, region, "Quarter 3" as quarter, avg(kwh_per_acc) as avg_kwh
	from householdelectricityconsumption
    where month in("7","8","9")
    group by year, region)
union
(select year, region, "Quarter 4" as quarter, avg(kwh_per_acc) as avg_kwh
	from householdelectricityconsumption
    where month in("10","11","12")
    group by year,region)
order by year asc, region, quarter;



## Question 11
SELECT year, sub_housing_type,
ROUND(AVG(CASE WHEN month IN ("1","2","3") THEN avg_mthly_hh_tg_consp_kwh+0.0 END),4) AS Q1,
ROUND(AVG(CASE WHEN month IN ("4","5","6") THEN avg_mthly_hh_tg_consp_kwh+0.0 END),4) AS Q2,
ROUND(AVG(CASE WHEN month IN ("7","8","9") THEN avg_mthly_hh_tg_consp_kwh+0.0 END),4) AS Q3,
ROUND(AVG(CASE WHEN month IN ("10","11","12") THEN avg_mthly_hh_tg_consp_kwh+0.0 END),4) AS Q4
FROM householdtowngasconsumption
WHERE sub_housing_type != "Overall" AND month NOT LIKE "%Region%"
GROUP BY sub_housing_type, year
ORDER BY sub_housing_type, year;

#Checking for Quarterly Effects
SELECT
AVG(CASE WHEN month IN ("1","2","3") THEN avg_mthly_hh_tg_consp_kwh+0.0 END) AS Q1,
AVG(CASE WHEN month IN ("4","5","6") THEN avg_mthly_hh_tg_consp_kwh+0.0 END) AS Q2,
AVG(CASE WHEN month IN ("7","8","9") THEN avg_mthly_hh_tg_consp_kwh+0.0 END) AS Q3,
AVG(CASE WHEN month IN ("10","11","12") THEN avg_mthly_hh_tg_consp_kwh+0.0 END) AS Q4
FROM householdtowngasconsumption
WHERE sub_housing_type != "Overall";
#Not much difference generally, but can see that Q1, Q2, Q3 is slightly higher, while Q4 is slightly lower

## Question 12
#Exploring Current Data Sets
SELECT *
FROM owid_energy_data;
#Noteworthy Columns: gdp, population, energy_per_gdp, energy_per_capita + Different types of energy consumption

#Finding Comparable References to Singapore
SELECT country, year, gdp, population
FROM (
	SELECT country, year, gdp, population
	FROM owid_energy_data
	WHERE (population BETWEEN ((
		SELECT population
		FROM owid_energy_data
		WHERE country = "Singapore" AND (year = 2018)) - 1000000.0) AND ((
		SELECT population
		FROM owid_energy_data
		WHERE country = "Singapore" AND (year = 2018)) + 1000000.0)) AND
	gdp BETWEEN ((
		SELECT gdp
		FROM owid_energy_data
		WHERE country = "Singapore" AND (year = 2018)) - 300000000000.0) AND ((
		SELECT gdp
		FROM owid_energy_data
		WHERE country = "Singapore" AND (year = 2018)) + 300000000000.0)) AS table2
WHERE year BETWEEN 2000 AND 2018
ORDER by country, year;
#For multiple years: Subquery returns more than 1 row, is there any way to do this to obtain countries?
#Cannot do that, need to set 1 year's value as baseline for comparison --> 2018
#Based on this, nearest reference for gdp + population is Denmark and Finland
#Reasoning: Only 2 countries to have all 19 years in range

#Extracting GDP per capita for Singapore, Denmark and Finland
SELECT country, year, (gdp+0.0/population+0.0) AS gdp_per_capita
FROM owid_energy_data
WHERE country IN ("Singapore", "Denmark", "Finland") AND year BETWEEN 2000 AND 2018
ORDER BY country, year;
#Analysis 1: Singapore started lower than Denmark and Finland in 2000, but overtook Denmark in 2007, Finland in 2003
#Analysis 2: All 3 show increasing trend, but SG has the highest magnitude of change (Nearly tripled)

#Extracting energy per capita for Singapore, Denmark and Finland
SELECT country, year, energy_per_capita
FROM owid_energy_data
WHERE country IN ("Singapore", "Denmark", "Finland") AND year BETWEEN 2000 AND 2018
ORDER BY country, year;
#Analysis 1: Singapore increasing trend, while Denmark and Finland decreasing trend
#Analysis 2: Denmark has higher magnitude of decrease

#Comparing energy per gdp for Singapore, Denmark and Finland
SELECT country, year, energy_per_gdp
FROM owid_energy_data
WHERE country IN ("Singapore", "Denmark", "Finland") AND year BETWEEN 2000 AND 2018
ORDER BY country, year;
#Analysis 1: All 3 experience decreasing trend
#Analysis 2: Denmark highest magnitude of decrease, Singapore lowest magnitude of decrease

#Comparing just total energy consumption for Singapore, Denmark and Finland
SELECT country, year, ((biofuel_consumption+0.0)+(coal_consumption+0.0)+(fossil_fuel_consumption+0.0)+(gas_consumption+0.0)
+(hydro_consumption+0.0)+(low_carbon_consumption+0.0)+(nuclear_consumption+0.0)+(oil_consumption+0.0)
+(other_renewable_consumption+0.0)+(primary_energy_consumption+0.0)+(renewables_consumption+0.0)+(solar_consumption+0.0)
+(wind_consumption+0.0))
AS total_energy_consumption
FROM owid_energy_data
WHERE country IN ("Singapore", "Denmark", "Finland") AND year BETWEEN 2000 AND 2018
ORDER BY country, year;
#Analysis: Singapore increased drastically (More than double), Denmark and Finland decreased slightly



## Question 13

-- Big level overview
select *
from owid_energy_data
order by country;


-- Seems like the data is quite unreliable in the sense that some countries flat out don't provide any data for all years

-- Forcing it to show only columns where not null
select distinct(country), year, biofuel_consumption, coal_consumption, fossil_fuel_consumption, 
gas_consumption, hydro_consumption, low_carbon_consumption, 
nuclear_consumption, oil_consumption, other_renewable_consumption, 
primary_energy_consumption, renewables_consumption, solar_consumption, wind_consumption 
from owid_energy_data
where biofuel_consumption not like ""
and coal_consumption not like ""
and fossil_fuel_consumption not like ""
and gas_consumption not like ""
and hydro_consumption not like ""
and low_carbon_consumption not like ""
and nuclear_consumption not like ""
and oil_consumption not like ""
and other_renewable_consumption not like ""
and primary_energy_consumption not like ""
and renewables_consumption not like ""
and solar_consumption not like ""
and wind_consumption not like ""
and year >= 2000
order by country;

-- Important to note that the blank values are actually an empty string. 
-- Now that we can view all the consumption throughout, the next step is to create a column with all the values summed together 
-- to see the yearly values required for each "country".
-- How much each country requires a year
select distinct(country), year, round(biofuel_consumption+coal_consumption+fossil_fuel_consumption+
gas_consumption+hydro_consumption+low_carbon_consumption+
nuclear_consumption+oil_consumption+other_renewable_consumption+
primary_energy_consumption+renewables_consumption+solar_consumption+wind_consumption) as total_consumption
from owid_energy_data
where biofuel_consumption not like ""
and coal_consumption not like ""
and fossil_fuel_consumption not like ""
and gas_consumption not like ""
and hydro_consumption not like ""
and low_carbon_consumption not like ""
and nuclear_consumption not like ""
and oil_consumption not like ""
and other_renewable_consumption not like ""
and primary_energy_consumption not like ""
and renewables_consumption not like ""
and solar_consumption not like ""
and wind_consumption not like ""
and year >= 2000
order by country;


-- Similarly looking at the numbers of electricity generated as a total
select distinct(country), year, biofuel_electricity+coal_electricity+electricity_generation+
fossil_electricity+gas_electricity+hydro_electricity+low_carbon_electricity+nuclear_electricity+
oil_electricity+other_renewable_electricity+other_renewable_exc_biofuel_electricity+
renewables_electricity+solar_electricity+ wind_electricity as total_generated
from owid_energy_data
where biofuel_electricity not like ""
and coal_electricity not like ""
and fossil_electricity not like ""
and electricity_generation not like ""
and gas_electricity not like ""
and hydro_electricity not like ""
and low_carbon_electricity not like ""
and nuclear_electricity not like ""
and oil_electricity not like ""
and other_renewable_electricity not like ""
and other_renewable_exc_biofuel_electricity not like ""
and renewables_electricity not like ""
and solar_electricity not like ""
and wind_electricity not like ""
and year >= 2000
order by country;




-- Looking at other high level metrics of energy share
select country, year, gdp, population, biofuel_share_energy, coal_share_energy, fossil_share_energy
, gas_share_energy, hydro_share_energy, low_carbon_share_energy, nuclear_share_energy,oil_share_energy,
other_renewables_share_energy, renewables_share_energy, solar_share_energy, wind_share_energy
from owid_energy_data
where year not like ""
and gdp not like ""
and population not like ""
and biofuel_share_energy not like ""
and coal_share_energy not like ""
and fossil_share_energy not like ""
and gas_share_energy not like ""
and hydro_share_energy not like ""
and low_carbon_share_energy not like ""
and nuclear_share_energy not like ""
and oil_share_energy not like ""
and other_renewables_share_energy not like ""
and renewables_share_energy not like ""
and solar_share_energy not like ""
and wind_share_energy not like ""
and year >= 2000
order by country;


## Looking at ASEAN nations
select country, year, gdp, population, biofuel_share_energy, coal_share_energy, fossil_share_energy
, gas_share_energy, hydro_share_energy, low_carbon_share_energy, nuclear_share_energy,oil_share_energy,
other_renewables_share_energy, renewables_share_energy, solar_share_energy, wind_share_energy
from owid_energy_data
where country in ("Singapore", "Malaysia", "Brunei", "Cambodia","Indonesia","Laos","Myanmar","Philippines","Thailand","Vietnam")
and gdp not like "";


-- Global Economy Trend
-- Looking at big countries that affect global shifts in renewable energy perspectives
-- AKA( USA, China, Russia, India )
select country, fossil_electricity, renewables_electricity, gdp, year, 
(fossil_electricity+0.0 - lag(fossil_electricity+0.0,1) over(partition by country order by year)) as fossil_change,
(renewables_electricity+0.0 - lag(renewables_electricity+0.0,1) over(partition by country order by year)) as renewable_change,
(gdp+0.0 - lag(gdp+0.0,1) over(partition by country order by year)) as gdp_change
from owid_energy_data
where country in ("United States", "China", "Japan", "Germany")
and renewables_electricity not like ""
and fossil_electricity not like ""	
and gdp not like ""
and year >= 2000
order by country, year;


-- ASEAN Economic Trend
select country, fossil_electricity, renewables_electricity, gdp, year, 
(fossil_electricity+0.0 - lag(fossil_electricity+0.0,1) over(partition by country order by year)) as fossil_change,
(renewables_electricity+0.0 - lag(renewables_electricity+0.0,1) over(partition by country order by year)) as renewable_change,
(gdp+0.0 - lag(gdp+0.0,1) over(partition by country order by year)) as gdp_change
from owid_energy_data
where country in ("Singapore", "Malaysia", "Brunei", "Cambodia","Indonesia","Laos","Myanmar","Philippines","Thailand","Vietnam")
and renewables_electricity not like ""
and fossil_electricity not like ""
and gdp not like ""
and year >= 2000
order by fossil_change asc, country, year;




## Question 14
# table showing the transition away from fossil fuel to renewable energy more specifically solar energy
select country, year, fossil_share_energy, solar_share_energy, nuclear_electricity
from owid_energy_data
where country = "Singapore" and year in (1985,1986, 2020);		

(select year, "Quarter 1" as month, avg(kwh_per_acc) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("1","2","3")
    group by year,region)
union
(select year, "Quarter 2" as month, avg(kwh_per_acc) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("4","5","6")
    group by year,region)
union
(select year, "Quarter 3" as month, avg(kwh_per_acc) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("7","8","9")
    group by year, region)
union
(select year, "Quarter 4" as month, avg(kwh_per_acc) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("10","11","12")
    group by year,region)
order by year asc;

#Grouping by QuarterlyValue
# Table to show quarterly consumption 
(select year, "Quarter 1" as month, round(avg(kwh_per_acc),3) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("1","2","3")
    group by year,region)
union
(select year, "Quarter 2" as month, round(avg(kwh_per_acc),3) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("4","5","6")
    group by year,region)
union
(select year, "Quarter 3" as month, round(avg(kwh_per_acc),3) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("7","8","9")
    group by year, region)
union
(select year, "Quarter 4" as month, round(avg(kwh_per_acc),3) as QuarterlyValue, region
	from householdelectricityconsumption
    where month in("10","11","12")
    group by year,region)
order by QuarterlyValue;



## Question 15
-- 1. globalTempChange
CREATE TABLE globalTempChange
AS SELECT 'World' AS area, year, AVG(avgValueChange) AS Average_Value
FROM
    (SELECT area, year, AVG(value) AS avgValueChange
    FROM sustainability2022.temperaturechangebycountry
    GROUP BY area , year
    ORDER BY area , year) AS World_average
GROUP BY year;

-- 2. globalTempChange_GHGemissions
CREATE TABLE globalTempChange_GHGemissions
AS SELECT area, globalTempChange.year, Average_Value AS tempChange, greenhouse_gas_emissions, (greenhouse_gas_emissions/population) AS emissions_per_capita
FROM globalTempChange, owid_energy_data
WHERE country = "World"
AND globalTempChange.year = owid_energy_data.year
AND greenhouse_gas_emissions IS NOT NULL;

SET SQL_SAFE_UPDATES = 0;

UPDATE owid_energy_data
SET population = NULL
WHERE population = "";

-- 3. gdp_emissions_cap
CREATE TABLE gdp_emissions_cap
AS SELECT iso_code, country, year, gdp, population, (gdp/population) AS gdp_per_capita,greenhouse_gas_emissions, (greenhouse_gas_emissions/population) AS emissions_per_capita
FROM owid_energy_data
WHERE greenhouse_gas_emissions IS NOT NULL
AND (LENGTH(iso_code) = 3 OR country = "World")
OR country LIKE "Greenland" 
OR country LIKE "Kosovo";

-- 4. energy
CREATE TABLE energy
AS SELECT iso_code, country, year, gdp, population, (gdp/population) AS gdp_per_capita,greenhouse_gas_emissions, (greenhouse_gas_emissions/population) AS emissions_per_capita, energy_cons_change_pct, energy_per_capita, fossil_fuel_consumption, fossil_energy_per_capita, renewables_consumption, renewables_energy_per_capita, nuclear_consumption, nuclear_energy_per_capita
FROM owid_energy_data
WHERE greenhouse_gas_emissions IS NOT NULL
AND (LENGTH(iso_code) = 3 OR country = "World")
OR country LIKE "Greenland" 
OR country LIKE "Kosovo";


## Closing safe update
SET SQL_SAFE_UPDATES = 1;