SET SQL_SAFE_UPDATES = 0;
-- Remember to sort the above out later

select * from exportsofenergyproducts;
-- This result are all in years

select * from householdelectricityconsumption;

select year,month, SUM(kwh_per_acc)
from householdelectricityconsumption
group by year, month
order by month asc;

-- Upon further inspection, it seems that the data type for columns are text. Will be changing them to type integer
alter table householdelectricityconsumption 
modify column year int,
modify column month int,
modify column kwh_per_acc int;

-- Seems like the month column cannot be recoded as int because they have the "annual" summary count.
-- Should remove it from the DB

delete from householdelectricityconsumption
where month like "%annual%";

-- Rerun the above statement
alter table householdelectricityconsumption 
modify column year int,
modify column month int,
modify column kwh_per_acc int;

-- Seems like there are other issues within the kwh_per_acc columns such as it containing letters instead of numbers
select count(year) from householdelectricityconsumption
where kwh_per_acc ="";
-- There are zero blank values

-- Checking for string or text values now
select count(kwh_per_acc) 
from householdelectricityconsumption
where NOT REGEXP_LIKE(kwh_per_acc, '^-?[0-9.]+$');
-- There are 839 instances where there are non-numeric values

-- Okay try and count how many rows there are in total first
select count(kwh_per_acc)
from householdelectricityconsumption;
-- There is only a 1.15% of data which has the non numeric value. Could potentially drop it

## Deleting the data given that it is only a 1.15% loss in data
delete from householdelectricityconsumption
where NOT REGEXP_LIKE(kwh_per_acc, '^-?[0-9.]+$');

-- Maybe just change month first
alter table householdelectricityconsumption 
modify column year int,
modify column month int,
modify column kwh_per_acc int;



-- Re-exploring data
select year, month, sum(kwh_per_acc)
from householdelectricityconsumption
group by year, month
order by year asc, month asc;

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


## Sorting by QuarterlyValue starting with the highest value (desc)
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
order by QuarterlyValue desc;
## This shows us that on average Quarter 1 throughout the years are the quarters with the least energy consumption




























