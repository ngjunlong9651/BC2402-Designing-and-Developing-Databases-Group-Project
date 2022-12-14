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

