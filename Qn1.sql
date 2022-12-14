SELECT * FROM
owid_energy_data;


-- There are 241 unique "countries" in the dataset
SELECT COUNT(DISTINCT(country))
FROM owid_energy_data;

-- As a student, i have decided to rely on the UN for the definition of a nation.
-- As such, I shall import the data table for cross reference.

select * from owid_energy_data;
select * from UN_country;

-- This shows us that it looks like we can join the two different tables through their names
select * from
owid_energy_data, UN_country
where owid_energy_data.country = UN_country.name;


-- This gives us the count of countries that are matched bettween owid_energy_data and UN_country
select count(distinct(country))
from owid_energy_data, UN_country
where owid_energy_data.country = UN_country.name;


select distinct(country)
from owid_energy_data, UN_country
where owid_energy_data.country
not in
(select distinct(country)
from owid_energy_data, UN_country
where owid_energy_data.country = UN_country.name);






