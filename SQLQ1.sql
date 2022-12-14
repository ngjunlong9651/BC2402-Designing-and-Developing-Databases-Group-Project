## Will be counting the countries by ISO_code
select count(distinct(iso_code))
from owid_energy_data
where length(iso_code) = 3;

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
-- Therefore total = 218 countries after adding kosovo;

