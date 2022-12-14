select count(distinct(country))
from owid_energy_data, proper_countries
where owid_energy_data.iso_code = proper_countries.alpha3
or owid_energy_data.country = proper_countries.en;

## This returns me the list of 187 countries
## Inspecting list

select distinct(country), iso_code
from owid_energy_data, proper_countries
where owid_energy_data.iso_code = proper_countries.alpha3
or owid_energy_data.country = proper_countries.en;