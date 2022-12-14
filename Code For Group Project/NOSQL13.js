db.owid_energy_data.find()


// First step is to only show the documents where there are no blank values
db.owid_energy_data.aggregate([
    {$match:
        {"$and":
            [{biofuel_consumption: {$ne: ""}},
            {coal_consumption: {$ne: ""}},
            {fossil_fuel_consumption: {$ne: ""}},
            {gas_consumption: {$ne: ""}},
            {hydro_consumption: {$ne: ""}},
            {low_carbon_consumption: {$ne: ""}},
            {nuclear_consumption: {$ne: ""}},
            {oil_consumption: {$ne: ""}},
            {other_renewable_consumption:{$ne: ""}},
            {primary_energy_consumption:{$ne: ""}},
            {renewables_consumption: {$ne: ""}},
            {solar_consumption: {$ne: ""}},
            {wind_consumption:{$ne: ""}}
            ]}
    },
    {$project:
        {_id:0, 
        country:1, 
        year:1,
        biofuel_consumption:1,
        coal_consumption:1,
        fossil_fuel_consumption:1,
        gas_consumption:1,
        hydro_consumption:1,
        low_carbon_consumption:1,
        nuclear_consumption:1,
        oil_consumption:1,
        other_renewable_consumption:1,
        primary_energy_consumption:1,
        renewables_consumption:1,
        solar_consumption:1,
        wind_consumption:1
        }},
    {$sort: 
        {country:1}}
        ])
        
// Next step is to sum up all the consumption based on the country and the year
