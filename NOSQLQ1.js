// Question 1
db.owid_energy_data.aggregate([
    {$group : {_id: "$iso_code"}},
    {$count : "Amount of countries in Owid_Energy_data"}]) 
// This shows that there is a total of 223 countries in the second collection

// Need to only show those with an ISO code = 3
db.owid_energy_data.aggregate([
    {$project: 
        {iso_check: 
            {$substr: ["$iso_code", 0, 4]}}}, // Project iso_codes and extract from string iso_code from 1st char to 3rd char
    {$match:{
        "$and": 
            [{"iso_check": {$ne: ""}},
            {"iso_check": {$ne: "OWID"}}]
    }}, // Removing iso_codes that are empty and displaying country code that are OWID
    {$group: 
        {_id: "$iso_check"}}
    ]).count()
// This shows us that there are 217 countries with isocode = 3

// Will do a check to see countries and ISO_code that 
// is not inside the query above

// Check for countries that do not have iso_code =3
db.owid_energy_data.aggregate([
    {$project:
        {_id:0, country:1, iso_code:1}},
    {$match:
        {"iso_code": {$eq:""}}},
    {$group:
        {_id:{groupByCountry: "$country"}}}
    ])

