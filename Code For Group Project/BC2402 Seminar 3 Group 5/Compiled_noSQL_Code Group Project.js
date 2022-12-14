//BC2402 Designing & Developing Databases
//Seminar Group 3
//Group 5
//Group Project Queries for Questions 1 to 15

use group_project //Rename accordingly

/*
Question 1: How many countries are captured in [owid_energy_data]?
Note: Be careful! The devil is in the details.
*/
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

// Check for countries that do not have iso_code = 3
db.owid_energy_data.aggregate([
    {$project:
        {_id:0, country:1, iso_code:1}},
    {$match:
        {"iso_code": {$eq:""}}},
    {$group:
        {_id:{groupByCountry: "$country"}}},
    {$sort:{"_id.groupByCountry":1}}
    ])

// Add  in country Kosovo
// Total country : 217 + 1:Kosovo

/*
Question 2: Find the earliest and latest year in [owid_energy_data]. What are the countries having a record in <owid_energy_data> every year
throughout the entire period (from the earliest year to the latest year)?
Note: The output must provide evidence that the countries have the same number of records.
*/
db.owid_energy_data.aggregate([         // find minimum year and maximum year
    {$group:{_id: {}, minyear: {$min: "$year"}, maxyear: {$max: "$year"}}}
    ]) 		// to display the min and max year
    
db.owid_energy_data.aggregate([         // find countries with 122 years of record
    {$project: {iso_code: {$substr: ["$iso_code", 0, 3]}, year: {$toInt: "$year"}}},
    {$group: {_id: "$iso_code", yearscount: {$sum: 1}}},
    {$match:{"$or": [{
        "$and":[{"country": "Kosovo"}]}, // to consider "Kosovo" which was left out using iso_code as a filter
        {"$and":[{_id: {$ne: ""}}, {_id: {$ne: "OWID"}}, {yearscount: 122}]}]}},
    ]) 
    
/*
Question 3: Specific to Singapore, in which year does <fossil_share_energy> stop being the full source of energy (i.e., <100)? Accordingly,
show the new sources of energy.
*/
db.owid_energy_data.aggregate([
    {$project: {_id: 0, "country": 1, "year": 1, "fossil_share_energy": {$convert: {input: "$fossil_share_energy", to: "double", onError: null, onNull: null}}}},
    {$match: {"country": "Singapore"}},
    {$match: {"fossil_share_energy": {$lt: 100}}},
    {$sort: {"year":1}},
    {$limit: 1},
    ]) 

db.owid_energy_data.aggregate([
    {$match: {"country": "Singapore"}},
    {$match: {"year": "1986"}},
    {$project: {iso_code: 1, country: 1, year: 1, fossil_share_energy: 1, oil_share_energy: 1, coal_share_energy: 1, gas_share_energy: 1, low_carbon_share_energy: 1, nuclear_share_energy: 1, renewables_share_energy: 1, other_renewables_share_energy: 1, biofuel_share_energy: 1, hydro_share_energy: 1, solar_share_energy: 1, wind_share_energy: 1}}
    ])

/*
Question 4: Compute the average <GDP> of each ASEAN country from 2000 to 2021 (inclusive of both years). Display the list of countries based
on the descending average GDP value.
*/
db.owid_energy_data.aggregate([ 
 { 
     $project: { 
         'gdp': {$convert: { input: "$gdp", to: 1, onError: null, onNull: null}}, 
         'country': 1, 
         'year': {$toInt: '$year'} 
     } 
 }, 
 { 
     $match: { 
         'country': { 
             $in: [ 
                 "Brunei", 
                 "Cambodia", 
                 "Indonesia", 
                 "Laos", 
                 "Malaysia", 
                 "Myanmar", 
                 "Philippines", 
                 "Singapore", 
                 "Thailand", 
                 "Vietnam" 
             ] 
         }, 
         'gdp': {$ne: null}, 
         'year': {$gte: 2000, $lte: 2021}, 
     } 
 }, 
 { 
     $group: { 
         _id: '$country', 
         avgGdp: {$avg: '$gdp'} 
     } 
 }, 
 { 
     $sort: { 
         avgGdp: -1 
     } 
 } 
])

/*
Question 5: (Without creating additional tables/collections) For each ASEAN country, from 2000 to 2021 (inclusive of both years), compute the
3-year moving average of <oil_consumption> (e.g., 1st: average oil consumption from 2000 to 2002, 2nd: average oil consumption from 2001 to
2003, etc.). Based on the 3-year moving averages, identify instances of negative changes (e.g., An instance of negative change is detected
when 1st 3-yo average = 74.232, 2nd 3-yo average = 70.353). Based on the pair of 3-year averages, compute the corresponding 3-year moving
averages in GDP.
*/
db.owid_energy_data.aggregate( [
    //Data conversions and keeping relevant fields + Cleaning of NULL values
    {$project: {country: 1, year: {$toInt: "$year"}, 
                oil_consumption: {$convert:{input: "$oil_consumption", to: 1, onError: null, onNull: null}}, 
                gdp:{$convert:{input: "$gdp", to: 1, onError: null, onNull: null}}}}, //1 for double
   //ASEAN countries from 2000 to 2021 --> Start from 2002 because of window frame set below
    {$match: {country: {$in: ["Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore",
    "Thailand", "Vietnam"]}, year: {$gte: 2002, $lte: 2021}}},
    //Create new fields of oil & gdp 3-year moving average
    {$setWindowFields: {partitionBy: "$country" , 
                        sortBy: {year:1},            
                        output: {oil_moving_avg: {$avg: "$oil_consumption", window: {documents: [-2, 0]}} ,
                                   gdp_moving_avg: {$avg: "$gdp", window: {documents: [-2, 0]}}}}},
    //Create new fields of previous year's 3-year moving averages for oil and gdp 
    {$setWindowFields: {partitionBy: "$country",
                        sortBy: {year:1},
                        output: {pre_oil_moving_avg: {$avg: "$oil_moving_avg", window: {documents: [-1, -1]}} ,
                                   pre_gdp_moving_avg: {$avg: "$gdp_moving_avg", window: {documents: [-1, -1]}}}}},
   //Create new field of moving_avg_diff for oil & gdp
   {$addFields: {moving_avg_oil_diff: {$subtract: ["$oil_moving_avg", "$pre_oil_moving_avg"]}, 
                 moving_avg_gdp_diff: {$subtract: ["$gdp_moving_avg", "$pre_gdp_moving_avg"]}}},
   //Only show years where moving oil average difference < 0
   {$match: {moving_avg_oil_diff: {$lt: 0}}},
   {$sort: {"country":1,"year":1}
] )

/*
Question 6: For each <energy_products> and <sub_products>, display the overall average of <value_ktoe> from [importsofenergyproducts] and
[exportsofenergyproducts].
*/
db.importsofenergyproducts.aggregate([
    {$project: {sub_products: 1, value_ktoe: {$convert: {input: "$value_ktoe", to: 1, onError: null, onNull: null}}}},
    {$group: {_id: "$sub_products", avg_value_ktoe: {$avg: "$value_ktoe"}}}
    ])

db.exportsofenergyproducts.aggregate([
    {$project: {sub_products: 1, value_ktoe: {$convert: {input: "$value_ktoe", to: 1, onError: null, onNull: null}}}},
    {$group: {_id: "$sub_products", avg_value_ktoe: {$avg: "$value_ktoe"}}}
    ])

/*
Question 7: For each combination of <energy_products> and <sub_products>, find the yearly difference in <value_ktoe> from
[importsofenergyproducts] and [exportsofenergyproducts]. Identify those years where more than 4 instances of export value > import value can
be detected.
*/
db.importsofenergyproducts.aggregate([ 
 { 
     $group: { 
         "_id": { 
             year: '$year', 
             energy_products: '$energy_products', 
             sub_products: '$sub_products', 
         }, 
         count: {$count: {}} 
     } 
 }, 
 { 
     $sort: {'count': -1} 
 } 
]) 
 
//Yearly Difference 
db.importsofenergyproducts.aggregate([ 
 { 
     $lookup: 
         { 
             from: "exportsofenergyproducts", 
             let: { 
                 energy_products: "$energy_products", 
                 sub_products: "$sub_products", 
                 year: "$year", 
             }, 
             pipeline: [ 
                 { 
                     $match: { 
                         $expr: { 
                             $and: [ 
                                 {$eq: ["$energy_products", "$$energy_products"]}, 
                                 {$eq: ["$sub_products", "$$sub_products"]}, 
                                 {$eq: ["$year", "$$year"]}, 
                             ] 
                         } 
                     } 
                 }, 
                 { 
                     $project: { 
                         energy_products: 1, sub_products: 1, year: 1, value_ktoe: {$toDouble: "$value_ktoe"}, _id: 0 
                     } 
                 } 
             ], 
             as: "exportsdata" 
         } 
 }, 
 { 
     $project: { 
         energy_products: 1, 
         sub_products: 1, 
         import_ktoe: {$toDouble: "$value_ktoe"}, 
         export_ktoe: {$first: "$exportsdata.value_ktoe"}, 
         year: 1 
     } 
 }, 
 { 
     $addFields: { 
         export_sub_import: {$subtract: ['$export_ktoe', '$import_ktoe']}, 
     } 
 }, 
])

/*
Question 8: In [householdelectricityconsumption], for each <region>, excluding “overall”, generate the yearly average <kwh_per_acc>.
*/
db.householdelectricityconsumption.aggregate([
    {$match: {"dwelling_type": "Overall"}},
    {$match: {"month": "Annual"}},
    {$match: {"Region": {$not : /Overall/}}},
    {$match: {"Description": {$not: /.*Region.*/}}},
    {$match: {"kwh_per_acc": {$ne: "s\r"}}},
    {$project: {year: {$toInt: "$year"}, Region: 1, kwh_per_acc: {$convert: {input: {$substr: ["$kwh_per_acc", 0, {$subtract: [{$strLenCP: "$kwh_per_acc"}, 1]}]}, to: 1, onError: null, onNull: null}}}},
    {$group: {_id: {year: "$year", region: "$Region"}, avg_kwh: {$avg: "$kwh_per_acc"}}},
    {$sort: {"_id.region": 1, "_id.year": 1}}
    ])

/*
Question 9: Who are the energy-saving stars? Compute the yearly average of <kwh_per_acc> in each region, excluding “overall”. Generate the
moving 2-year average difference (i.e. year 1 average kwh_per_acc for the central region = 1223, year 2 = 1000, the moving 2-year average
difference = -223). Display the top 3 regions with the most instances of negative 2-year averages.
*/
db.householdelectricityconsumption.aggregate([
    {$match: {"dwelling_type": "Overall"}},
    {$match: {"month": "Annual"}},
    {$match: {"Region": {$not : /Overall/}}},
    {$match: {"Description": {$not: /.*Region.*/}}},
    {$project: {year: {$toInt: "$year"}, Region: 1, kwh_per_acc: {$convert: {input: {$substr: ["$kwh_per_acc", 0, {$subtract: [{$strLenCP: "$kwh_per_acc"}, 1]}]}, to: 1, onError: null, onNull: null}}}},
    {$group: {_id: {year: "$year", region: "$Region"}, avg_kwh: {$avg: "$kwh_per_acc"}}},
    {$setWindowFields: { partitionBy: "$_id.region", 
                         sortBy: { "_id.year":1},            
                         output: { prev_year_avg_kwh: {$avg: "$avg_kwh", window: { documents: [-1, -1] } } } } },
    {$addFields: {mvg_diff: {$cond: {if: {$gte: [{$subtract: ["$avg_kwh", "$prev_year_avg_kwh"]}, 0]}, then: 0, else: 1}}}},
    {$match: {"_id.year":{$ne: 2005}}},
    {$group: {_id: "$_id.region", count: {$sum: "$mvg_diff"}}},
    {$sort: {count: -1}}
    ]).limit(3)

// Output: E8, W8 , N7

/*
Question 10: Are there any seasonal (quarterly) effects on energy consumption? Visualizations are typically required to eyeball the effects.
For each region, in each year, compute the quarterly average in <kwh_per_acc>. Exclude “Overall” in <region>.
Note: 1st quarter = January, February, and March, 2nd quarter = April, May, and June, and so on.
*/
db.householdelectricityconsumption.find()

db.householdelectricityconsumption.aggregate([
    {$match:
        {"$and":
        [{Region: {$ne:"Overall"}}, {month:{$ne: "Annual"}}, // This essentiall removes documents where Region is Overall and Month is recorded as annual
        {kwh_per_acc: {$ne: "s\r"}}]}},//This line removes all instances where there is a "s" value
    {$project:
        {year: {$toInt: "$year"}, month: {$toInt: "$month"}, 
        Region: 1, kwh_per_acc: {$convert: {input :{$substr: ["$kwh_per_acc", 0, {$subtract: [{$strLenCP: "$kwh_per_acc"},1]}]}, to: 1, onError: null, onNull:null}}}},
    {$addFields: {quarter: { $switch: { branches: [ {case: {$lte: ["$month", 3]}, then: 1},
                                                    {case: {$lte: ["month",6]}, then: 2},
                                                    {case: {$lte:["month",9]}, then: 3},
                                                    {case: {$lte: ["$month", 12]}, then: 4},],
                                                    default: 4 }}}},
            {$group: {_id: {year: "$year", region : "$Region", quarter: "$quarter"}, avg_kwh: {$avg: "$kwh_per_acc"}}},
            {$sort: {"_id.avg_kwh":1,"_id.region":1, "_id.year": 1,  "id_quarter":1}}
            ])

/*
Question 11: Consider [householdtowngasconsumption]. Are there any seasonal (quarterly) effects on town gas consumption? For each
<sub_housing_type>, in each year, compute the quarterly average in <avg_mthly_hh_tg_consp_kwh>. Exclude “Overall” in <sub_housing_type>.
*/
db.householdtowngasconsumption.aggregate([
    {$match: {sub_housing_type: {$ne: "Overall"}}},
    {$project:{year: {$toInt: "$year"},month:{$convert:{input:"$month", to:16, onError:null, onNull:null}}, //16 for integer
    sub_housing_type:"$sub_housing_type",
    avg_mthly_hh_tg_consp_kwh: {$convert: {input: {$trim:{input: "$avg_mthly_hh_tg_consp_kwh"}}, to: 1, onError: null, onNull: null}}}},
    {$addFields:{quarter: {$switch: {branches:[{case:{$gte:["$month",10]}, then:4},
                                                     {case:{$gte:["$month",7]}, then:3},
                                                     {case:{$gte:["$month",4]}, then:2},
                                                     {case:{$gte:["$month",1]}, then:1}],   
                                                     default:0}}}},
    {$group:{_id:{year: "$year",quarter: "$quarter",sub_housing_type:"$sub_housing_type"},quarter_avg:{$avg: "$avg_mthly_hh_tg_consp_kwh"}}},
    {$match:{"_id.quarter":{$ne: 0}}},
    {$sort:{"_id.sub_housing_type": 1, "_id.year": 1, "_id.quarter": 1}}
    ])
    
/*
Question 12: *Open-ended question* How has Singapore been performing in terms of energy consumption? Find a comparable reference(s) to
illustrate changes in energy per capita, energy per GDP, and various types of energy (e.g., solar, gas, and oil) over the years.
Hint: The formal technique to identify comparable references is “matching” in econometrics (i.e., propensity score matching, see
https://en.wikipedia.org/wiki/Propensity_score_matching). For this question, you may consider countries with somewhat comparable GDP and/or
population).
*/

//Exploring Current Data Sets
db.owid_energy_data.find()
//Noteworthy Attributes: gdp, population, energy_per_gdp, energy_per_capita + Different types of energy consumption

//Finding Comparable References to Singapore
//Creating new collection, population_range, for desired population range
db.owid_energy_data.aggregate([
    {$project: {country: 1, year:{$toInt: "$year"}, 
                population: {$convert:{input: "$population", to: 1, onError: null, onNull: null}}}},
    {$match:{$and:[{country:"Singapore"},{year:2018}]}},
    {$addFields:{
        population_lower_limit:{$add:["$population",-1000000]},
        population_upper_limit:{$add:["$population",1000000]}
    }},
    {$out:{db:"group_project",coll:"population_range"}}
    ])

//Checking that population_range was created correctly
db.population_range.find()

//Creating new collection, gdp_range, for desired gdp range
db.owid_energy_data.aggregate([
    {$project: {country: 1, year:{$toInt: "$year"}, 
                gdp: {$convert:{input: "$gdp", to: 1, onError: null, onNull: null}}}},
    {$match:{$and:[{country:"Singapore"},{year:2018}]}},
    {$addFields:{
        gdp_lower_limit:{$add:["$gdp",-300000000000]},
        gdp_upper_limit:{$add:["$gdp",300000000000]}
    }},
    {$out:{db:"group_project",coll:"gdp_range"}}
    ])
    
//Checking that gdp_range was created correctly
db.gdp_range.find()

//Creating a collection of countries within population_range, called countries_in_population_range
db.owid_energy_data.aggregate([
    {$project: {country: 1, year:{$toInt: "$year"}, 
                population: {$convert:{input: "$population", to: 1, onError: null, onNull: null}}}},
    {$lookup:{
        from:"population_range",
        let:{target_population:"$population"},
        pipeline:[
            {
                $match:{
                    $expr:{$and:[
                        {$gte:["$$target_population","$population_lower_limit"]},
                        {$lte:["$$target_population","$population_upper_limit"]}
                        ]
                }
            }
            },
            {$project:{country:1,year:1,population:1}}
            ],
        as:"population_result"}},
    {$match:{$and:[{year:{$gte:2000}},{year:{$lte:2018}},{"population_result":{$size:1}}]}},
    {$out:{db:"group_project",coll:"countries_in_population_range"}
    ])
    
//Checking that countries_in_population_range was created correctly
db.countries_in_population_range.find()

//Creating a collection of countries within gdp_range, called countries_in_gdp_range
db.owid_energy_data.aggregate([
    {$project: {country: 1, year:{$toInt: "$year"}, 
                gdp: {$convert:{input: "$gdp", to: 1, onError: null, onNull: null}}}},
    {$lookup:{
        from:"gdp_range",
        let:{target_gdp:"$gdp"},
        pipeline:[
            {
                $match:{
                    $expr:{$and:[
                        {$gte:["$$target_gdp","$gdp_lower_limit"]},
                        {$lte:["$$target_gdp","$gdp_upper_limit"]}
                        ]
                }
            }
            },
            {$project:{country:1,year:1,gdp:1}}],
        as:"gdp_result"}},
    {$match:{$and:[{year:{$gte:2000}},{year:{$lte:2018}},{"gdp_result":{$size:1}}]}},
    {$out:{db:"group_project",coll:"countries_in_gdp_range"}
    ])
    
//Checking that countries_in_gdp_range was created correctly
db.countries_in_gdp_range.find()
    
//Joining countries_in_population_range and countries_in_gdp_range
db.countries_in_population_range.aggregate([
    {$project:{country:1, year:{$toInt: "$year"}, population:1}},
    {$lookup:{
        from:"countries_in_gdp_range",
        let:{country_match:"$country",year_match:"$year"}
        pipeline:[
            {
                $match:{
                    $expr:{$and:[
                        {$eq:["$$country_match","$country"]},
                        {$eq:["$$year_match","$year"]}
                        ]}
                }
            }],
        as:"matching_result"
    }},
    {$match:{"matching_result":{$size:1}}}
    ])

/*
Based on this output, nearest reference for gdp + population is Denmark and Finland
Reasoning: Only 2 countries to have all 19 years in range
*/

//Extracting GDP per capita for Singapore, Denmark and Finland
db.owid_energy_data.aggregate([
    {$project:{country:1, year:{$toInt: "$year"},
    population: {$convert:{input: "$population", to: 1, onError: null, onNull: null}},
    gdp: {$convert:{input: "$gdp", to: 1, onError: null, onNull: null}}}},
    {$match:{$and:[
        {year:{$gte:2000, $lte:2018}},
        {country:{$in:["Singapore", "Denmark", "Finland"]}}
        ]}},
    {$addFields:{
        gdp_per_capita:{$divide:["$gdp","$population"]}
    }}
    ])
/*
Analysis 1: Singapore started lower than Denmark and Finland in 2000, but overtook Denmark in 2007, Finland in 2003
Analysis 2: All 3 show increasing trend, but SG has the highest magnitude of change (Nearly tripled)
*/

//Extracting energy per capita for Singapore, Denmark and Finland
db.owid_energy_data.aggregate([
    {$project:{country:1, year:{$toInt: "$year"},
    energy_per_capita: {$convert:{input: "$energy_per_capita", to: 1, onError: null, onNull: null}}}},
    {$match:{$and:[
        {year:{$gte:2000, $lte:2018}},
        {country:{$in:["Singapore", "Denmark", "Finland"]}}
        ]}}
    ])
/*
Analysis 1: Singapore increasing trend, while Denmark and Finland decreasing trend
Analysis 2: Denmark has higher magnitude of decrease
*/

//Comparing energy per gdp for Singapore, Denmark and Finland
db.owid_energy_data.aggregate([
    {$project:{country:1, year:{$toInt: "$year"},
    energy_per_gdp: {$convert:{input: "$energy_per_gdp", to: 1, onError: null, onNull: null}}}},
    {$match:{$and:[
        {year:{$gte:2000, $lte:2018}},
        {country:{$in:["Singapore", "Denmark", "Finland"]}}
        ]}}
    ])
/*
Analysis 1: All 3 experience decreasing trend
Analysis 2: Denmark highest magnitude of decrease, Singapore lowest magnitude of decrease
*/

//Comparing just total energy consumption for Singapore, Denmark and Finland
db.owid_energy_data.aggregate([
    {$project:{country:1, year:{$toInt: "$year"},
    biofuel_consumption: {$convert: {input: "$biofuel_consumption", to: 1, onError: 0, onNull: 0}},
    coal_consumption: {$convert: {input: "$coal_consumption", to: 1, onError: 0, onNull: 0}},
    fossil_fuel_consumption: {$convert: {input: "$fossil_fuel_consumption", to: 1, onError: 0, onNull: 0}},
    gas_consumption: {$convert: {input: "$gas_consumption", to: 1, onError: 0, onNull: 0}},
    hydro_consumption: {$convert: {input: "$hydro_consumption", to: 1, onError: 0, onNull: 0}},
    low_carbon_consumption: {$convert: {input: "$low_carbon_consumption", to: 1, onError: 0, onNull: 0}},
    nuclear_consumption: {$convert: {input: "$nuclear_consumption", to: 1, onError: 0, onNull: 0}},
    oil_consumption: {$convert: {input: "$oil_consumption", to: 1, onError: "0", onNull: 0}},
    other_renewable_consumption: {$convert: {input: "$other_renewable_consumption", to: 1, onError: 0, onNull: 0}},
    primary_energy_consumption: {$convert: {input: "$primary_energy_consumption", to: 1, onError: 0, onNull: 0}},
    renewables_consumption: {$convert: {input: "$renewables_consumption", to: 1, onError: 0, onNull: 0}},
    solar_consumption: {$convert: {input: "$solar_consumption", to: 1, onError: 0, onNull: 0}},
    wind_consumption: {$convert: {input: "$wind_consumption", to: 1, onError: 0, onNull: 0}}}},
    {$match:{$and:[
        {year:{$gte:2000, $lte:2018}},
        {country:{$in:["Singapore", "Denmark", "Finland"]}}
        ]}},
    {$addFields:{
        total_energy_consumption:{$add:["$biofuel_consumption","$coal_consumption","$fossil_fuel_consumption","$gas_consumption",
        "$hydro_consumption","$low_carbon_consumption","$nuclear_consumption","$oil_consumption","$other_renewable_consumption",
        "$primary_energy_consumption","$renewables_consumption","$solar_consumption","$wind_consumption"]}
    }}
    ])
/*
Analysis: Singapore increased drastically (More than double), Denmark and Finland decreased slightly
*/

/*
Question 13: *Open-ended question* Can renewable energy adequately power continued economic growth?
*/
db.owid_energy_data.find()

// First step is to only show the documents where there are no blank values
db.owid_energy_data.aggregate([
    {$match:
        {"$and":
            [{biofuel_consumption: {$ne: ""}}, {coal_consumption: {$ne: ""}}, {fossil_fuel_consumption: {$ne: ""}}, {gas_consumption: {$ne: ""}},
            {hydro_consumption: {$ne: ""}}, {low_carbon_consumption: {$ne: ""}}, {nuclear_consumption: {$ne: ""}}, {oil_consumption: {$ne: ""}}, 
            {other_renewable_consumption:{$ne: ""}}, {primary_energy_consumption:{$ne: ""}}, {renewables_consumption: {$ne: ""}},
            {solar_consumption: {$ne: ""}}, {wind_consumption:{$ne: ""}}
            ]}
    },
    {$project:
        {_id:0, 
        country:1, year:1, biofuel_consumption:1,coal_consumption:1,fossil_fuel_consumption:1,gas_consumption:1,
        hydro_consumption:1,low_carbon_consumption:1,nuclear_consumption:1, oil_consumption:1,other_renewable_consumption:1,
        primary_energy_consumption:1,renewables_consumption:1,solar_consumption:1, wind_consumption:1, total_consumption:1
        }},
    {$sort: 
        {country:1}}}
        ])
        
// Next step is to sum up all the consumption based on the country and the year
// How much each country requires / Year
db.owid_energy_data.aggregate([
    {$match:
        {"$and":
            [{biofuel_consumption: {$ne: ""}}, {coal_consumption: {$ne: ""}}, {fossil_fuel_consumption: {$ne: ""}}, {gas_consumption: {$ne: ""}},
            {hydro_consumption: {$ne: ""}}, {low_carbon_consumption: {$ne: ""}}, {nuclear_consumption: {$ne: ""}}, {oil_consumption: {$ne: ""}}, 
            {other_renewable_consumption:{$ne: ""}}, {primary_energy_consumption:{$ne: ""}}, {renewables_consumption: {$ne: ""}},
            {solar_consumption: {$ne: ""}}, {wind_consumption:{$ne: ""}},
            {year: {$gte: "2000"}}
            ]}
    },
    {$project:
        {country:1, year:1
        biofuel_consumption: {$convert: {input: "$biofuel_consumption", to: 1, onError: null, onNull: null}},
        coal_consumption: {$convert: {input: "$coal_consumption", to: 1, onError: null, onNull: null}},
        fossil_fuel_consumption: {$convert: {input: "$fossil_fuel_consumption", to: 1, onError: null, onNull: null}},
        gas_consumption: {$convert: {input: "$gas_consumption", to: 1, onError: null, onNull: null}},
        hydro_consumption: {$convert: {input: "$hydro_consumption", to: 1, onError: null, onNull: null}},
        low_carbon_consumption: {$convert: {input: "$low_carbon_consumption", to: 1, onError: null, onNull: null}},
        nuclear_consumption: {$convert: {input: "$nuclear_consumption", to: 1, onError: null, onNull: null}},
        oil_consumption: {$convert: {input: "$oil_consumption", to: 1, onError: null, onNull: null}},
        other_renewable_consumption: {$convert: {input: "$other_renewable_consumption", to: 1, onError: null, onNull: null}},
        primary_energy_consumption: {$convert: {input: "$primary_energy_consumption", to: 1, onError: null, onNull: null}},
        renewables_consumption: {$convert: {input: "$renewables_consumption", to: 1, onError: null, onNull: null}},
        solar_consumption: {$convert: {input: "$solar_consumption", to: 1, onError: null, onNull: null}},
        wind_consumption: {$convert: {input: "$wind_consumption", to: 1, onError: null, onNull: null}}}},
    {$addFields:{
        total_consumption: {$add: ["$biofuel_consumption","$coal_consumption","$fossil_fuel_consumption","$gas_consumption","$hydro_consumption","$low_carbon_consumption",
        "$nuclear_consumption","$oil_consumption","$other_renewable_consumption","$primary_energy_consumption","$renewables_consumption","$solar_consumption",
        "$wind_consumption"]
        }}},
    {$project:{
        _id:0, country:1, year:1,total_consumption:1
    }}
])


// The above query tells us how much each country uses per year
// Hence to better understand how much energy is needed, we should look at energy generated per country as well this includes renewable and non-renewable
db.owid_energy_data.aggregate([
    {$match:
        {"$and":
            [{biofuel_electricity: {$ne: ""}}, {coal_electricity: {$ne: ""}}, {electricity_generation: {$ne: ""}}, {fossil_electricity: {$ne: ""}},
            {gas_electricity: {$ne: ""}}, {hydro_electricity: {$ne: ""}}, {low_carbon_electricity: {$ne: ""}}, {nuclear_electricity: {$ne: ""}}, 
            {oil_electricity:{$ne: ""}}, {other_renewable_electricity:{$ne: ""}}, {other_renewable_exc_biofuel_electricity: {$ne: ""}},
            {renewables_electricity: {$ne: ""}}, {solar_electricity:{$ne: ""}}, {wind_electricity:{$ne: ""}},
            {year: {$gte: "2000"}}
            ]}
    },
    {$project:
        {country:1, year:1,
        biofuel_electricity: {$convert: {input: "$biofuel_electricity", to: 1, onError: null, onNull: null}},
        coal_electricity: {$convert: {input: "$coal_electricity", to: 1, onError: null, onNull: null}},
        electricity_generation: {$convert: {input: "$electricity_generation", to: 1, onError: null, onNull: null}},
        fossil_electricity: {$convert: {input: "$fossil_electricity", to: 1, onError: null, onNull: null}},
        gas_electricity: {$convert: {input: "$gas_electricity", to: 1, onError: null, onNull: null}},
        hydro_electricity: {$convert: {input: "$hydro_electricity", to: 1, onError: null, onNull: null}},
        low_carbon_electricity: {$convert: {input: "$low_carbon_electricity", to: 1, onError: null, onNull: null}},
        nuclear_electricity: {$convert: {input: "$nuclear_electricity", to: 1, onError: null, onNull: null}},
        oil_electricity: {$convert: {input: "$oil_electricity", to: 1, onError: null, onNull: null}},
        other_renewable_electricity: {$convert: {input: "$other_renewable_electricity", to: 1, onError: null, onNull: null}},
        other_renewable_exc_biofuel_electricity: {$convert: {input: "$other_renewable_exc_biofuel_electricity", to: 1, onError: null, onNull: null}},
        renewables_electricity: {$convert: {input: "$renewables_electricity", to: 1, onError: null, onNull: null}},
        solar_electricity: {$convert: {input: "$solar_electricity", to: 1, onError: null, onNull: null}},
        wind_electricity: {$convert: {input: "$wind_electricity", to: 1, onError: null, onNull: null}}}},

    {$addFields:{
        total_generated: {$add: ["$biofuel_electricity","$coal_electricity","$electricity_generation","$fossil_electricity","$gas_electricity","$hydro_electricity",
        "$low_carbon_electricity","$nuclear_electricity","$oil_electricity","$other_renewable_electricity","$other_renewable_exc_biofuel_electricity","$renewables_electricity",
        "$solar_electricity","$wind_electricity"]
        }}},
    {$project:{
        _id:0, country:1, year:1,total_generated:1
    }}
])



// High Level Overview of Energy Share amongst different countries
db.owid_energy_data.aggregate([
    {$project: {country: 1, 
        year: {$convert: {
            input: "$year", to: "int", onError: 0, onNull: 0}}, 
        gdp: {$convert: {
            input: "$gdp", to: "double", onError: "remove", onNull: "remove"}}, 
        population: {$convert: {
            input: "$population", to: "double", onError: "remove", onNull: "remove"}}, 
        biofuel_share_energy: {$convert: {
            input: "$biofuel_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        coal_share_energy: {$convert: {
            input: "$coal_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        fossil_share_energy: {$convert: {
            input: "$fossil_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        gas_share_energy: {$convert: {
            input: "$gas_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        hydro_share_energy: {$convert: {
            input: "$hydro_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        low_carbon_share_energy: {$convert: {
            input: "$low_carbon_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        nuclear_share_energy: {$convert: {
            input: "$nuclear_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        oil_share_energy: {$convert: {
            input: "$oil_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        other_renewables_share_energy: {$convert: {
            input: "$other_renewables_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        renewables_share_energy: {$convert: {
            input: "$renewables_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        solar_share_energy: {$convert: {
            input: "$solar_share_energy", to: "double", onError: "remove", onNull: "remove"}}, 
        wind_share_energy: {$convert: {
            input: "$wind_share_energy", to: "double", onError: "remove", onNull: "remove"}}}},
    {$match: 
        {$and: [
              {gdp: {$ne: "remove"}},
              {population: {$ne: "remove"}},
              {biofuel_share_energy: {$ne: "remove"}},
              {coal_share_energy: {$ne: "remove"}},
              {fossil_share_energy: {$ne: "remove"}},
              {gas_share_energy: {$ne: "remove"}},
              {hydro_share_energy: {$ne: "remove"}},
              {low_carbon_share_energy: {$ne: "remove"}},
              {nuclear_share_energy: {$ne: "remove"}},
              {oil_share_energy: {$ne: "remove"}},
              {other_renewables_share_energy: {$ne: "remove"}},
              {renewables_share_energy: {$ne: "remove"}},
              {solar_share_energy: {$ne: "remove"}},
              {wind_share_energy: {$ne: "remove"}},
              {year: {$gte: 2000}}

        }
    ])
  
// Looking at the trend of previous years versus current years for big players AKA China, Russia, USA, India
db.owid_energy_data.aggregate([
    {$match:
        {$and:[
            {country: {$in:['China','United States','Japan', 'Germany']}},
            {renewables_electricity: {$ne: " "}},
            {fossil_electricity: {$ne: " "}},
            {gdp: {$ne: " "}},
            {year:{$gte: "2000"}}
              ]}},
    {$addFields:{
        renewables_electricity: {$convert: {
            input: {$substr: ["$renewables_electricity", 0, {$subtract: [{$strLenCP: "$renewables_electricity"}, 1]}]}, to: 1, onError: null, onNull: null}},
        fossil_electricity: {$convert: {
            input: {$substr: ["$fossil_electricity", 0, {$subtract: [{$strLenCP: "$fossil_electricity"}, 1]}]}, to: 1, onError: null, onNull: null}},
        gdp: {$convert: {
            input: {$substr: ["$gdp", 0, {$subtract: [{$strLenCP: "$gdp"}, 1]}]}, to: 1, onError: null, onNull: null}}
    }},
    {$setWindowFields: {
     partitionBy: '$country',
     sortBy: {'country': 1, 'year': 1},
     output: {
         'renewable_prev': {$avg: "$renewables_electricity",window: {documents: [-1, -1]}},
         'fossil_energy_prev': {$avg: "$fossil_electricity",window: {documents: [-1,-1]}}
         'gdp_prev': {$avg: "$gdp",window: {documents: [-1,-1]}}
     }
    }},
    {$project:{
        country: 1,
        year: {$toInt: "$year"},
        renewables_electricity: 1,
        renewable_change: {$subtract: ["$renewables_electricity", "$renewable_prev"]},
        fossil_electricity: 1,
        fossil_change: {$subtract: ["$fossil_electricity","$fossil_energy_prev"]},
        gdp: 1
        gdp_change: {$subtract: ["$gdp","$gdp_prev"]
    }}
        ])

  
// Looking at the trend of previous years versus current years for ASEAN Players
db.owid_energy_data.aggregate([
    {$match:
        {$and:[
            {country: {$in:['Singapore','Malaysia','Brunei', 'Cambodia', 'Indonesia', 'Laos', 'Myanmar', 'Philippines', 'Thailand', 'Vietnam']}},
            {renewables_electricity:{$ne: ""}},
            {fossil_electricity: {$ne: ""}},
            {gdp: {$ne: ""}},
            {year:{$ne: ""}}
              ]}},
    {$addFields:{
        renewables_electricity: {$convert: {
            input: {$substr: ["$renewables_electricity", 0, {$subtract: [{$strLenCP: "$renewables_electricity"}, 1]}]}, to: 1, onError: null, onNull: null}},
        fossil_electricity: {$convert: {
            input: {$substr: ["$fossil_electricity", 0, {$subtract: [{$strLenCP: "$fossil_electricity"}, 1]}]}, to: 1, onError: null, onNull: null}},
        gdp: {$convert: {
            input: {$substr: ["$gdp", 0, {$subtract: [{$strLenCP: "$gdp"}, 1]}]}, to: 1, onError: null, onNull: null}}
    }},
    {$setWindowFields: {
     partitionBy: '$country',
     sortBy: {'country': 1, 'year': 1},
     output: {
         'renewable_prev': {$avg: "$renewables_electricity",window: {documents: [-1, -1]}},
         'fossil_energy_prev': {$avg: "$fossil_electricity",window: {documents: [-1,-1]}}
         'gdp_prev': {$avg: "$gdp",window: {documents: [-1,-1]}}
     }
    }},
    {$project:{
        country: 1,
        year: {$toInt: "$year"},
        renewables_electricity: 1,
        renewable_change: {$subtract: ["$renewables_electricity", "$renewable_prev"]},
        fossil_electricity: 1,
        fossil_change: {$subtract: ["$fossil_electricity","$fossil_energy_prev"]},
        gdp: 1
        gdp_change: {$subtract: ["$gdp","$gdp_prev"]
    }}}
        ]])

/*
Question 14: *Open-ended question* Say micro-nuclear reactors (see https://energypost.eu/micro-nuclear-reactors-up-to-20mw-portable-safer/)
have become environmentally viable and economically feasible for Singapore. Shall we go nuclear? Why / why not?
Substantiate your team’s opinion with the data provided.
*/
db.owid_energy_data.aggregate([
    {$match: {"country": "Singapore"}},
    {$match: {"$or": [{"year": "1985"}, {"year": "1986"},{"year": "2020"}]}},
    {$project: {country: 1, year: 1, fossil_share_energy: 1, solar_share_energy: 1, nuclear_electricity: 1}}
    ])

db.householdelectricityconsumption.aggregate([
    {$match:
        {"$and":
        [{Region: {$ne:"Overall"}}, {month:{$ne: "Annual"}}, // This essentially removes documents where Region is Overall and Month is recorded as annual
        {kwh_per_acc: {$ne: "s\r"}}]}},//This line removes all instances where there is a "s" value
    {$project:
        {year: {$toInt: "$year"}, month: {$toInt: "$month"}, 
        Region: 1, kwh_per_acc: {$convert: {input :{$substr: ["$kwh_per_acc", 0, {$subtract: [{$strLenCP: "$kwh_per_acc"},1]}]}, to: 1, onError: null, onNull:null}}}},
    {$addFields: {quarter: { $switch: { branches: [ {case: {$lte: ["$month", 3]}, then: 1},
                                                    {case: {$lte: ["month",6]}, then: 2},
                                                    {case: {$lte:["month",9]}, then: 3},
                                                    {case: {$lte: ["$month", 12]}, then: 4},],
                                                    default: 4 }}}},
            {$group: {_id: {year: "$year", region : "$Region", quarter: "$quarter"}, avg_kwh: {$avg: "$kwh_per_acc"}}},
            {$sort: {"_id.region":1, "_id.year": 1,  "id_quarter":1}}
            ])

/*
Question 15: *Blue-sky question* Despite the increasing awareness of environmental issues, some remain skeptical about climate change being
a problem (see https://www.bbc.com/news/science-environment-62225696). Using the data provided in this project and the individual assignment
(as well as any other publicly available data, if your team shall desire), build a convincing data narrative to illustrate climate change
problems associated with emissions.
*/
//Table 1: globalTempChange
db.temperaturechangebycountry.find()

db.temperaturechangebycountry.aggregate([
    {$project: {Area:1, Year:1, Value:{$convert:{input: "$Value", to: 1, onError: null, onNull: null}}}},
    {$project: {Area:1, Year:1, avgValueChange:{$avg: "$Value"}}}
    {$group: {_id:{"Area": "World", "Year" : "$Year"}, "Average_Value": {$avg: "$avgValueChange"}}},
    {$sort:{"_id.Year":1}},
    {$out: "globalTempChange"}
    ])
    
db.globalTempChange.find({})

//Table 2: globalTempChange_GHGemissions
//Document 1: globalTempChange

db.globalTempChange.find({})

db.globalTempChange.aggregate([
    {$project:{"_id.Area":1, "_id.Year":1, "tempChange" :"$Average_Value"}},
    {$out: "globalTempChangeTEMP"}
    ])

db.globalTempChangeTEMP.find()

//Document 2: owid_energy_data

db.owid_energy_data.find({"country": "World"})

db.owid_energy_data.aggregate([
    {$project:{
    population:{$convert:{input: "$population", to: 1, onError: null, onNull: null}},
    greenhouse_gas_emissions:{$convert:{input: "$greenhouse_gas_emissions", to: 1, onError: null, onNull: null}},
    year: 1, country: 1
    }},
    {$project:{ greenhouse_gas_emissions: 1,"greenhouse_gas_emissions_per_capita": {$divide: ["$greenhouse_gas_emissions", "$population"]}, year: 1, country: 1}},
    {$match: {$and: [{"country": "World"}, {"greenhouse_gas_emissions": {$ne: null}}]}},
    {$out: "owid_energy_dataTEMP"}])


db.owid_energy_dataTEMP.find()

//Combine: 
db.owid_energy_dataTEMP.aggregate([
    {$lookup:
        {
            from: "globalTempChangeTEMP", 
            localField:"year",
            foreignField:"_id.Year",
            as: "globalTempChange_GHGemissions"
        }
    }}
])

//Table 3: gdp_emissions_cap

db.owid_energy_data.aggregate([
    {$project:{_id:0, iso_code:1, country:1, year:1, 
    gdp:{$convert:{input: "$gdp", to: 1, onError: null, onNull: null}},
    population:{$convert:{input: "$population", to: 1, onError: null, onNull: null}},
    greenhouse_gas_emissions:{$convert:{input: "$greenhouse_gas_emissions", to: 1, onError: null, onNull: null}}}},
    {$project:{_id:0, iso_code:1, "isox":{$strLenCP:"$iso_code"}, country:1, year:1, gdp: 1, population : 1,
    "gdp_per_capita": {$divide: ["$gdp", "$population"]}, greenhouse_gas_emissions : 1,
    "greenhouse_gas_emissions_per_capita": {$divide: ["$greenhouse_gas_emissions", "$population"]}}},
    {$match: {"greenhouse_gas_emissions": {$ne: null}}},
    {$match: { $or: [ {isox: 3}, {"country": "World"}, {"country": "Greenland"}, {"country": "Kosovo"}]}},
    {$out: "gdp_emissions_cap"}])

db.gdp_emissions_cap.find()


//Table 4: energy
db.owid_energy_data.aggregate([
    {$project:{_id:0, iso_code:1, country:1, year:1, 
    gdp:{$convert:{input: "$gdp", to: 1, onError: null, onNull: null}},
    population:{$convert:{input: "$population", to: 1, onError: null, onNull: null}},
    greenhouse_gas_emissions:{$convert:{input: "$greenhouse_gas_emissions", to: 1, onError: null, onNull: null}},
    energy_cons_change_pct:1, energy_per_capita:1, fossil_fuel_consumption:1, fossil_energy_per_capita:1, renewables_consumption:1,renewables_energy_per_capita:1,  nuclear_consumption:1, nuclear_energy_per_capita:1}},
    {$project:{_id:0, iso_code:1, "isox":{$strLenCP:"$iso_code"}, country:1, year:1, "gdp_per_capita": {$divide: ["$gdp", "$population"]}, greenhouse_emissions_per_capita: {$divide: ["$greenhouse_gas_emissions", "$population"]}, energy_cons_change_pct:1, energy_per_capita:1, fossil_fuel_consumption:1, fossil_energy_per_capita:1, renewables_consumption:1,renewables_energy_per_capita:1,  nuclear_consumption:1, nuclear_energy_per_capita:1}},
    {$match: { $or: [ {isox: 3}, {"country": "World"}, {"country": "Greenland"}, {"country": "Kosovo"}]}},
    {$out: "energy"}])

db.energy.find()