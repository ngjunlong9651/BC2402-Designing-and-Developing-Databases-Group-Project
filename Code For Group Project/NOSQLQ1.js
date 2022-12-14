// Question 1
db.proper_countries.aggregate([
    {$group: {_id: "$alpha3"}},
    {$count: "Amount in Proper Countries Database:"}])
// This shows a that there are 193 instances of countries within this collection
    
db.owid_energy_data.aggregate([
    {$group : {_id: "$iso_code"}},
    {$count : "Amount of countries in Owid_Energy_data"}]) 
// This shows that there is a total of 223 countries in the second collection

db.getCollection("owid_energy_data").aggregate([
    {$lookup:
        {
            from:"proper_countries",
            localField: "iso_code",
            foreignField:"alpha3",
            as: "CombinedCountryCount"
        }
    },
    {
        $addFields:
        {
            countryCount:
            {$size : "$CombinedCountryCount"}
        }
    }])




    
    
    
 