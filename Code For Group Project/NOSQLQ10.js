db.household_electricity_consumption.aggregate([
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
            {$group: {_id: {year: "$year", region : "$Region", quarter: "quarter"}, avg_kwh: {$avg: "$kwh_per_acc"}}},
            {$sort: {"_id.region":1, "_id.year": 1,  "id_quarter":1}}
            ])