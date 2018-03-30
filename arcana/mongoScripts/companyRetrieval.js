var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var obj = {
    table: []
};

var uniqueCompanies = [];

var companyArray = {
    companies: []
};

MongoClient.connect(url, function (err, db) {
    if (err) throw err;
    var dbo = db.db("arcana");
    
    var query = {
        companyName: {
            $exists: true
        }
    };

    dbo.collection("constants").find(query).toArray(function (err, result) {
        if (err) throw err;
        result.forEach(app => {
            companyName = app.companyName;
            obj.table.push(companyName);
        });

        uniqueCompanies = obj.table.filter(function(item, pos, self) {
            return self.indexOf(item) == pos;
        })

        uniqueCompanies.forEach(company => {
            let compObj = {
                companyName:"",
                categories:[],
                size:"",
                marketCapital:0,
                internalPoc:"",
                externalPoc:"",
                appTotal:0,
                reviewsTotal:0,
                ratingAvg:0
            };
            compObj.companyName = company;
            companyArray.companies.push(compObj);
        });

        dbo.collection("companies").insertMany(companyArray.companies, function(err, res) {
            if (err) throw err;
            console.log("Number of documents inserted: " + res.insertedCount);
        });

        console.log(companyArray);
        db.close();
    });

});
