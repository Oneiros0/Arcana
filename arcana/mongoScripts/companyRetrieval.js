var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var company = {
    companyName:'',
    domainName:'',
    categories:[]
};

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
        console.log(result.length);
        result.forEach(app => {
            let company = {
                companyName: app.companyName,
                domainName: app.domain,
                categories: app.category
            }
            console.log(company);
        });

        // dbo.collection("companies").insertMany(companyArray.companies, function(err, res) {
        //     if (err) throw err;
        //     console.log("Number of documents inserted: " + res.insertedCount);
        // });

        db.close();
    });

});
