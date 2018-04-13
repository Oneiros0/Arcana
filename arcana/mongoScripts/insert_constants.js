var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');

var url = "mongodb://localhost:27017/";

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var constArr = new Array();

function createArray() {
    return new Promise((resolve, reject) => {
        fs.readFile(__dirname + '/../constants/'+ dateString + '_const.json', 'utf8', function (err, contents) {
            if (err) {
                reject(err)
            } else {
                resolve(constArr = JSON.parse(contents))
            }
        })
    })
}

createArray().then(data => {
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        var dbo = db.db("arcana");
        dbo.collection("constants").insertMany(data, function(err, res) {
          if (err) throw err;
          console.log("Number of documents inserted: " + res.insertedCount);
          db.close();
        });
      });
})

// MongoClient.connect(url, function(err, db) {
//   if (err) throw err;
//   var dbo = db.db("arcana");

//   dbo.collection("constants").insertMany(myobj, function(err, res) {
//     if (err) throw err;
//     console.log("Number of documents inserted: " + res.insertedCount);
//     db.close();
//   });
// });