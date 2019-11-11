var mysql = require('mysql');
const fs = require('fs');
const { Pool, Client } = require('pg')

var readfiledirectory = __dirname + '/../dynamic/10_5_2019_dynamic_total.json';

const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'Ishtar842!',
    port: 5432,
  })

  client.connect();
  
fs.readFile(readfiledirectory, (err, data) => {
    if (err) throw err;
    let appList = JSON.parse(data);
    for(app in appList){
        // console.log(appList[app]);
    }
    var values = [];
    
    for(var i=0; i < appList.length; i++){
    var update = appList[i].lastUpdate;
    var listed = appList[i].dateListed;

    listed = formatDateString(listed);
    update = formatDateString(update);

    values.push([
            appList[i].id, appList[i].appName, appList[i].companyName, 
            listed, update, appList[i].category, 
            appList[i].domain, appList[i].price, Number(appList[i].totalReviews), 
            Number(appList[i].overallRating)
        ]);
    }

    var sql = "INSERT INTO public.arcana_data (id, appName, companyName, dateListed, lastUpdated, category, domain, price, totalReviews, overallRating)"
    + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    for(value in values){
        client.query(sql, values[value], (err, res) => {
            console.log(err, res)
            // client.end()
        })
    }
    // for(value in values){
    //     connection.query(sql, [values[value]], function(err) {
    //         if (err) throw err.sqlMessage;
    //     });
    // }
 });

 function formatMonth(dateString){
    var newString = '';
    if(dateString.charAt(0) === '1'){
        if(dateString.charAt(1) === '/'){
            newString = "0" + dateString;
        }else{
            return dateString;
        }
    }else{
        newString = "0" + dateString
    }
    return newString;
};

function formatDay(dateString){
    var newString = '';
    if(dateString.charAt(4) === '/'){
        newString = [dateString.slice(0, 3), "0", dateString.slice(3)].join('');
        return newString;
    }else{
        return dateString;
    }
};

function formatDateString(dateString){
    var newString;
    newString = formatMonth(dateString);
    newString = formatDay(newString);
    return newString;
}