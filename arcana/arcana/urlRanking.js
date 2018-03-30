const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var urlArray = [];
var procArray = [];

var obj = {
    table: []
 };

//TODO: Research how to find the element via tagName.
var categoryClass = "strong";

var urlData = {
    id: '',
    position: '',
    companyName: '',
    appName: '',
    ratingAvg: '',
    numOfRating: ''
}

let ws = fs.createWriteStream('../' + __dirname + '/rankdata/' + dateString + '_rankings.json',);

function createArray() {
    return new Promise((resolve, reject) => {
        fs.readFile(__dirname + '/rawdata/' + dateString + '.txt', 'utf8', function (err, contents) {
            if (err) {
                reject(err)
            } else {
                let jsonData = JSON.parse(contents);
                resolve(jsonData);
            }
        })
    })
}

createArray().then(payload => {
    let counter = 0;
    payload.forEach(item => {
        let id = item.substring(item.indexOf('=')+1);
        console.log(id);
        obj.table.push(`{id: ${id}, position: "${counter}", timestamp: "${dateString}"}`);
        counter++;
    });
    
    let strArr = JSON.stringify(obj);
    ws.write(strArr);
    process.exit();
});