const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var urlArray = [];
var procArray = [];
var appNameClass = "appx-page-header-root";
var companyNameClass = "appx-company-name";
var listedXPATH = "(//div[@class='appx-detail-section-first-listed']//p)[2]";
var lastReleaseXPATH = "(//span[@class='appx-detail-section-last-update']//p)[2]";
var overviewTabXPATH = "//li[@title='Overview']//a"
var ratingsTabXPath = "//li[@title='Reviews']//a";
var ratingCountClass = "appx-rating-amount";
var ratingValueClass = "appx-average-rating-numeral";

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

let ws = fs.createWriteStream(__dirname + '/rankdata/' + dateString + '_rankings.tsx');

fs.readFile(__dirname + '/rawdata/' + dateString + '.tsx', 'utf8', function (err, contents) {
    urlArray = contents.replace(/"/g,'').replace('[', '').replace(']', '').split(',');
    console.log(urlArray.length);
    for(var i = 0; i < 3000; i++){
        if(i != 2999){
            ws.write(`{id: '${urlArray[i]}', position: '${i}'}, \n`);        
        }else{
            ws.write(`{id: '${urlArray[i]}', position: '${i}'} \n`);
        }
    }
});

return
