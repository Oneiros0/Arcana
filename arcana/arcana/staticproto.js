const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var urlArray = [];
var procArray = new Array();
let results = new Array();
let promiseArray = new Array();
var appNameClass = ".appx-page-header-root";
var companyNameClass = "appx-company-name";
var listedXPATH = "(//div[@class='appx-detail-section-first-listed']//p)[2]";
var lastReleaseXPATH = "(//span[@class='appx-detail-section-last-update']//p)[2]";
var overviewTabXPATH = "//li[@title='Overview']//a"
var ratingsTabXPath = "//li[@title='Reviews']//a";
var ratingCountClass = "appx-rating-amount";
var ratingValueClass = "appx-average-rating-numeral";
var category = "//div[@class='appx-detail-section appx-headline-details-categories']//a//strong";

let ws = fs.createWriteStream(__dirname + '/constants/' + dateString + '_const.json');

bundle();

// function staticArcana(){
    
// }

async function bundle() {
    createArray()
        .then(async data => {
            //while (data.length > 0) {
            subData = data.splice(0,data.length);
            processBatch(subData, 10, procArray).then((processed)=>{
                console.log("Result Set 2", procArray.length);
                for(let i = 0; i < procArray.length; i++){
                    for(let j = 0; j < procArray[i].length; j++){
                       results.push(procArray[i][j]);
                    }
                }
                console.log(results);
                ws.write(JSON.stringify(results));
            });
            console.log("After Promise All", );
        })
}

function processBatch(masterList, batchSize, procArray){
    return Promise.all(masterList.splice(0, batchSize).map(async url => {
        console.log("In Promise All");
        return singleScrape(url) //.then(listing => console.log(listing));
    })).then((results) => {
        if (masterList.length < batchSize) {
            console.log('done');
            procArray.push(results);
            return procArray;
        } else {
            console.log('MasterList Size :: ' + masterList.length);
            procArray.push(results);
            return processBatch(masterList, batchSize, procArray);
        }
    })
}

async function singleScrape(url) {
    let bbyid = url.substring(url.indexOf('=')+1);
    let browser = await puppeteer.launch({
        headless: true
    });
    let page = await browser.newPage();
    await page.goto(url, {
        timeout: 0
    });

    await page.waitFor(1000);
    let result = await page.evaluate(() => {
        let appTitle = document.querySelector('.appx-page-header-2_title');
        appTitle = appTitle ? appTitle.innerText : '';
        let companyName = document.querySelector('.appx-company-name');
        companyName = companyName ? companyName.innerText : '';
        let dateListed = document.querySelector('.appx-detail-section-first-listed p:nth-child(2)');
        dateListed = dateListed ? dateListed.innerText : '';
        let category = document.querySelector('.appx-detail-section:nth-child(3) a strong');
        category = category ? category.innerText : '';

        return {
            appTitle,
            companyName,
            dateListed,
            category
        }
    });

    let urlData = {
        id: bbyid,
        appName: result.appTitle,
        companyName: result.companyName,
        dateListed: result.dateListed,
        category: result.category
    }
    await browser.close();
    return urlData;
}

function createArray() {
    return new Promise((resolve, reject) => {
        fs.readFile(__dirname + '/rawdata/' + dateString + '.txt', 'utf8', function (err, contents) {
            if (err) {
                reject(err)
            } else {
                resolve(contents.replace(/"/g, '').replace('[', '').replace(']', '').split(','))
            }
        })
    })
}

//Old code for references

// async function sampleTimeout() {
//     setTimeout(() => {
//         return
//     }, 1000);
// }

// async function recursive(list, currIndex) {
//     await sampleTimeout();
//     if (currIndex < list.length) {
//         singleScrape(list[currIndex])
//             .then(listing => console.log(listing));
//         recursive(list, currIndex++);
//     } else {
//         return
//     }
// }

   //
        // let dateListed = document.evaluate(
        //     "(//div[@class='appx-detail-section-first-listed']//p)[2]",
        //     document,
        //     null,
        //     XPathResult.FIRST_ORDERED_NODE_TYPE,
        //     null
        // ).singleNodeValue.innerText;
        // let category = document.evaluate(
        //     "//div[@class='appx-detail-section appx-headline-details-categories']//a//strong",
        //     document,
        //     null,
        //     XPathResult.FIRST_ORDERED_NODE_TYPE,
        //     null
        // ).singleNodeValue.innerText;
        /*  */