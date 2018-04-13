const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var urlArray = [];
var procArray = new Array();
let results = new Array();
let promiseArray = new Array();

var readfiledirectory = __dirname + '/../rawdata/'+ dateString + '.txt';
var writefiledirectory = __dirname + '/../constants/' + dateString + '_const.json'

console.log('>>>>>', readfiledirectory);
//console.log(writefiledirectory);

let ws = fs.createWriteStream(writefiledirectory);

bundle(readfiledirectory);

function bundle(readfiledirectory) {
    console.log(readfiledirectory, ' >>>>>>>>>>In the Bundle Context');
    createArray(readfiledirectory)
        .then(async data => {
            subData = data.splice(0,10);
            processBatch(subData, 10, procArray).then((processed)=>{
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
        let domain =  document.querySelector('div.appx-extended-detail-subsection-description.slds-truncate > a');
        domain = domain ? domain.innerText : '';

        return {
            appTitle,
            companyName,
            dateListed,
            category,
            domain
        }
    });

    let urlData = {
        id: bbyid,
        appName: result.appTitle,
        companyName: result.companyName,
        dateListed: result.dateListed,
        category: result.category,
        domain: result.domain
    }
    await browser.close();
    return urlData;
}

function createArray(readfiledirectory){
    return new Promise((resolve, reject) => {
        console.log(readfiledirectory, ' In the creat Array Context');
        fs.readFile(readfiledirectory, 'utf8', function (err, contents) {
            if (err) {
                reject(err)
            } else {
                resolve(contents.replace(/"/g, '').replace('[', '').replace(']', '').split(','))
            }
        })
    })
}
