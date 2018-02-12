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

let ws = fs.createWriteStream('staticContent.tsx');

fs.readFile(__dirname + '/rawdata/' + dateString + '.tsx', 'utf8', function (err, contents) {
    urlArray = contents.replace(/"/g, '').replace('[', '').replace(']', '').split(',');
    console.log(urlArray.length);
    procArray = urlArray;
    let obj = singleScrape(procArray[0]);
    console.log(obj);
})

async function singleScrape(url) {
    const browser = await puppeteer.launch();
    let page = await browser.newPage();
    await page.goto(url, {
        timeout: 0
    });
    await page.waitFor(1000);
    let result = await page.evaluate(() => {
        let appTitle = document.querySelector('.appx-page-header-root').innerText;
        let companyName = document.querySelector('.appx-company-name').innerText;
        let dateListed = document.evaluate("(//div[@class='appx-detail-section-first-listed']//p)[2]", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.innerText;
        let category = document.evaluate("//div[@class='appx-detail-section appx-headline-details-categories']//a//strong", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.innerText;
        /*  */
        return {
            appTitle,
            companyName,
            dateListed,
            category
        }
    });
    let urlData = {
        id: url,
        appName: result.appTitle,
        companyName: result.companyName,
        dateListed: result.dateListed,
        category: result.category
    }
    await browser.close();
    return urlData;
}

function hazzit(){
    console.log(procArray[0]);
}