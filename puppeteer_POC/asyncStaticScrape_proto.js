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

var urlMax = 0;

var urlData = {
    id: '',
    position: '',
    companyName: '',
    appName: '',
    ratingAvg: '',
    numOfRating: ''
}

let ws = fs.createWriteStream('staticContent.tsx');

fs.readFile(dateString + '.tsx', 'utf8', function (err, contents) {
    urlArray = contents.replace(/"/g, '').replace('[', '').replace(']', '').split(',');
    console.log(urlArray.length);
    urlMax = urlArray.length;
    console.log(urlMax + " Value of the urlMax");
    subProc(urlArray, 5);
    console.log(urlArray.length);
});

async function subProc(list, batchSize) {
    let subList = null;
    let i = 0;

    while (list.length > 0) {
        let browser = await puppeteer.launch();
        subList = list.splice(0, batchSize);
        console.log("Master List Size :: " + list.length);
        console.log("SubList Size :: " + subList.length);

        for (let j = 0; j < subList.length; j++) {
            promiseArray.push(new Promise((resolve, reject) => {
                resolve(pageScrape(subList[j], browser));
            }));
        }
        Promise.all(promiseArray)
            .then(response => {
                procArray.concat(response);
            });
        promiseArray = new Array();

        try {
            await browser.close();
        } catch(ex){
            console.log(ex);
        }
    };
}

async function pageScrape(url, browser) {
    let page = await browser.newPage();
    await page.goto(url, {
        timeout: 0
    });
    await page.waitFor(1000);
    return await page.evaluate(() => {
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
    }).then(response => {
        let urlData = {
            id: subList[j],
            appName: response.appTitle,
            companyName: response.companyName,
            dateListed: response.dateListed,
            category: response.category
        }
        return urlData;
    });
};