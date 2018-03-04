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

//Asynchronous Function
fs.readFile(__dirname + '/rawdata/' + dateString + '.tsx', 'utf8', function (err, contents) {
    urlArray = contents.replace(/"/g, '').replace('[', '').replace(']', '').split(',');
    console.log(urlArray.length);
    procArray = urlArray;
    let obj = procArray[0];
    console.log(obj);
})

let scrape = async () => {
    const browser = await puppeteer.launch({headless: false});
    const page = await browser.newPage();

    await page.goto();
    await page.click('#default > div > div > div > div > section > div:nth-child(2) > ol > li:nth-child(1) > article > div.image_container > a > img');
    await page.waitFor(1000);

    const result = await page.evaluate(() => {
        let appTitle = document.querySelector('.appx-page-header-2_title').innerText;
        let companyName = document.querySelector('.appx-company-name').innerText;
        let dateListed = document.evaluate("(//div[@class='appx-detail-section-first-listed']//p)[2]", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.innerText;
        let category = document.evaluate("//div[@class='appx-detail-section appx-headline-details-categories']//a//strong", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.innerText;
        
        return {
            title,
            price
        }

    });

    browser.close();
    return result;
};

// scrape().then((value) => {
//     console.log(value); // Success!
// });