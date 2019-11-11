const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var urlArray = [];
var procArray = new Array();
let results = new Array();
let promiseArray = new Array();
let anchor = 'div.appx-write-review-text div';

var readfiledirectory = __dirname + '/../rawdata/' + dateString + '.txt';
var writefiledirectory = __dirname + '/../dynamic/' + dateString + '_dynamic.json'

let ws = fs.createWriteStream(writefiledirectory);

bundle(readfiledirectory);

function bundle(readfiledirectory) {
    createArray(readfiledirectory)
        .then(async data => {
            console.log("DATA LENGTH : " + data.length);
            subData = data.splice(0, data.length);
            processBatch(subData, 10, procArray).then((processed) => {
                for (let i = 0; i < procArray.length; i++) {
                    for (let j = 0; j < procArray[i].length; j++) {
                        results.push(procArray[i][j]);
                    }
                }
                console.log(results);
                ws.write(JSON.stringify(results));
            });
            console.log("After Promise All", );
        })
}

function processBatch(masterList, batchSize, procArray) {
    return Promise.all(masterList.splice(0, batchSize).map(async url => {
        return singleScrape(url)
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
    let bbyid = url.substring(url.indexOf('=') + 1);
    let browser = await puppeteer.launch({
        headless: true
    });
    let page = await browser.newPage();
    await page.goto(url, {
        timeout: 0
    });

    await page.waitFor(1000);

    let result1 = await page.evaluate(() => {
        let appTitle = document.querySelector('.appx-page-header-2_title');
        appTitle = appTitle ? appTitle.innerText : '';

        let companyName = document.querySelector('.appx-company-name');
        companyName = companyName ? companyName.innerText : '';

        let lastUpdate = document.querySelector('.appx-detail-section-last-update p:nth-child(2)');
        lastUpdate = lastUpdate ? lastUpdate.innerText : '';
        return {
            appTitle,
            companyName,
            lastUpdate,
        }
    });

    await Promise.all([
        // page.evaluate(() => {
        //     var button = document.getElementById('tab-default-2__item');
        // }),

        page.click("a[id=tab-default-2__item]"),
        page.waitFor(7000)
        // page.waitForSelector("div.appx-write-review-text div")
    ]);

    let result2 = await page.evaluate(() => {
        let reviewPercentage = document.querySelectorAll('.appx-extended-detail-review-percentage');
        console.log("After the review percentages.")

        let percentages = {
            five: '',
            four: '',
            three: '',
            two: '',
            one: ''
        }

        let price = document.querySelector('.appx-pricing-detail-header');
        price = price ? price.innerText : '';

        let reviewscount_5 = document.getElementsByClassName("appx-extended-detail-review-bar appx-review-bar-graph-horizontal-5");
        let reviewscount_4 = document.getElementsByClassName("appx-extended-detail-review-bar appx-review-bar-graph-horizontal-4");
        let reviewscount_3 = document.getElementsByClassName("appx-extended-detail-review-bar appx-review-bar-graph-horizontal-3");
        let reviewscount_2 = document.getElementsByClassName("appx-extended-detail-review-bar appx-review-bar-graph-horizontal-2");
        let reviewscount_1 = document.getElementsByClassName("appx-extended-detail-review-bar appx-review-bar-graph-horizontal-1");

        let actuals = {
            five: '',
            four: '',
            three: '',
            two: '',
            one: ''
        }

        percentages.five = reviewPercentage[0] ? reviewPercentage[0].innerText : '';
        percentages.four = reviewPercentage[1] ? reviewPercentage[1].innerText : '';
        percentages.three = reviewPercentage[2] ? reviewPercentage[2].innerText : '';
        percentages.two = reviewPercentage[3] ? reviewPercentage[3].innerText : '';
        percentages.one = reviewPercentage[4] ? reviewPercentage[4].innerText : '';

        actuals.five = reviewscount_5[0] ? reviewscount_5[0].dataset.reviewCount : '';
        actuals.four = reviewscount_4[0] ? reviewscount_4[0].dataset.reviewCount : '';
        actuals.three = reviewscount_3[0] ? reviewscount_3[0].dataset.reviewCount : '';
        actuals.two = reviewscount_2[0] ? reviewscount_2[0].dataset.reviewCount : '';
        actuals.one = reviewscount_1[0] ? reviewscount_1[0].dataset.reviewCount : '';

        let totalReviews = reviewscount_5[0] ? reviewscount_5[0].dataset.reviewTotal : '';

        let overallRating = document.querySelector('.appx-average-rating-numeral');
        overallRating = overallRating ? overallRating.innerText : '';

        return {
            price,
            percentages,
            actuals,
            totalReviews,
            overallRating
        }
    });

    let urlData = {
        id: bbyid,
        appName: result1.appTitle,
        companyName: result1.companyName,
        lastUpdate: result1.lastUpdate,
        price: result2.price,
        percentages: result2.percentages,
        actuals: result2.actuals,
        totalReviews: result2.totalReviews,
        overallRating: result2.overallRating,
        date: dateString
    }
    await browser.close();
    return urlData;
}

function createArray(readfiledirectory) {
    return new Promise((resolve, reject) => {
        fs.readFile(readfiledirectory, 'utf8', function (err, contents) {
            if (err) {
                reject(err)
            } else {
                resolve(contents.replace(/"/g, '').replace('[', '').replace(']', '').split(','))
            }

        })
    })
}