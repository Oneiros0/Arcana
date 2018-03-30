const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() + '_' + d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var correctxp = "//div//ul[@id='appx-table-results']//li";
var listSize = 0;
var gauge = 0;
var newSize = 0;

console.log(__dirname + '/../rawdata/' + dateString + '.txt');

(async () => {
  const tilexp = '.appx-tile.appx-tile-app.tile-link-click';
  const url = 'https://appexchange.salesforce.com/appxStore?type=App';
  let browser = await puppeteer.launch({
    headless: true
  });
  let page = await browser.newPage();
  await page.goto(url);
  await page.waitForSelector(tilexp);

  await page.evaluate(() => {
    window.onCompleteFunction = function() {
      console.log("onComplete is RUNNING!!!");
      if (true) {
        var tableAjax = document.getElementById('ajax-result');

        var elems = tableAjax.children;
        var tbody = document.getElementById('appx-table-results');
        for (var i = 0; i < tableAjax.children.length; i++) {
          tbody.appendChild(tableAjax.children[i].cloneNode(true));
        }
        tableAjax.innerHTML = '';
      } else {
        var table = document.getElementById('new-rows-table');
        var rows = table.rows;
        var tbody = document.getElementById('appx-results-table-body');

        table.querySelectorAll('tr').forEach(function (element) {
          tbody.appendChild(element.cloneNode(true));
          element.remove();
        });
      }
      AppxTile.init();
      disableLoadMore();
      AppxFavorites.hideSpinnerButton();
      Appx.enableButton('appx-load-more-button-id');
      loadMoreListingsJS()
    }
    loadMoreListingsJS();
  });

  // await page.evaluate(() => {
  //   console.log("Trigger loadMoreListingJS();");
  //   loadMoreListingsJS();
  // });

  await page.waitForFunction(() => {
    return document.querySelectorAll("div ul#appx-table-results li a").length >= 2000;
  }, {
    'timeout': 1600000
  });


  //Takes the entire list of available tiles
  //and passes the HREF's into a collection.
  const hrefcol = await page.$$eval("div ul#appx-table-results li a", links => {
    console.log(typeof links);

    //console.log('Length :: ', linkNums);    console.log(links);
    let linkArray = Array.from(links);
    console.log(linkArray.length);

    //  loadMoreListingsJS();

    return linkArray.map(link => link.href);
  });

  fs.writeFile(__dirname + '/../rawdata/' + dateString + '.txt', JSON.stringify(hrefcol), (err) => {
    if (err) {
      return console.log(err)
    }
    console.log('URL List dumped and created!');
    process.exit();
  });

  //console.log('Length :: ', linkNums);
  console.log(hrefcol.length);
  console.log('Type for Linkcol :: ', typeof hrefcol);
  console.log(JSON.stringify(hrefcol));
  //await browser.close();
})();

function delay(timeout) {
  return new Promise((resolve) => {
    console.log("Totes Delaying");
    setTimeout(resolve, timeout);
  });
}