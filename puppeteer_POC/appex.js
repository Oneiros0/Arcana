const puppeteer = require('puppeteer');
const devices = require('puppeteer/DeviceDescriptors');

var d1 = new Date();
var dateString = (d1.getMonth() +'_'+ d1.getDate() + '_' + d1.getFullYear());
var fs = require('fs');
var correctxp = "//div//ul[@id='appx-table-results']//li";

(async() => {
  const tilexp = '.appx-tile.appx-tile-app.tile-link-click';
  const url = 'https://appexchange.salesforce.com/appxStore?type=App';
  let browser = await puppeteer.launch({headless: true});
  let page = await browser.newPage();
  await page.goto(url);
  await page.waitForSelector(tilexp);

  for(let i = 0; i < 275; i++){
    console.log("Iteration : " + i);
    await page.evaluate(() => {
        loadMoreListingsJS();
    });  
  }

   await page.waitForFunction(() =>{
     return document.querySelectorAll("div ul#appx-table-results li a").length >= 3000;
   },{'timeout':1600000}); 
  

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
  
  fs.writeFile(__dirname + '/rawdata/'+ dateString + '.tsx', JSON.stringify(hrefcol), (err) => {
    if (err) {return console.log(err)}
    console.log( 'URL List dumped and created!');
  });
  
  //console.log('Length :: ', linkNums);
  console.log(hrefcol.length);
  console.log('Type for Linkcol :: ', typeof hrefcol);
  console.log(JSON.stringify(hrefcol));
  //await browser.close();
})();