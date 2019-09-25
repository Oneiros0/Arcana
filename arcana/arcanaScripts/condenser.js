var fs = require('fs');
var readfiledirectory = __dirname + '/../dynamic/6_8_2019_dynamic.json';

function fsReadFileSyncToArray(readfiledirectory) {
    var data = JSON.parse(fs.readFileSync(readfiledirectory));
    console.log(data);
    return data;
}

fsReadFileSyncToArray();
