const spawn = require('child_process').spawn;

var proc = spawn(process.argv.slice(2).join());
