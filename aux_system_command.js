const execSync = require('child_process').execSync;

var cmd = execSync(process.argv.slice(2).join());
