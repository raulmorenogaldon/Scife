var spawn = require('child_process').spawn;
var psTree = require('ps-tree');

// Kill code
var kill = function(pid, signal, callback){
	signal = signal || 'SIGKILL';
	callback = callback || function(){};
	var killTree = true;
	if(killTree) {
		psTree(pid, function(err, children) {
			[pid].concat(
				children.map(function(p){
					return p.PID;
				})
			).forEach(function (tpid) {
				try { process.kill(tpid, signal)}
				catch(ex) { }
			});
			callback();
		});
	} else {
		try { process.kill(pid, signal) }
		catch (ex) {}
		callback();
	}
};

console.log("Spawning: "+process.argv[2]+" "+process.argv.slice(3));

// Execute command
var proc = spawn(process.argv[2], process.argv.slice(3));

// Print back data
proc.stdout.on("data", function(data){
	process.stdout.write(data);
});

// Print back data
proc.stderr.on("data", function(data){
	process.stderr.write(data);
});

// Handle error
proc.on('error', function(err){
	console.log("ERROR: "+err);
	kill(proc.pid);
});
