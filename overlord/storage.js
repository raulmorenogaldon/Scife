var zerorpc = require('zerorpc');
var constants = require('./constants.json');

/**
 * Module name
 */
var MODULE_NAME = "SG";

/**
 * RPC client
 */
var client = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});

/**
 * Connect to storage
 */
console.log("["+MODULE_NAME+"] Connecting to storage RPC in: " + constants.STORAGE_URL);
client.connect(constants.STORAGE_URL);

/**
 * Handle error in connect step
 */
client.on("error", function(error){
   console.error("["+MODULE_NAME+"] Failed to connect to storage RPC, error: ", error);
})

exports.client = client;
exports.MODULE_NAME = MODULE_NAME;
