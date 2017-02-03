/**
 * Copyright 2017 University of Castilla - La Mancha
 */
var zerorpc = require('zerorpc');
var logger = require('./utils.js').logger;

/**
 * Module name
 */
var MODULE_NAME = "ST";
var constants = {};

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
var init = function(cfg, initCallback){
   // Set constants
   constants = cfg;

   // Handle error in connect step
   client.on("error", function(error){
      logger.error('['+MODULE_NAME+'] Failed to connect to storage RPC, error: ', error);
   })

   logger.info('['+MODULE_NAME+'] Connecting to storage RPC in: ' + constants.STORAGE_URL);
   client.connect(constants.STORAGE_URL);
   initCallback(null);
}

exports.init = init;
exports.client = client;
exports.MODULE_NAME = MODULE_NAME;
