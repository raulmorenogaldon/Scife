/**
 * Copyright 2017 University of Castilla - La Mancha
 */
var mongo = require('mongodb').MongoClient;
var logger = require('./utils.js').logger;

/**
 * Module name
 */
var MODULE_NAME = "DB";
var constants = {};

var mod = {
   MODULE_NAME: MODULE_NAME,
   db: null
}

/**
 * Connect to database
 */
var init = function(cfg, initCallback){
   // Set constants
   constants = cfg;

   logger.info('['+MODULE_NAME+'] Connecting to MongoDB: ' + constants.MONGO_URL);
   mongo.connect(constants.MONGO_URL, function(error, database){
      if(error){
         logger.error('['+MODULE_NAME+'] Failed to connect to MongoDB.');
         initCallback(error);
      } else {
         logger.info('['+MODULE_NAME+'] Successfull connection to DB');
         mod.db = database;
         initCallback(null);
      }
   });
}

module.exports = mod;
module.exports.init = init;
