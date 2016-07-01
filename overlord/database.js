var mongo = require('mongodb').MongoClient;
var constants = require('./constants.json');

/**
 * Module name
 */
var MODULE_NAME = "DB";

var mod = {
   MODULE_NAME: MODULE_NAME,
   db: null
}

/**
 * Connect to database
 */
console.log("["+MODULE_NAME+"] Connecting to MongoDB: " + constants.MONGO_URL);
mongo.connect(constants.MONGO_URL, function(error, database){
   if(error){
      console.error("["+MODULE_NAME+"] Failed to connect to MongoDB, error: ", error);
   } else {
      console.log("["+MODULE_NAME+"] Successfull connection to DB");
      mod.db = database;
   }
});

module.exports = mod;
