var mongo = require('mongodb').MongoClient;
var zerorpc = require('zerorpc');
var constants = require('./constants.json');
var database = require('./database.js');

/**
 * Module name
 */
var MODULE_NAME = "IM";

/**
 * Module vars
 */
var minionClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});

/**
 * Initialize minions
 */
console.log("["+MODULE_NAME+"] Connecting to minion in: " + constants.MINION_URL);
minionClient.connect(constants.MINION_URL);

/**
 * Retrieves available sizes
 * @param {String} - OPTIONAL - Sizes for this minion
 */
var getAvailableSizes = function(minion, getCallback){
   var sizes = [];
   getCallback(null, sizes);
}

/**
 * Retrieves a dedicated instance.
 * It could be an existing one or a newly instanced one
 * @param {String} - Size ID.
 * @param {String} - Image ID.
 */
var getDedicatedInstance = function(size_id, image_id, getCallback){
   var instance = null;
   getCallback(null, instance);
}

/**
 * Get image metadata
 * @param {String} - Image ID
 */
var getImage = function(image_id, getCallback){
   database.db.collection('images').findOne({_id: image_id}, function(error, image){
      if(error){
         getCallback(new Error('Image with ID "'+image_id+'" does not exists'));
      } else {
         getCallback(null, image);
      }
   });
}

/**
 * Get size metadata
 * @param {String} - Size ID
 */
var getSize = function(image_id, getCallback){
   database.db.collection('sizes').findOne({_id: size_id}, function(error, size){
      if(error){
         getCallback(new Error('Size with ID "'+size_id+'" does not exists'));
      } else {
         getCallback(null, size);
      }
   });
}


exports.getImage = getImage;
exports.getSize = getSize;
