var mongo = require('mongodb').MongoClient;
var zerorpc = require('zerorpc');
var async = require('async');
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
 * Get instance metadata
 * @param {String} - Instance ID
 */
var getInstance = function(inst_id, getCallback){
   database.db.collection('instances').findOne({_id: inst_id}, function(error, inst){
      if(error || inst == undefined){
         getCallback(new Error('Instance with ID "'+inst_id+'" does not exists'));
      } else {
         getCallback(null, inst);
      }
   });
}

/**
 * Get image metadata
 * @param {String} - Image ID
 */
var getImage = function(image_id, getCallback){
   database.db.collection('images').findOne({_id: image_id}, function(error, image){
      if(error || image == undefined){
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
var getSize = function(size_id, getCallback){
   database.db.collection('sizes').findOne({_id: size_id}, function(error, size){
      if(error || size == undefined){
         getCallback(new Error('Size with ID "'+size_id+'" does not exists'));
      } else {
         getCallback(null, size);
      }
   });
}

/**
 * Get images list
 */
var getImagesList = function(getCallback){
   minionClient.invoke('getImages', null, function (error, images) {
      if(error){
         getCallback(new Error('Failed to retrieve images'));
      } else {
         getCallback(null, images);
      }
   });
}

/**
 * Get sizes list
 */
var getSizesList = function(getCallback){
   minionClient.invoke('getSizes', null, function (error, sizes) {
      if(error){
         getCallback(new Error('Failed to retrieve sizes'));
      } else {
         getCallback(null, sizes);
      }
   });
}

/**
 * Get instances list
 */
var getInstancesList = function(getCallback){
   minionClient.invoke('getInstances', null, function (error, insts) {
      if(error){
         getCallback(new Error('Failed to retrieve instances'));
      } else {
         getCallback(null, insts);
      }
   });
}

/**
 * Retrieve instance
 */
var requestInstance = function(image_id, size_id, requestCallback){
   // TODO: Select a minion
   var minion = minionClient;

   // Instance
   minion.invoke('createInstance', {
      name:"Unnamed",
      image_id: image_id,
      size_id: size_id
   }, function (error, instance_id) {
      if(error){
         requestCallback(new Error("Failed to create instance, err: " + error));
      } else {
         requestCallback(null, instance_id);
      }
   });
}

/**
 * Destroy instance
 */
var destroyInstance = function(inst_id, destroyCallback){
   // Destroy instance
   minionRPC.invoke('destroyInstance', inst_id, function (error) {
      if(error){
         destroyCallback(new Error("Failed to destroy instance " + inst_id + ", err: " + error));
      } else {
         destroyCallback(null);
      }
   });
}

/**
 * Remove experiment from instance
 */
var cleanExperiment = function(exp_id, inst_id, b_job, b_code, b_input, b_remove, cleanCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Clean job
      function(inst, minion, wfcb){
         if(b_remove || b_job){
            _cleanExperimentJob(minion, exp_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null, inst, minion);
            });
         } else {
            // Update database
            var query = {};
            query['exps.'+exp_id+'.job'] = "";
            database.db.collection('instances').updateOne({id: inst_id},{$unset:query});

            // Callback
            wfcb(null, inst, minion);
         }
      },
      // Clean code
      function(inst, minion, wfcb){
         if(b_remove || b_code){
            _cleanExperimentCode(minion, exp_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null, inst, minion);
            });
         } else {
            wfcb(null, inst, minion);
         }
      },
      // Clean input
      function(inst, minion, wfcb){
         if(b_remove || b_input){
            _cleanExperimentInput(minion, exp_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      }
   ],
   function(error){
      if(error){
         cleanCallback(error);
         return;
      }
      // Update database if removal
      if(b_remove){
         var query = {};
         query['exps.'+exp_id] = "";
         database.db.collection('instances').updateOne({id: inst_id},{$unset:query});
      }
      // Callback
      cleanCallback(null);
   });
}

/**
 * Execute a command in an instance and wait for return
 */
var executeCommand = function(inst_id, cmd, executeCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeCommand', cmd, inst_id, function (error, result) {
            if (error) {
               wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
            } else {
               wfcb(null, result);
            }
         });
      }
   ],
   function(error, result){
      if(error){
         executeCallback(error);
         return;
      }
      // Callback
      executeCallback(null, result);
   });
}

/**
 * Execute a command in an instance but as a job
 */
var executeJob = function(inst_id, exp_id, cmd, work_dir, nodes, executeCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeScript', cmd, work_dir, inst_id, nodes, function (error, job_id, more) {
            if (error) {
               wfcb(new Error("Failed to execute job command:\n", cmd, "\nError: ", error));
            } else {
               // Add job to instance
               var query = {};
               query['exps.'+exp_id+'.job'] = job_id;
               database.db.collection('instances').updateOne({id: inst_id},{$set:query});
               wfcb(null, job_id);
            }
         });
      }
   ],
   function(error, job_id){
      if(error){
         executeCallback(error);
         return;
      }
      // Callback
      executeCallback(null, job_id);
   });
}

/**
 * Clean experiment Job in instance
 */
var _cleanExperimentJob = function(minion, exp_id, inst, cleanCallback){
   // Exists a job for this experiment?
   if(inst.exps && inst.exps[exp_id] && inst.exps[exp_id].job){
      var job_id = inst.exps[exp_id].job;
      minion.invoke('cleanJob', job_id, inst.id, function (error, result, more) {
         if (error) {
            cleanCallback(new Error('Failed to clean job: "', job_id, '", error: ', error));
         } else {
            cleanCallback(null);
         }
      });
   } else {
      // Skip step
      cleanCallback(null);
   }
}

/**
 * Clean experiment code in instance
 */
var _cleanExperimentCode = function(minion, exp_id, inst, cleanCallback){
   var work_dir = inst.workpath+"/"+exp_id;
   var cmd = 'rm -rf '+work_dir;
   // Execute command
   minion.invoke('executeCommand', cmd, inst.id, function (error, result, more) {
      if (error) {
         cleanCallback(new Error("Failed to clean experiment code, error: ", error));
      } else {
         cleanCallback(null);
      }
   });
}

/**
 * Clean experiment input data in instance
 */
var _cleanExperimentInput = function(minion, exp_id, inst, cleanCallback){
   // Remove experiment code folder
   var input_dir = inst.inputpath+"/"+exp.id;
   var cmd = 'rm -rf '+input_dir;
   // Execute command
   minion.invoke('executeCommand', cmd, inst.id, function (error, result, more) {
      if (error) {
         cleanCallback(new Error("Failed to clean experiment input data, error: ", error));
      } else {
         cleanCallback(null);
      }
   });
}



module.exports.getInstance = getInstance;
module.exports.getImage = getImage;
module.exports.getSize = getSize;

module.exports.getImagesList = getImagesList;
module.exports.getSizesList = getSizesList;
module.exports.getInstancesList = getInstancesList;

module.exports.requestInstance = requestInstance;
module.exports.destroyInstance = destroyInstance;

module.exports.executeCommand = executeCommand;
module.exports.executeJob = executeJob;

module.exports.cleanExperiment = cleanExperiment;
