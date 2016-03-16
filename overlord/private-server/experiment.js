var zerorpc = require('zerorpc');
var async = require('async');
var mongo = require('mongodb').MongoClient;
var constants = require('./constants.json');
var utils = require('./utils.js');
var apps = require('./application.js');

var storageClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});
storageClient.connect(constants.STORAGE_URL);

// Connect to DB
console.log('Conecting to MongoDB: ' + constants.MONGO_URL);
var db = null;
mongo.connect(constants.MONGO_URL, function(error, database){
   if(error){
      console.error("Failed to connect to MongoDB, error: ", error);
   } else {
      console.log("Successfull connection to DB");
      db = database;
   }
});

/**
 * Get experiment data
 */
var getExperiment = function(exp_id, getCallback){
   // Connected to DB?
   if(db == null){
      getCallback(new Error("Not connected to DB"));
      return;
   }

   // Retrieve experiment metadata
   db.collection('experiments').findOne({id: exp_id}, function(error, exp){
      if(error){
         getCallback(new Error("Query for experiment " + exp_id + " failed"));
      } else if (!exp){
         getCallback(new Error("Experiment " + exp_id + " not found"));
      } else {
         getCallback(null, exp);
      }
   });
}

/**
 * Search for an experiment
 */
var searchExperiments = function(name, searchCallback){
   // Connected to DB?
   if(db == null){
      searchCallback(new Error("Not connected to DB"));
      return;
   }

   // Set query
   var query;
   if(!name){
      query = ".*";
   } else {
      query = ".*"+name+".*";
   }

   // Retrieve experiment metadata
   db.collection('experiments').find({name: {$regex: query}}).toArray(function(error, exps){
      if(error){
         searchCallback(new Error("Query for experiments with name: " + name + " failed"));
      } else {
         searchCallback(null, exps);
      }
   });
}

/**
 * Create an experiment and insert it into DB
 */
var createExperiment = function(exp_cfg, createCallback){
   // Check parameters
   if(!'name' in exp_cfg){
      createCallback(new Error("Error creating experiment, 'name' not set."));
      return;
   }
   if(!'app_id' in exp_cfg){
      createCallback(new Error("Error creating experiment, 'app_id' not set."));
      return;
   }

   // Do tasks
   async.waterfall([
      // Check if experiment name exists
      function(wfcb){
         db.collection('experiments').findOne({name: exp_cfg.name}, function(error, exp){
            console.log("db")
            if(error){
               wfcb(new Error("Query for experiment name " + exp_cfg.name + " failed"));
            } else if (exp){
               wfcb(new Error("Experiment with name '" + exp_cfg.name + "' already exists"));
            } else {
               wfcb(null);
            }
         });
      },
      // Check if application exists
      function(wfcb){
         apps.getApplication(exp_cfg.app_id, function(error, app){
            if(error){
               wfcb(error);
            } else {
               wfcb(null);
            }
         });
      },
      function(wfcb){
         // Create UUID
         exp_cfg.id = utils.generateUUID();

         // Copy experiment to storage from application
         storageClient.invoke('copyExperiment', exp_cfg.id, exp_cfg.app_id, function (error) {
            if(error){
               wfcb(error);
            } else {
               wfcb(null);
            }
         });
      },
      function(wfcb){
         //Create experiment data
         exp = {
            _id: exp_cfg.id,
            id: exp_cfg.id,
            name: exp_cfg.name,
            desc: ('desc' in exp_cfg) ? exp_cfg.desc : "Description...",
            status: "created",
            app_id: exp_cfg.app_id,
            labels: ('labels' in exp_cfg) ? exp_cfg.labels : {},
            exec_env: ('exec_env' in exp_cfg) ? exp_cfg.exec_env : {}
         };

         // Add experiment to DB
         db.collection('experiments').insert(exp, function(error){
            if(error){
               wfcb(error);
            } else {
               // Success adding experiment
               wfcb(null, exp);
            }
         });
      }
   ],
   function(error, exp){
      if(error){
         console.log("Error creating experiment with config: " + JSON.stringify(exp_cfg));
         createCallback(error);
      }

      // Return experiment data
      console.log("Created experiment: " + JSON.stringify(exp));
      createCallback(null, exp);
   });
}

/**
 * Update experiment data
 */
var updateExperiment = function(exp_id, exp_cfg, updateCallback){
   // Get experiment
   getExperiment(exp_id, function(error, exp){
      if(error){
         updateCallback(error);
      }
      // Get valid parameters
      var new_exp = {};
      if('name' in exp_cfg) new_exp.name = exp_cfg.name;
      if('desc' in exp_cfg) new_exp.desc = exp_cfg.desc;
      if('labels' in exp_cfg) new_exp.labels = exp_cfg.labels;
      if('exec_env' in exp_cfg) new_exp.exec_env = exp_cfg.exec_env;

      // Update DB
      db.collection('experiments').updateOne({id: exp_id},{$set: new_exp});
      updateCallback(null);
   });
}

exports.getExperiment = getExperiment;
exports.searchExperiments = searchExperiments;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
