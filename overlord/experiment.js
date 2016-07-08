var zerorpc = require('zerorpc');
var async = require('async');

var constants = require('./constants.json');
var utils = require('./utils.js');
var apps = require('./application.js');
var database = require('./database.js');
var storage = require('./storage.js');


/**
 * Get experiment data
 */
var getExperiment = function(exp_id, fields, getCallback){
   // Connected to DB?
   if(database.db == null){
      getCallback(new Error("Not connected to DB"));
      return;
   }

   // Check experiment arg
   if(!exp_id){
      getCallback(new Error("Incorrect parameters in getExperiment, no Experiment ID has been passed"));
      return;
   }

   // Parse fields
   if(!fields){
      fields = {
         input_tree: 0,
         src_tree: 0,
         logs: 0
      };
   } else {
      // Required values
      fields.id = 1;
      fields.name = 1;
      fields.app_id = 1;
      fields.status = 1;
   }

   // Retrieve experiment metadata
   database.db.collection('experiments').findOne({id: exp_id}, fields, function(error, exp){
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
   if(database.db == null){
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

   // Projection
   var fields = {
         input_tree: 0,
         src_tree: 0,
         logs: 0
   };

   // Retrieve experiment metadata
   database.db.collection('experiments').find({name: {$regex: query}}, fields).toArray(function(error, exps){
      if(error) return searchCallback(new Error("Query for experiments with name: " + name + " failed"));
      searchCallback(null, exps);
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
         database.db.collection('experiments').findOne({name: exp_cfg.name}, function(error, exp){
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
      // Copy experiment to storage from application
      function(wfcb){
         // Create UUID
         exp_cfg.id = utils.generateUUID();

         storage.client.invoke('copyExperiment', exp_cfg.id, exp_cfg.app_id, function (error) {
            if(error){
               wfcb(error);
            } else {
               wfcb(null);
            }
         });
      },
      // Obtain experiment input data tree
      function(wfcb){
         storage.client.invoke('getInputFolderTree', exp_cfg.id, function (error, tree) {
            if(error){
               wfcb(error);
            } else {
               exp_cfg.input_tree = tree;
               wfcb(null);
            }
         });
      },
      // Obtain experiment source code tree
      function(wfcb){
         storage.client.invoke('getExperimentSrcFolderTree', exp_cfg.id, exp_cfg.app_id, function (error, tree) {
            if(error){
               wfcb(error);
            } else {
               exp_cfg.src_tree = tree;
               wfcb(null);
            }
         });
      },
      //Create experiment data
      function(wfcb){
         exp = {
            _id: exp_cfg.id,
            id: exp_cfg.id,
            name: exp_cfg.name,
            desc: ('desc' in exp_cfg) ? exp_cfg.desc : "Description...",
            status: "created",
            app_id: exp_cfg.app_id,
            input_tree: exp_cfg.input_tree,
            src_tree: exp_cfg.src_tree,
            inst_id: null,
            labels: ('labels' in exp_cfg) ? exp_cfg.labels : {},
            exec_env: ('exec_env' in exp_cfg) ? exp_cfg.exec_env : {}
         };

         // Add experiment to DB
         database.db.collection('experiments').insert(exp, function(error){
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
      if(error) return createCallback(error);
      createCallback(null, exp);
   });
}

/**
 * Update experiment data
 */
var updateExperiment = function(exp_id, exp_cfg, updateCallback){
   // Get experiment
   getExperiment(exp_id, function(error, exp){
      if(error) return updateCallback(error);

      // Get valid parameters
      var new_exp = {};
      if('name' in exp_cfg) new_exp.name = exp_cfg.name;
      if('desc' in exp_cfg) new_exp.desc = exp_cfg.desc;
      if('labels' in exp_cfg) new_exp.labels = exp_cfg.labels;
      if('exec_env' in exp_cfg) new_exp.exec_env = exp_cfg.exec_env;

      // Update DB
      database.db.collection('experiments').updateOne({id: exp_id},{$set: new_exp});
      updateCallback(null);
   });
}

exports.getExperiment = getExperiment;
exports.searchExperiments = searchExperiments;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
