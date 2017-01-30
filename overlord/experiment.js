var zerorpc = require('zerorpc');
var async = require('async');

var utils = require('./utils.js');
var logger = utils.logger;
var apps = require('./application.js');
var database = require('./database.js');
var storage = require('./storage.js');
var usermanager = require('./users.js');

/**
 * Module name
 */
var MODULE_NAME = "EP";

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
         _id: 0,
         input_tree: 0,
         src_tree: 0,
         output_tree: 0,
         logs: 0
      };
   } else {
      // Required values
      fields._id = 0;
      fields.id = 1;
      fields.name = 1;
      fields.app_id = 1;
   }

   // Retrieve experiment metadata
   database.db.collection('experiments').findOne({id: exp_id}, fields, function(error, exp){
      if(error){
         getCallback(new Error("Query for experiment " + exp_id + " failed"));
      } else if (!exp){
         getCallback(new Error("Experiment " + exp_id + " not found"));
      } else {
         // Fill missing fields (backward compatibility)
         if(fields.input_tree && !exp.input_tree) exp.input_tree = [];
         if(fields.src_tree && !exp.src_tree) exp.src_tree = [];
         if(fields.output_tree && !exp.output_tree) exp.output_tree = [];

         // Return
         getCallback(null, exp);
      }
   });
}

/**
 * Search for an experiment
 */
var searchExperiments = function(fields, searchCallback){
   // Connected to DB?
   if(database.db == null){
      searchCallback(new Error("Not connected to DB"));
      return;
   }

   // General query
   var query = {};

   // Filter name
   if(fields.name){
      query['name'] = {$regex: ".*"+name+".*"};
   }

   // Filter owner
   if(fields.owner){
      query['owner'] = fields.owner;
   }

   // Projection
   var projection = {
         _id: 0,
         id: 1,
         name: 1,
         desc: 1,
         app_id: 1,
         labels: 1,
         last_execution: 1
   };

   // Retrieve experiment metadata
   database.db.collection('experiments').find(query, projection).toArray(function(error, exps){
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
   if(!'owner' in exp_cfg){
      createCallback(new Error("Error creating experiment, 'owner' not set."));
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
               exp_cfg.app = app;
               wfcb(null);
            }
         });
      },
      // Check if owner exists
      function(wfcb){
         usermanager.getUser(exp_cfg.owner, function(error, user){
            if(error){
               wfcb(error);
            } else {
               exp_cfg.owner = user.id;
               wfcb(null);
            }
         });
      },
      // Copy experiment to storage from application
      function(wfcb){
         // Create UUID
         exp_cfg.id = utils.generateUUID();
         storage.client.invoke('copyExperiment', exp_cfg.id, exp_cfg.app_id, function (error) {
            if(error) return wfcb(error);
            return wfcb(null);
         });
      },
      // Setup metadata
      function(wfcb){
         storage.client.invoke('discoverMetadata', exp_cfg.app_id, exp_cfg.id, function(error, metadata){
            if(error) return wfcb(error);

            // Get logs meta
            exp_cfg.logs_meta = metadata.logs_meta;

            // Specified labels?
            if(!exp_cfg.labels && exp_cfg.app.labels){
               // Copy labels to experiment
               exp_cfg.labels = exp_cfg.app.labels;
               // Iterate labels and set current value
               for(var label_key in exp_cfg.labels){
                  // Get default value and set if exists
                  if(exp_cfg.labels[label_key].default_value){
                     exp_cfg.labels[label_key].value = exp_cfg.labels[label_key].default_value;
                  }
               }
            }
            return wfcb(null);
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
            owner: exp_cfg.owner,
            desc: ('desc' in exp_cfg) ? exp_cfg.desc : "Description...",
            app_id: exp_cfg.app_id,
            input_tree: exp_cfg.input_tree,
            src_tree: exp_cfg.src_tree,
            last_execution: null,
            times_executed: 0,
            labels: ('labels' in exp_cfg) ? exp_cfg.labels : {},
            logs_meta: ('logs_meta' in exp_cfg) ? exp_cfg.logs_meta : {}
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

      // Update DB
      database.db.collection('experiments').updateOne({id: exp_id},{$set: new_exp});
      updateCallback(null);
   });
}

/**
 * Execute an operation over an experiment
 */
var maintainExperiment = function(exp_id, operation, maintainCallback){
   // Check parameters
   if(!exp_id) return maintainCallback(new Error("Experiment ID not set."));

   // Do tasks
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Apply operation
      function(exp, wfcb){
         if(operation == 'discoverMetadata'){
            // Get labels list
            storage.client.invoke('discoverMetadata', exp.app_id, exp_id, function(error, metadata){
               if(error) return wfcb(error);

               // Get meta
               var labels = metadata.labels;
               var logs_meta = metadata.logs_meta;

               // Iterate current experiment labels
               for(var label in exp.labels){
                  // Set current value
                  if(labels[label] && exp.labels[label].value){
                     labels[label].value = exp.labels[label].value;
                  }
               }

               // Update labels in DB
               database.db.collection('experiments').update({id: exp_id}, {$set: {labels: labels, logs_meta: logs_meta}}, function(error){
                  if(error) return wfcb(error);
                  // Success updating labels
                  return wfcb(null, exp);
               });
            });
         } else {
            return wfcb(new Error("Unknown operation: "+operation));
         }
      }
   ],
   function(error, exp){
      if(error) return maintainCallback(error);

      // Success
      logger.debug('['+MODULE_NAME+']['+exp_id+'] Operation "'+operation+'" success.');
      return maintainCallback(null);
   });
}

exports.getExperiment = getExperiment;
exports.searchExperiments = searchExperiments;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
exports.maintainExperiment = maintainExperiment;
