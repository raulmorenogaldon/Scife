var zerorpc = require('zerorpc');
var async = require('async');

/**
 * Load submodules
 */
var utils = require('./utils.js');
var logger = utils.logger;
var database = require('./database.js');
var storage = require('./storage.js');
var instmanager = require('./instance.js');

/**
 * Module name
 */
var MODULE_NAME = "EX";

/**
 * Module vars
 */
var constants = null;

var createExecution = function(exp_id, name, parent_id, launch_opts, labels, cb){
   // Connected to DB?
   if(database.db == null) return cb(new Error("Not connected to DB"));

   // Create UUID
   var exec_id = utils.generateUUID();

   // Get current date
   var curr_date = new Date();

   // Create execution data
   var exec = {
      _id: exec_id,
      id: exec_id,
      parent_id: parent_id,
      name: name,
      exp_id: exp_id,
      inst_id: null,
      create_date: curr_date.toString(),
      launch_date: null,
      finish_date: null,
      create_date_epoch: curr_date.valueOf().toString(),
      launch_opts: launch_opts,
      status: "created",
      labels: labels,
      usage: {},
      logs: null,
      output_tree: []
   };

   // Add execution to DB
   database.db.collection('executions').insert(exec, function(error){
      if(error) return wfcb(error);
      // Success creating experiment
      cb(null, exec);
   });
}

var getExecution = function(exec_id, fields, cb){
   // Check execution arg
   if(!exec_id) return cb(new Error("Incorrect parameters in getExecution, no execution ID has been passed"));

   // Parse fields
   if(!fields){
      fields = {
         _id: 0,
         output_tree: 0,
         logs: 0
      };
   } else {
      // Required values
      fields._id = 0;
      fields.id = 1;
      fields.status = 1;
   }

   // Retrieve execution metadata
   database.db.collection('executions').findOne({id: exec_id}, fields, function(error, exec){
      if(error){
         cb(new Error("Query for execution " + exec_id + " failed"));
      } else if (!exec){
         cb(new Error("Execution " + exec_id + " not found"));
      } else {
         // Return
         cb(null, exec);
      }
   });
}

/**
 * Delete execution data
 */
var destroyExecution = function(exec_id, remove_from_db, app_id, cb){
   logger.debug('['+MODULE_NAME+']['+exec_id+'] Destroy: Destroying...');
   async.waterfall([
      // Get execution
      function(wfcb){
         getExecution(exec_id, null, wfcb);
      },
      // Update database
      function(exec, wfcb){
         // Already deleted?
         if(exec.status == "deleted") return wfcb(true);

         database.db.collection('executions').updateOne({id: exec_id},{$set:{status:"deleting"}});
         wfcb(null, exec);
      },
      // Clean job
      function(exec, wfcb){
         if(exec.inst_id){
            var job_id = null;
            // Experiment will be removed completely from the instance
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Destroy: Cleaning instance...');
            instmanager.cleanExecution(exec_id, exec.inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               if (error) logger.error('['+MODULE_NAME+']['+exec_id+'] Destroy: Failed to clean execution, error: ' + error);
               wfcb(null, exec);
            });
            database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: null}});
         } else {
            wfcb(null, exec);
         }
      },
      // Clean storage
      function(exec, wfcb){
         // Clean output
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Destroy: Cleaning storage...');
         storage.client.invoke('deleteExecution', app_id, exec_id, function(error){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      }
   ],
   function(error){
      if(error){
         // No error, execution was deleted already
         if(error == true) return cb(null);

         // Error trying to delete execution
         database.db.collection('executions').updateOne({id: exec_id},{$set:{status:"delete_failed"}});
         return cb(error);
      }
      // Update status
      if(remove_from_db){
         database.db.collection('executions').remove({id: exec_id});
      } else {
         database.db.collection('executions').updateOne({id: exec_id},{$set:{status:"deleted"}});
      }

      // Callback
      logger.debug('['+MODULE_NAME+']['+exec_id+'] Delete: Done.');
      cb(null);
   });
}

/**
 * Search for an execution
 */
var searchExecutions = function(fields, cb){
   // Connected to DB?
   if(database.db == null){
      cb(new Error("Not connected to DB"));
      return;
   }

   // Do not allow null fields
   if(!fields) fields = {};

   // General query
   var query = {};

   // Filter status
   if(fields.status){
      query['status'] = fields.status;
   }

   // Filter experiment
   if(fields.exp_id){
      query['exp_id'] = fields.exp_id;
   }

   // Projection
   var projection = {
         _id: 0,
         inst_id: 0,
         output_tree: 0,
         logs: 0
   };

   // Retrieve experiment metadata
   database.db.collection('executions').find(query, projection).sort({create_date_epoch:-1}).toArray(function(error, execs){
      if(error) return cb(new Error("Query for executions failed: "+error));
      cb(null, execs);
   });
}

/**
 * Reload executions' file tree.
 */
var reloadExecutionOutputTree = function(exec_id, cb){
   logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Reloading execution output tree...');
   async.waterfall([
      // Get execution
      function(wfcb){
         getExecution(exec_id, null, wfcb);
      },
      // Obtain experiment output data tree
      function(exec, wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Getting output folder tree...');
         storage.client.invoke('getOutputFolderTree', exec_id, function (error, tree) {
            if(error){
               logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Error getting output tree - '+error);
               return wfcb(error);
            }

            database.db.collection('executions').updateOne({id: exec_id},{$set: {output_tree: tree}});
            wfcb(null, exec);
         });
      },
      // Obtain experiment output usage
      function(exec, wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Getting output storage usage...');
         storage.client.invoke('getOutputFolderUsage', exec_id, function (error, bytes) {
            if(error){
               logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Error getting output usage - '+error);
               return wfcb(error);
            }

            // Update DB
            database.db.collection('executions').updateOne({id: exec_id},{$set: {'usage.storage': bytes}});
            wfcb(null, exec);
         });
      }
   ],
   function(error){
      if(error) return cb(error);
      logger.debug('['+MODULE_NAME+']['+exec_id+'] ReloadTree: Trees reloaded.');
      cb(error);
   });
}

/**
 * Initialize module
 */
var init = function(cfg, cb){
   // Set constants
   constants = cfg;

   // Create DB indices
   database.db.collection('executions').dropIndexes();
   database.db.collection('executions').ensureIndex({id:-1});
   database.db.collection('executions').ensureIndex({create_date_epoch:-1});
   database.db.collection('executions').ensureIndex({exp_id:-1});
   database.db.collection('executions').ensureIndex({status:-1});

   // Return
   cb(null);
}

module.exports.init = init;

exports.createExecution = createExecution;
exports.getExecution = getExecution;
exports.destroyExecution = destroyExecution;
exports.searchExecutions = searchExecutions;
exports.reloadExecutionOutputTree = reloadExecutionOutputTree;
