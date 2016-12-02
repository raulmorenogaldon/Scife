var zerorpc = require('zerorpc');
var async = require('async');

var utils = require('./utils.js');
var logger = utils.logger;
var database = require('./database.js');
var storage = require('./storage.js');

/**
 * Module vars
 */
var MODULE_NAME = "AP";

/**
 * Get application data
 */
var getApplication = function(app_id, getCallback){
   // Connected to DB?
   if(database.db == null){
      getCallback(new Error("Not connected to DB"));
      return;
   }

   // Retrieve application metadata
   database.db.collection('applications').findOne({id: app_id}, function(error, app){
      if(error){
         getCallback(new Error("Query for application " + app_id + " failed"));
      } else if (!app){
         getCallback(new Error("Application " + app_id + " not found"));
      } else {
         getCallback(null, app);
      }
   });
}

/**
 * Search for an application
 */
var searchApplications = function(name, searchCallback){
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

   // Retrieve application metadata
   database.db.collection('applications').find({name: {$regex: query}}).toArray(function(error, apps){
      if(error){
         searchCallback(new Error("Query for applications with name: " + name + " failed"));
      } else {
         searchCallback(null, apps);
      }
   });
}

/**
 * Create an application and insert it into DB
 */
var createApplication = function(app_cfg, createCallback){
   // Check parameters
   if(!'name' in app_cfg){
      createCallback(new Error("Error creating application, 'name' not set."));
      return;
   }
   if(!'creation_script' in app_cfg){
      createCallback(new Error("Error creating application, 'creation_script' not set."));
      return;
   }
   if(!'execution_script' in app_cfg){
      createCallback(new Error("Error creating application, 'execution_script' not set."));
      return;
   }
   if(!'path' in app_cfg){
      createCallback(new Error("Error creating application, 'path' not set."));
      return;
   }

   // Do tasks
   async.waterfall([
      // Check if application name exists
      function(wfcb){
         database.db.collection('applications').findOne({name: app_cfg.name}, function(error, app){
            if(error){
               wfcb(new Error("Query for application name " + app_cfg.name + " failed"));
            } else if (app){
               wfcb(new Error("Application with name '" + app_cfg.name + "' already exists"));
            } else {
               wfcb(null);
            }
         });
      },
      function(wfcb){
         // Create UUID
         app_cfg.id = utils.generateUUID();

         // Copy application to storage
         storage.client.invoke('copyApplication', app_cfg.id, app_cfg.path, function (error) {
            if(error){
               wfcb(error);
            } else {
               wfcb(null);
            }
         });
      },
      function(wfcb){
         // Get labels list
         storage.client.invoke('discoverMetadata', app_cfg.id, function(error, labels){
            if(error){
               wfcb(error);
            } else {
               // Save labels
               app_cfg.labels = labels;
               wfcb(null);
            }
         });
      },
      function(wfcb){
         //Create application data
         app = {
            _id: app_cfg.id,
            id: app_cfg.id,
            name: app_cfg.name,
            desc: ('desc' in app_cfg) ? app_cfg.desc : "Description...",
            creation_script: app_cfg.creation_script,
            execution_script: app_cfg.execution_script,
            labels: app_cfg.labels
         }

         // Add application to DB
         database.db.collection('applications').insert(app, function(error){
            if(error){
               wfcb(error);
            } else {
               // Success adding app
               wfcb(null, app);
            }
         });
      }
   ],
   function(error, app){
      if(error){
         console.log("Error creating application with config: " + JSON.stringify(app_cfg));
         createCallback(error);
      }

      // Return app data
      console.log("Created application: " + JSON.stringify(app));
      createCallback(null, app);
   });
}

/**
 * Execute an operation over an application
 */
var maintainApplication = function(app_id, operation, maintainCallback){
   // Check parameters
   if(!app_id) return maintainCallback(new Error("Application ID not set."));

   // Do tasks
   async.waterfall([
      // Get application
      function(wfcb){
         getApplication(app_id, wfcb);
      },
      // Apply operation
      function(app, wfcb){
         if(operation == 'discoverMetadata'){
            // Get labels list
            storage.client.invoke('discoverMetadata', app_id, null, function(error, labels){
               if(error) return wfcb(error);
               // Update labels in DB
               database.db.collection('applications').update({id: app_id}, {$set: {labels: labels}}, function(error){
                  if(error) return wfcb(error);
                  // Success updating labels
                  return wfcb(null, app);
               });
            });
         } else {
            return wfcb(new Error("Unknown operation: "+operation));
         }
      }
   ],
   function(error, app){
      if(error) return maintainCallback(error);

      // Success
      logger.info('['+MODULE_NAME+']['+app_id+'] Operation "'+operation+'" success.');
      return maintainCallback(null);
   });
}

exports.getApplication = getApplication;
exports.searchApplications = searchApplications;
exports.createApplication = createApplication;
exports.maintainApplication = maintainApplication;
