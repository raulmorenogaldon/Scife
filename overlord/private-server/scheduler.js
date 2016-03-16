var zerorpc = require('zerorpc');
var mongo = require('mongodb').MongoClient;
var async = require('async');
var constants = require('./constants.json');

var minionClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});

var storageClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});

// Submodules
var apps = require('./application.js');
var exps = require('./experiment.js');

// Connect to minions
console.log('Conecting to minion-url: ' + constants.MINION_URL +
            '\nConecting to storage-url: ' + constants.STORAGE_URL);

minionClient.connect(constants.MINION_URL);
storageClient.connect(constants.STORAGE_URL);

// Connect to DB
console.log('Conecting to MongoDB: ' + constants.MONGO_URL);
var db = null;
mongo.connect(constants.MONGO_URL, function(err, database){
   if(err){
      console.error("Failed to connect to MongoDB, err: ", err);
   } else {
      console.log("Successfull connection to DB");
      db = database;
   }
});

/**
 * Get application metadata
 */
var getApplication = function(app_id, getCallback){
   apps.getApplication(app_id, getCallback);
}

/**
 * Create an aplication from config
 */
var createApplication = function(app_cfg, createCallback){
   apps.createApplication(app_cfg, createCallback);
}

/**
 * Search applications
 */
var searchApplications = function(name, searchCallback){
   apps.searchApplications(name, searchCallback);
}

/**
 * Get experiment metadata
 */
var getExperiment = function(exp_id, getCallback){
   exps.getExperiment(exp_id, getCallback);
}

/**
 * Create an experiment from config
 */
var createExperiment = function(exp_cfg, createCallback){
   exps.createExperiment(exp_cfg, createCallback);
}

/**
 * Update an experiment
 */
var updateExperiment = function(exp_id, exp_cfg, updateCallback){
   exps.updateExperiment(exp_id, exp_cfg, updateCallback);
}

/**
 * Search experiments
 */
var searchExperiments = function(name, searchCallback){
   exps.searchExperiments(name, searchCallback);
}


/**
 * Entry point for experiment execution
 */

var launchExperiment = function(exp_id, nodes, image_id, size_id, checkCallback){
   var minion = minionClient;

   // Check data
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Check experiment status
      function(exp, wfcb){
         if(exp.status && exp.status != "created"){
            wfcb(new Error("Experiment " + exp.id + " is already deployed!, status: " + exp.status));
         } else {
            // Check image ID exists
            _getImage(minion, image_id, wfcb);
         }
      },
      // Check size ID exists
      function(image, wfcb){
         _getSize(minion, size_id, wfcb);
      }
   ],
   function(error){
      if(error){
         // Error trying to launch experiment
         checkCallback(error);
      } else {
         // No error in launch
         checkCallback(null);

         // Update status
         db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created"}});

         // Launch workflow
         _workflowExperiment(exp_id, minion, nodes, image_id, size_id);
      }
   });
}

/**
 * Reset experiment to create status
 */
var resetExperiment = function(exp_id, resetCallback){
   var minion = minionClient;

   // Get experiment data
   getExperiment(exp_id, function(error, exp){
      if(error){
         resetCallback(error);
         return;
      }
      console.log("Reseting experiment " + exp_id);;

      // Remove data from instance
      if(exp.system){
         // Call clean
         minion.invoke('cleanExperiment', exp, exp.system, function (error, result, more) {
            if (error) {
               resetCallback(new Error("Failed to set execution environment of experiment ", exp_id, ", error: ", error));
            } else {
               // Update status
               db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", system:""}});
               // Callback
               resetCallback(null);
            }
         });
      } else {
         // Update status
         db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", system:""}});
         resetCallback(null);
      }
   });
}

var _workflowExperiment = function(exp_id, minion, nodes, image_id, size_id){

   var _system = null;

   getExperiment(exp_id, function(error, exp){
      // Execute operations on experiment
      async.waterfall([
         // Get application
         function(wfcb){
            getApplication(exp.app_id, wfcb);
         },
         // First, define a system where execution will take place
         function(app, wfcb){
            console.log("Selected experiment: " + exp.name + "\n" +
                        "-- App: " + app.name);
            _defineSystem(minion, nodes, image_id, size_id, wfcb);
         },
         // Instance system
         function(system, wfcb){
            console.log("-----------------------\n",
                        "System configured with:\n",
                        "-- Image: ", system.image.name, " - ", system.image.id, "\n",
                        "-- Size: ", system.size.name, " - ", system.size.id, "\n");
                        _system = system;
                        _instanceSystem(minion, system, wfcb);
         },
         // Set experiment execution environment
         function(wfcb){
            msg = "-----------------------\nSystem instances:\n";
            for(i = 0; i < _system.nodes; i++){
               msg += "-- " + i + ": " + _system.instances[i] + "\n";
            }
            console.log(msg);

            // Set execution environment
            var exec_env = {
               nodes: _system.nodes,
               cpus: _system.size.cpus,
               inputpath: _system.image.inputpath + "/" + exp_id,
               libpath: _system.image.libpath,
               tmppath: _system.image.tmppath
            };
            _setExecEnvironment(exp_id, exec_env, wfcb);
         },
         // Prepare experiment for the system
         function(wfcb){
            console.log("Execution environment has been set");

            // Prepare experiment
            _prepareExperiment(exp_id, _system, wfcb);
         },
         // Deploy experiment in master instance
         function(wfcb){
            console.log("Deploying experiment " + exp.id);

            // Prepare experiment
            _deployExperiment(minion, exp_id, _system, wfcb);
         },
         // Execute experiment
         function(wfcb){
            console.log("Executing experiment " + exp.id);

            // Prepare experiment
            _executeExperiment(minion, exp_id, _system, wfcb);
         }
      ],
      function(error){
         if(error){
            console.error("Failed to launch experiment " + exp.id + ", error: " + error);
         } else {
            console.log("Experiment " + exp.id + " has been executed");
         }
      });
   });
}

/**
 * Define a system of instances
 */
var _defineSystem = function(minion, nodes, image_id, size_id, defineCallback){
   // Get image and size IDs for the system in this minion
   _getImage(minion, image_id, function(error, image){
      if(error){
         defineCallback(error);
         return;
      }
      _getSize(minion, size_id, function(error, size){
         if(error){
            defineCallback(error);
            return;
         }

         // Create system object
         var system = {
            nodes: nodes,
            image: image,
            size: size,
            status: "defined"
         };

         // Return system
         defineCallback(null, system);
      });
   });
}

var _instanceSystem = function(minion, system, instanceCallback){
   // Check if is already being instanced
   if(system.status != "defined"){
      instanceCallback(new Error("System is already instanced..."));
      return;
   }

   // Change system status
   system.instances = [];
   system.status = "instancing";

   // Create instances
   var tasks = [];
   for(i = 0; i < system.nodes; i++){
      tasks.push(function(taskcb){
         minion.invoke('createInstance', {
            name:"Unnamed",
            image_id: system.image.id,
            size_id: system.size.id
         }, function (error, instance_id, more) {
            if(error){
               taskcb(new Error("Failed to create instance from minion " + minion + ", err: " + error));
            } else {
               // Add instance to system
               system.instances.push(instance_id);
               taskcb(null);
            }
         });
      });
   }

   // Execute tasks
   async.parallel(tasks, function(err){
      if(err){
         instanceCallback(err);
      } else {
         // Set first instance as master
         system.master = system.instances[0];
         // Set system status
         system.status = "instanced";
         // Callback with instanced system
         instanceCallback(null);
      }
   });
}

/**
 * Set experiment execution environment
 * DEPRECATED
 */
var _setExecEnvironment = function(exp_id, exec_env, setCallback){
   setCallback(null);
   //storageClient.invoke('updateExperiment', exp_id, {exec_env: exec_env}, function (error, result, more) {
   //   if (error) {
   //      setCallback(new Error("Failed to set execution environment of experiment ", exp_id, ", err: ", error));
   //   } else {
   //      // Experiment have now an execution environment
   //      setCallback(null);
   //   }
   //});
}

/**
 * Prepare an experiment to be deployed.
 * Labels will be applied.
 */
var _prepareExperiment = function(exp_id, system, prepareCallback){
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Get application
      function(exp, wfcb){
         getApplication(exp.app_id, function(error, app){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp);
            }
         });
      },
      // Update labels for this system
      function(app, exp, wfcb){
         // Get application labels and join with experiment ones
         for(var i in app.labels){
            if(!exp.labels[app.labels[i]]){
               exp.labels[app.labels[i]] = "";
            }
         }

         // Set system labels
         exp.labels['#EXPERIMENT_ID'] = exp.id;
         exp.labels['#EXPERIMENT_NAME'] = exp.name.replace(/ /g, "_");
         exp.labels['#APPLICATION_ID'] = app.id;
         exp.labels['#APPLICATION_NAME'] = exp.name.replace(/ /g, "_");
         exp.labels['#INPUTPATH'] = system.image.inputpath;
         exp.labels['#LIBPATH'] = system.image.libpath;
         exp.labels['#TMPPATH'] = system.image.tmppath;
         exp.labels['#CPUS'] = system.size.cpus+'';
         exp.labels['#NODES'] = system.size.nodes+'';
         exp.labels['#TOTALCPUS'] = (system.size.nodes * system.size.cpus)+'';

         // Apply labels
         storageClient.invoke('prepareExperiment', app.id, exp.id, exp.labels, function(error){
            if(error){
               wfcb(error);
            } else {
               wfcb(null);
            }
         });
      }
   ],
   function(error){
      if(error){
         prepareCallback(error);
      } else {
         console.log("Experiment " + exp_id + " is prepared for deployment");
         prepareCallback(null);
      }
   });
}

/**
 * Deploy an experiment in target system
 */
var _deployExperiment = function(minion, exp_id, system, deployCallback){
   if(system.status != "instanced"){
      deployCallback(new Error("Target system is not instanced"));
      return;
   }

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Get application
      function(exp, wfcb){
         getApplication(exp.app_id, function(error, app){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp);
            }
         });
      },
      // Get experiment URL
      function(app, exp, wfcb){
         storageClient.invoke('getApplicationURL', app.id, function(error, url){
            if(error){
               wfcb(error);
            } else {
               exp.exp_url = url;
               wfcb(null, app, exp);
            }
         });
      },
      // Get input URL
      function(app, exp, wfcb){
         storageClient.invoke('getExperimentInputURL', exp_id, function(error, url){
            if(error){
               wfcb(error);
            } else {
               exp.input_url = url;
               wfcb(null, app, exp);
            }
         });
      },
      // Deploy
      function(app, exp, wfcb){
         minion.invoke('deployExperiment', app, exp, system, function (error, result, more) {
            if (error) {
               wfcb(new Error("Failed to deploy experiment ", exp.id, ", err: ", error));
            } else {
               // Set experiment system
               db.collection('experiments').updateOne({id: exp.id},{$set:{system:system}});

               // Experiment is deployed and waiting to be compiled
               _waitExperimentCompilation(minion, exp_id, system, wfcb);
            }
         });
      }
   ],
   function(error){
      if(error){
         deployCallback(error);
      } else {
         console.log("Experiment " + exp_id + " has been deployed");
         deployCallback(null);
      }
   });
}

var _waitExperimentCompilation = function(minion, exp_id, system, waitCallback){
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Poll experiment
      function(exp, wfcb){
         minion.invoke('pollExperiment', exp, system, function (error, status) {
            if(error){
               wfcb(new Error("Failed to poll experiment ", exp.id, ", err: ", error));
               return;
            }

            // Update status
            db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

            // Check status
            if(status == "deployed" || status == "compiling"){
               // Recheck later
               setTimeout(_waitExperimentCompilation, 5000, minion, exp_id, system, waitCallback);
            } else if(status == "compiled") {
               // Compilation successful
               wfcb(null);
            } else {
               // Compilation failed
               wfcb(new Error("Experiment " + exp.id + " failed to compiled, status: " + status));
            }
         });
      }
   ],
   function(error){
      if(error){
         waitCallback(error);
      } else {
         console.log("Experiment " + exp_id + " compilation succeded");
         waitCallback(null);
      }
   });
}

/**
 * Execute an experiment in target system
 */
var _executeExperiment = function(minion, exp_id, system, executionCallback){
   if(system.status != "instanced"){
      executionCallback(new Error("Target system is not instanced"));
      return;
   }

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Get application
      function(exp, wfcb){
         // First, check experiment status
         if(exp.status != "compiled"){
            wfcb(new Error("Failed to execute experiment ", exp.id, ", Invalid status: ", exp.status));
            return;
         }

         // Now, get application
         getApplication(exp.app_id, function(error, app){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp);
            }
         });
      },
      // Execute experiment
      function(app, exp, wfcb){
         minion.invoke('executeExperiment', app, exp, system, function (error, result, more) {
            if (error) {
               wfcb(new Error("Failed to deploy experiment ", exp.id, ", err: ", error));
            } else {
               // Set experiment system
               db.collection('experiments').updateOne({id: exp.id},{$set:{system:system}});

               // Experiment is deployed and waiting to be compiled
               _waitExperimentExecution(minion, exp_id, system, wfcb);
            }
         });
      }
   ],
   function(error){
      if(error){
         executionCallback(error);
      } else {
         console.log("Experiment " + exp_id + " has been deployed");
         executionCallback(null);
      }
   });
}

var _waitExperimentExecution = function(minion, exp_id, system, waitCallback){
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, wfcb);
      },
      // Poll experiment
      function(exp, wfcb){
         minion.invoke('pollExperiment', exp, system, function (error, status) {
            if(error){
               wfcb(new Error("Failed to poll experiment ", exp.id, ", err: ", error));
               return;
            }

            // Update status
            db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

            // Check status
            if(status == "compiled" || status == "executing"){
               // Recheck later
               setTimeout(_waitExperimentExecution, 5000, minion, exp_id, system, waitCallback);
            } else if(status == "done") {
               // Execution successful
               wfcb(null);
            } else {
               // Execution failed
               wfcb(new Error("Experiment " + exp.id + " failed to execute, status: " + status));
            }
         });
      }
   ],
   function(error){
      if(error){
         waitCallback(error);
      } else {
         console.log("Experiment " + exp_id + " execution succeded");
         waitCallback(null);
      }
   });
}

/**
 * Selects the best minion for an specific configuration
 */
var _selectBestMinion = function(nodes){
   // Basic selection
   return minionClient;
}

/**
 * Select an appropiate image from a minion
 */
var _getImage = function(minion, image_id, cb){
   // Select first image
   minion.invoke('getImages', image_id, function (error, images, more) {
      if(error){
         cb(error);
      } else {
         // Return image
         if(images instanceof Array){
            cb(new Error("Image " + image_id + " not found"));
         } else {
            cb(null, images);
         }
      }
   });
}

/**
 * Select an appropiate size from a minion
 */
var _getSize = function(minion, size_id, cb){
   // Select sizes with this parameters
   minion.invoke('getSizes', size_id, function (error, sizes, more) {
      if(error){
         cb(error);
      } else {
         if(sizes instanceof Array){
            cb(new Error("Size " + size_id + " not found"));
         } else {
            cb(null, sizes);
         }
      }
   });
}

exports.launchExperiment = launchExperiment;

exports.getApplication = getApplication;
exports.createApplication = createApplication;
exports.searchApplications = searchApplications;

exports.getExperiment = getExperiment;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
exports.resetExperiment = resetExperiment;
exports.searchExperiments = searchExperiments;
