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
 * Entry point for experiment execution
 */
var launchExperiment = function(exp_id, nodes, image_id, size_id, checkCallback){

   var _minion = minionClient;
   var _app = null;
   var _exp = null;
   var _system = null;

   _getExperiment(exp_id, function(err, exp){
      if(err){
         checkCallback(err);
         return;
      }
      checkCallback(null);

      // Set exp var
      _exp = exp;

      // Reinitialize experiment to status "created"
      _exp.status = "created";
      db.collection('experiments').updateOne({id: _exp.id},{$set:{status:"created"}});

      // Execute operations
      async.waterfall([
         // Retrieve application metadata
         function(cb){
            console.log("Getting application");
            _getApplication(_exp.app_id, cb);
         },
         // First, define a system where execution will take place
         function(app, cb){
            console.log("Selected experiment: " + _exp.name + "\n" +
                        "-- App: " + app.name);
            _app = app;
            _defineSystem(_minion, nodes, image_id, size_id, cb);
         },
         // Instance system
         function(system, cb){
            console.log("-----------------------\n",
                        "System configured with:\n",
                        "-- Image: ", system.image.name, " - ", system.image.id, "\n",
                        "-- Size: ", system.size.name, " - ", system.size.id, "\n");
                        _system = system;
                        _instanceSystem(_minion, system, cb);
         },
         // Set experiment execution environment
         function(cb){
            msg = "-----------------------\nSystem instances:\n";
            for(i = 0; i < _system.nodes; i++){
               msg += "-- " + i + ": " + _system.instances[i] + "\n";
            }
            console.log(msg);

            // Set execution environment
            var exec_env = {
               nodes: _system.nodes,
               cpus: _system.size.cpus,
               inputpath: _system.image.inputpath,
               libpath: _system.image.libpath,
               tmppath: _system.image.tmppath
            };
            _setExecEnvironment(exp_id, exec_env, cb);
         },
         // Prepare experiment for the system
         function(cb){
            console.log("Execution environment has been set");

            // Prepare experiment
            _prepareExperiment(exp_id, cb);
         },
         // Deploy experiment in master instance
         function(cb){
            console.log("Deploying experiment " + exp_id);

            // Prepare experiment
            _deployExperiment(_minion, _app, _exp, _system, cb);
         },
         // Execute experiment
         function(cb){
            console.log("Executing experiment " + exp_id);

            // Prepare experiment
            _executeExperiment(_minion, _app, _exp, _system, cb);
         }
      ],
      function(err){
         if(err){
            console.error("Failed to launch experiment " + exp_id + ", err: " + err);
         } else {
            console.log("Experiment " + exp_id + " has been executed");
         }
      });
   });
}

var _getApplication = function(app_id, cb){
   // Connected to DB?
   if(db == null){
      cb(new Error("Not connected to DB"));
   }

   // Retrieve experiment metadata
   db.collection('applications').findOne({id: app_id}, function(err, app){
      if(err){
         cb(new Error("Query for application " + app_id + " failed"));
      } else if (!app){
         cb(new Error("Application " + app_id + " not found"));
      } else {
         cb(null, app);
      }
   });
}

var _getExperiment = function(exp_id, cb){
   // Connected to DB?
   if(db == null){
      cb(new Error("Not connected to DB"));
   }

   // Retrieve experiment metadata
   db.collection('experiments').findOne({id: exp_id}, function(err, exp){
      if(err){
         cb(new Error("Query for experiment " + exp_id + " failed"));
      } else if (!exp){
         cb(new Error("Experiment " + exp_id + " not found"));
      } else {
         cb(null, exp);
      }
   });
}

/**
 * Define a system of instances
 */
var _defineSystem = function(minion, nodes, image_id, size_id, cb){
   // Get image and size IDs for the system in this minion
   _getImage(minion, image_id, function(err, image){
      if(err){
         cb(err);
         return;
      }
      _getSize(minion, size_id, function(err, size){
         if(err){
            console.error(err);
            cb(err);
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
         cb(null, system);
      });
   });
}

var _instanceSystem = function(minion, system, cb){
   // Check if is already being instanced
   if(system.status != "defined"){
      cb(new Error("System is already instanced..."));
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
         cb(err);
      } else {
         // Set first instance as master
         system.master = system.instances[0];
         // Set system status
         system.status = "instanced";
         // Callback with instanced system
         cb(null);
      }
   });
}

/**
 * Set experiment execution environment
 */
var _setExecEnvironment = function(exp_id, exec_env, cb){
   storageClient.invoke('updateExperiment', exp_id, {exec_env: JSON.stringify(exec_env)}, function (error, result, more) {
      if (error) {
         cb(new Error("Failed to set execution environment of experiment ", exp_id, ", err: ", error));
      } else {
         // Experiment have now an execution environment
         cb(null);
      }
   });
}

/**
 * Prepares an experiment to be deployed.
 * Labels will be applied.
 */
var _prepareExperiment = function(exp_id, cb){
   // First step is to prepare experiment for the selected configuration
   storageClient.invoke('prepareExperiment', exp_id, function (error, result, more) {
      if (error) {
         cb(new Error("Failed to prepare experiment ", exp_id, ", err: ", error));
      } else {
         // Experiment can now be deployed in target instances
         cb(null);
      }
   });
}

/**
 * Deploy an experiment in target system
 */
var _deployExperiment = function(minion, app, exp, system, cb){
   if(system.status != "instanced"){
      cb(new Error("Target system is not instanced"));
   } else {
      // Deploy experiment
      minion.invoke('deployExperiment', app, exp, system, function (error, result, more) {
         if (error) {
            cb(new Error("Failed to deploy experiment ", exp.id, ", err: ", error));
         } else {
            // Experiment is deployed and waiting to be compiled
            _waitExperimentCompilation(minion, exp, system, cb);
         }
      });
   }
}

var _waitExperimentCompilation = function(minion, exp, system, cb){
   // Poll experiment
   minion.invoke('pollExperiment', exp, system, function (error, status, more) {
      if (error) {
         cb(new Error("Failed to poll experiment ", exp.id, ", err: ", error));
      } else {
         // Update status
         exp.status = status;
         db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

         // Check status
         if(status == "deployed" || status == "compiling"){
            // Recheck later
            setTimeout(_waitExperimentCompilation, 5000, minion, exp, system, cb);
         } else if(status == "compiled") {
            // Compilation successful
            cb(null);
         } else {
            // Compilation failed
            cb(new Error("Experiment " + exp.id + " failed to compiled, status: " + status));
         }
      }
   });
}

/**
 * Execute an experiment in target system
 */
var _executeExperiment = function(minion, app, exp, system, cb){
   if(system.status != "instanced"){
      cb(new Error("Target system is not instanced"));
   } else {
      // Deploy experiment
      minion.invoke('executeExperiment', app, exp, system, function (error, result, more) {
         if (error) {
            cb(new Error("Failed to execute experiment ", exp.id, ", err: ", error));
         } else {
            // Experiment can now be executed
            _waitExperimentExecution(minion, exp, system, cb);
         }
      });
   }
}

var _waitExperimentExecution = function(minion, exp, system, cb){
   // Poll experiment
   minion.invoke('pollExperiment', exp, system, function (error, status, more) {
      if (error) {
         cb(new Error("Failed to poll experiment ", exp.id, ", err: ", error));
      } else {
         // Update status
         exp.status = status;
         db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

         // Check status
         if(status == "compiled" || status == "executing"){
            // Recheck later
            setTimeout(_waitExperimentExecution, 5000, minion, exp, system, cb);
         } else if(status == "done") {
            // Execution successful
            cb(null);
         } else {
            // Execution failed
            cb(new Error("Experiment " + exp.id + " failed to execute, status: " + status));
         }
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
         if(images.length != 1){
            cb(new Error("Image " + image_id + " not found"));
         } else {
            cb(null, images[0]);
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
         if(sizes.length != 1){
            cb(new Error("Size " + size_id + " not found"));
         } else {
            cb(null, sizes[0]);
         }
      }
   });
}

exports.launchExperiment = launchExperiment;
