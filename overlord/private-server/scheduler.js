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
console.log('Connecting to minion-url: ' + constants.MINION_URL +
            '\nConnecting to storage-url: ' + constants.STORAGE_URL);

minionClient.connect(constants.MINION_URL);
storageClient.connect(constants.STORAGE_URL);

// Set minions shortcuts
var minions = {}
minionClient.invoke('getMinionName', function(error, name){
   if(error){
      console.error("Failed to get minion name, error: ", error);
   } else {
      minions[name] = minionClient;
   }
});

// Connect to DB
console.log('Connecting to MongoDB: ' + constants.MONGO_URL);
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
var getExperiment = function(exp_id, fields, getCallback){
   exps.getExperiment(exp_id, fields, getCallback);
}

/**
 * Get experiment output data file path
 */
var getExperimentOutputFile = function(exp_id, getCallback){
   storageClient.invoke("getExperimentOutputFile", exp_id, function(error, file){
      if (error) {
         getCallback(new Error("Failed to get experiment ", exp_id, " output data, error: ", error));
      } else {
         getCallback(null, file);
      }
   });
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

var launchExperiment = function(exp_id, nodes, image_id, size_id, launchCallback){
   var minion = 'ClusterMinion';

   // Check data
   async.waterfall([
      // Check experiment status
      function(wfcb){
         // Check image ID exists
         _getImage(minion, image_id, wfcb);
      },
      // Check size ID exists
      function(image, wfcb){
         _getSize(minion, size_id, wfcb);
      },
      // Get experiment
      function(size, wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      function(exp, wfcb){
         if(exp.status && exp.status != "created"){
            wfcb(new Error("Experiment " + exp.id + " is already launched!, status: " + exp.status));
         } else {
            wfcb(null);
         }
      },
   ],
   function(error){
      if(error){
         // Error trying to launch experiment
         launchCallback(error);
      } else {
         // Update status
         db.collection('experiments').updateOne({id: exp_id},{$set:{status:"launched"}});

         // Callback
         launchCallback(null);

         // Launch workflow
         _workflowExperiment(exp_id, minion, nodes, image_id, size_id);
      }
   });
}

/**
 * Reset experiment to create status
 */
var resetExperiment = function(exp_id, hardreset, resetCallback){
   var minionRPC = null;

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         db.collection('experiments').updateOne({id: exp_id},{$set:{status:"resetting"}});
         if(exp.system){
            // Get minion RPC
            minionRPC = minions[exp.system.minion];
            _getInstance(exp.system.minion, exp.system.master, function(error, headnode){
               if(error){
                  wfcb(error);
               } else {
                  wfcb(null, exp, headnode);
               }
            });
         } else {
            wfcb(null, exp, null);
         }
      },
      // Get instance image
      function(exp, headnode, wfcb){
         if(exp.system){
            _getImage(exp.system.minion, headnode.image_id, function(error, image){
               if(error){
                  wfcb(error);
               } else {
                  wfcb(null, exp, headnode, image);
               }
            });
         } else {
            wfcb(null, exp, null, null);
         }
      },
      // Clean job
      function(exp, headnode, image, wfcb){
         if(exp.system){
            console.log("["+exp_id+"] Reset: cleaning job");
            minionRPC.invoke('cleanJob', exp.job_id, headnode.id, function (error, result, more) {
               if (error) {
                  wfcb(new Error("Failed to clean job: ", exp.job_id, "\nError: ", error));
               } else {
                  wfcb(null, exp, headnode, image);
               }
            });
         } else {
            wfcb(null, exp, null, null);
         }
      },
      // Remove experiment code folder
      function(exp, headnode, image, wfcb){
         if(exp.system){
            console.log("["+exp_id+"] Reset: removing code folder");
            var work_dir = image.workpath+"/"+exp.id;
            var cmd = 'rm -rf '+work_dir;
            // Execute command
            minionRPC.invoke('executeCommand', cmd, headnode.id, function (error, result, more) {
               if (error) {
                  wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
               } else {
                  wfcb(null, exp, headnode, image);
               }
            });
         } else {
            wfcb(null, exp, null, null);
         }
      },
      // Remove experiment input data (hard reset)
      function(exp, headnode, image, wfcb){
         if(exp.system && hardreset){
            console.log("["+exp_id+"] Reset: removing input folder");
            var input_dir = image.inputpath+"/"+exp.id;
            var cmd = 'rm -rf '+input_dir;
            // Execute command
            console.log("Executing: "+cmd);
            minionRPC.invoke('executeCommand', cmd, headnode.id, function (error, result, more) {
               if (error) {
                  wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
               } else {
                  wfcb(null);
               }
            });
         } else {
            wfcb(null);
         }
      },
   ],
   function(error){
      if(error){
         // Error trying to launch experiment
         resetCallback(error);
      } else {
         // Update status
         if(hardreset){
            db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", system:""}});
            console.log("["+exp_id+"] Reset: HARD reset done");
         } else {
            db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created"}});
            console.log("["+exp_id+"] Reset: done");
         }
         // Callback
         resetCallback(null);
      }
   });
}

/**
 * Remove an experiment.
 */
var destroyExperiment = function(exp_id, destroyCallback){
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Reset it
      function(exp, wfcb){
         resetExperiment(exp_id, true, wfcb);
      },
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Destroy system
      function(exp, wfcb){
         if(exp.system){
            _destroySystem(exp.system, wfcb);
         } else {
            wfcb(null);
         }
      },
      // Remove from DB
      function(wfcb){
         db.collection('experiments').remove({id: exp_id});
         wfcb(null);
      },
   ],
   function(error){
      if(error){
         console.log("Error destroying experiment, error: " + error);
         destroyCallback(error);
      } else {
         // Log success
         console.log("Deleted experiment: " + exp_id);
         destroyCallback(null);
      }
   });
}

var _workflowExperiment = function(exp_id, minion, nodes, image_id, size_id){

   console.log("["+exp_id+"] Workflow: Begin");
   var _system = null;

   getExperiment(exp_id, null, function(error, exp){
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
            console.log("["+exp_id+"] Workflow: Preparing...");

            // Prepare experiment
            _prepareExperiment(exp_id, _system, wfcb);
         },
         // Deploy experiment in master instance
         function(wfcb){
            console.log("["+exp_id+"] Workflow: Deploying...");

            // Prepare experiment
            _deployExperiment(minion, exp_id, _system, wfcb);
         },
         // Execute experiment
         function(wfcb){
            console.log("["+exp_id+"] Workflow: Executing...");

            // Prepare experiment
            _executeExperiment(minion, exp_id, _system, wfcb);
         },
         // Retrieve experiment output data
         function(wfcb){
            console.log("["+exp_id+"] Workflow: Retrieving...");

            // Prepare experiment
            _retrieveExperimentOutput(minion, exp_id, _system, wfcb);
         }
      ],
      function(error){
         if(error){
            console.log("["+exp_id+"] Workflow: Failed, error: " + error);
         } else {
            console.log("["+exp_id+"] Workflow: Executed...");
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
            minion: minion,
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
   system.minion = minion;

   // Get minion RPC
   var minionRPC = minions[minion];

   // Create instances
   var tasks = [];
   for(i = 0; i < system.nodes; i++){
      tasks.push(function(taskcb){
         minionRPC.invoke('createInstance', {
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

var _destroySystem = function(system, destroyCallback){
   // Check if system has instances
   if(!system.minion || !system.instances || system.instances.length == 0){
      destroyCallback(null);
      return;
   }

   // Get minion RPC
   var minionRPC = minions[system.minion];

   // Destroy instances
   var tasks = [];
   for(var inst in system.instances){
      (function(inst){
         tasks.push(function(taskcb){
            console.log("Instance: " + inst + " / " + system.instances[inst]);
            if(system.instances[inst]){
               console.log("Destroying instance " + system.instances[inst]);
               minionRPC.invoke('destroyInstance', system.instances[inst], function (error) {
                  if(error){
                     taskcb(new Error("Failed to destroy instance from minion " + system.minion+ ", err: " + error));
                  } else {
                     taskcb(null);
                  }
               });
            } else {
               taskcb(null);
            }
         });
      })(inst);
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error){
         destroyCallback(error);
      } else {
         // Change system status
         system.instances = [];
         system.status = "defined";

         // Callback
         destroyCallback(null);
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
         getExperiment(exp_id, null, wfcb);
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
         console.log("["+exp_id+"] Preparing labels");
         // Get application labels and join with experiment ones
         for(var i in app.labels){
            if(!exp.labels[app.labels[i]]){
               exp.labels[app.labels[i]] = "";
            }
         }

         // Numeric vars
         var nodes = system.nodes + '';
         var cpus = system.size.cpus + '';
         var totalcpus = system.size.cpus * system.nodes;
         totalcpus = totalcpus + '';

         // Set system labels
         exp.labels['#EXPERIMENT_ID'] = exp.id;
         exp.labels['#EXPERIMENT_NAME'] = exp.name.replace(/ /g, "_");
         exp.labels['#APPLICATION_ID'] = app.id;
         exp.labels['#APPLICATION_NAME'] = exp.name.replace(/ /g, "_");
         exp.labels['#INPUTPATH'] = system.image.inputpath + "/" + exp.id;
         exp.labels['#LIBPATH'] = system.image.libpath;
         exp.labels['#TMPPATH'] = system.image.tmppath;
         exp.labels['#CPUS'] = cpus;
         exp.labels['#NODES'] = nodes;
         exp.labels['#TOTALCPUS'] = totalcpus;

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
         console.log("["+exp_id+"] Prepared for deployment");
         prepareCallback(null);
      }
   });
}

/**
 * Deploy an experiment in target system
 */
var _deployExperiment = function(minion, exp_id, system, deployCallback){
   // Check the system is instanced
   if(system.status != "instanced"){
      deployCallback(new Error("Target system is not instanced"));
      return;
   }

   // Get minion RPC
   var minionRPC = minions[minion];

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get application
      function(exp, wfcb){
         if(!exp.status || exp.status != "launched"){
            wfcb(new Error("["+exp.id+"] Aborting deployment, status:"+exp.status));
         } else {
            getApplication(exp.app_id, function(error, app){
               if(error){
                  wfcb(error);
               } else {
                  wfcb(null, app, exp);
               }
            });
         }
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
      // Get instance
      function(app, exp, wfcb){
         _getInstance(minion, system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode);
            }
         });
      },
      // Get instance image
      function(app, exp, headnode, wfcb){
         _getImage(minion, headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode, image);
            }
         });
      },
      // Copy experiment in FS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Cloning experiment into instance");
         var cmd = "git clone -b "+exp.id+"-L "+exp.exp_url+" "+image.workpath+"/"+exp.id;
         // Execute command
         minionRPC.invoke('executeCommand', cmd, headnode.id, function (error, result, more) {
            if (error) {
               wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
            } else {
               wfcb(null, app, exp, headnode, image);
            }
         });
      },
      // Copy inputdata in FS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Making inputdata dir");
         var cmd = "mkdir -p "+image.inputpath+"/"+exp.id+"; rsync -Lr "+exp.input_url+"/* "+image.inputpath+"/"+exp.id;
         // Execute command
         minionRPC.invoke('executeCommand', cmd, headnode.id, function (error, result, more) {
            if (error) {
               wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
            } else {
               wfcb(null, app, exp, headnode, image);
            }
         });
      },
      // Init EXPERIMENT_STATUS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Initializing EXPERIMENT_STATUS");
         var cmd = 'echo -n "deployed" > '+image.workpath+'/'+exp.id+'/EXPERIMENT_STATUS';
         // Execute command
         minionRPC.invoke('executeCommand', cmd, headnode.id, function (error, result, more) {
            if (error) {
               wfcb(new Error("Failed to execute command:", cmd, "\nError: ", error));
            } else {
               wfcb(null, app, exp, headnode, image);
            }
         });
      },
      // Execute creation script
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Executing creation script");
         var work_dir = image.workpath+"/"+exp.id;
         var exe_script = ''+
         '#!/bin/sh \n'+
         'cd '+work_dir+'\n'+
         'echo -n "compiling" > EXPERIMENT_STATUS \n'+
         './'+app.creation_script+' &>COMPILATION_LOG \n'+
         'RETVAL=\$? \n'+
         'if [ \$RETVAL -eq 0 ]; then \n'+
         'echo -n "compiled" > EXPERIMENT_STATUS \n'+
         'else \n'+
         'echo -n "failed_compilation" > EXPERIMENT_STATUS \n'+
         'fi \n'+
         'echo -n \$RETVAL > COMPILATION_EXIT_CODE\n';

         // Execute command
         minionRPC.invoke('executeScript', exe_script, work_dir, headnode.id, 1, function (error, job_id, more) {
            if (error) {
               wfcb(new Error("Failed to execute script:\n", cmd, "\nError: ", error));
            } else {
               // Set experiment system and job_id
               db.collection('experiments').updateOne({id: exp.id},{$set:{system:system, job_id:job_id}})
               _waitExperimentCompilation(minion, exp.id, system, wfcb);
            }
         });
      },
   ],
   function(error){
      if(error){
         deployCallback(error);
      } else {
         console.log("["+exp_id+"] Deployed!");
         deployCallback(null);
      }
   });
}

var _waitExperimentCompilation = function(minion, exp_id, system, waitCallback){
   // Poll experiment
   _pollExperiment(minion, exp_id, system, function(error, status){
      if(error){
         waitCallback(new Error("Failed to poll experiment ", exp_id, ", err: ", error));
         return;
      }

      // Check status
      if(status == "launched" || status == "deployed" || status == "compiling"){
         // Recheck later
         setTimeout(_waitExperimentCompilation, 5000, minion, exp_id, system, waitCallback);
      } else if(status == "compiled") {
         // Compilation successful
         console.log("["+exp_id+"] Compilation succeed");
         waitCallback(null);
      } else {
         // Compilation failed
         waitCallback(new Error("["+exp_id+"] Compilation failed, status: " + status));
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

   // Get minion RPC
   var minionRPC = minions[minion];

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get application
      function(exp, wfcb){
         // First, check experiment status
         if(!exp.status || exp.status != "compiled"){
            wfcb(new Error("Failed to execute experiment ", exp.id, ", Invalid status: ", exp.status));
         } else {
            // Now, get application
            getApplication(exp.app_id, function(error, app){
               if(error){
                  wfcb(error);
               } else {
                  wfcb(null, app, exp);
               }
            });
         }
      },
      // Get instance
      function(app, exp, wfcb){
         _getInstance(minion, system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode);
            }
         });
      },
      // Get instance image
      function(app, exp, headnode, wfcb){
         _getImage(minion, headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode, image);
            }
         });
      },
      // Execute excution script
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp_id+"] Launching execution script");
         var work_dir = image.workpath+"/"+exp.id;
         var exe_script = ''+
         '#!/bin/sh \n'+
         'cd '+work_dir+'\n'+
         'echo -n "compiling" > EXPERIMENT_STATUS \n'+
         './'+app.execution_script+' &>EXECUTION_LOG \n'+
         'RETVAL=\$? \n'+
         'if [ \$RETVAL -eq 0 ]; then \n'+
         'echo -n "done" > EXPERIMENT_STATUS \n'+
         'else \n'+
         'echo -n "failed_execution" > EXPERIMENT_STATUS \n'+
         'fi \n'+
         'echo -n \$RETVAL > EXECUTION_EXIT_CODE\n';

         // Execute command
         minionRPC.invoke('executeScript', exe_script, work_dir, headnode.id, system.nodes, function (error, job_id, more) {
            if (error) {
               wfcb(new Error("Failed to execute script:\n", cmd, "\nError: ", error));
            } else {
               // Set script jobid
               db.collection('experiments').updateOne({id: exp.id},{$set:{system:system, job_id:job_id}})
               _waitExperimentExecution(minion, exp.id, system, wfcb);
            }
         });
      }
   ],
   function(error){
      if(error){
         executionCallback(error);
      } else {
         console.log("["+exp_id+"] Executed!");
         executionCallback(null);
      }
   });
}

var _waitExperimentExecution = function(minion, exp_id, system, waitCallback){
   // Poll experiment
   _pollExperiment(minion, exp_id, system, function(error, status){
      if(error){
         waitCallback(new Error("Failed to poll experiment ", exp_id, ", err: ", error));
         return;
      }

      // Check status
      if(status == "compiled" || status == "executing"){
         // Recheck later
         setTimeout(_waitExperimentExecution, 5000, minion, exp_id, system, waitCallback);
      } else if(status == "done") {
         // Execution successful
         waitCallback(null);
      } else {
         // Execution failed
         waitCallback(new Error("Experiment " + exp_id + " failed to execute, status: " + status));
      }
   });
}

/**
 * Retrieve experiment output data
 */
var _retrieveExperimentOutput = function(minion, exp_id, system, retrieveCallback){
   if(system.status != "instanced"){
      retrieveCallback(new Error("Target system is not instanced"));
      return;
   }

   // Get minion RPC
   var minionRPC = minions[minion];

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         _getInstance(minion, system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode);
            }
         });
      },
      // Get instance image
      function(exp, headnode, wfcb){
         _getImage(minion, headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Get instance hostname
      function(exp, headnode, image, wfcb){
         console.log("["+exp_id+"] Getting instance hostname");
         minionRPC.invoke('getInstanceHostname', headnode.id, function (error, hostname, more) {
            if (error) {
               wfcb(new Error("Failed to get instance hostname, error: ", error));
            } else {
               wfcb(null, exp, headnode, image, hostname);
            }
         });
      },
      // Execute excution script
      function(exp, headnode, image, hostname, wfcb){
         console.log("["+exp_id+"] Getting experiment output data path");
         var output_file = image.workpath+"/"+exp.id+"/output.tar.gz";
         var net_path = hostname + ":" + output_file;

         // Execute command
         storageClient.invoke('retrieveExperimentOutput', exp.id, net_path, function (error) {
            if (error) {
               wfcb(new Error("Failed to retrieve experiment output data, error: ", error));
            } else {
               wfcb(null);
            }
         });
      }
   ],
   function(error){
      if(error){
         retrieveCallback(error);
      } else {
         console.log("["+exp_id+"] Output data retrievement succeed");
         retrieveCallback(null);
      }
   });
}

var _pollExperiment = function(minion, exp_id, system, pollCallback){

   // Get minion RPC
   var minionRPC = minions[minion];

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         _getInstance(minion, system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode);
            }
         });
      },
      // Get instance image
      function(exp, headnode, wfcb){
         _getImage(minion, headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Poll experiment logs
      function(exp, headnode, image, wfcb){
         _pollExperimentLogs(minion, exp.id, system, image, ['COMPILATION_LOG','EXECUTION_LOG'], function (error, logs) {
            if(error){
               wfcb(new Error("Failed to poll experiment ", exp.id, " logs, error: ", error));
            } else {
               // Update status
               db.collection('experiments').updateOne({id: exp.id},{$set:{logs:logs}});

               // Callback status
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Poll experiment status
      function(exp, headnode, image, wfcb){
         var work_dir = image.workpath+"/"+exp.id;
         var cmd = 'cat '+work_dir+'/EXPERIMENT_STATUS';
         minionRPC.invoke('executeCommand', cmd, system.master, function (error, status, more) {
            if(error){
               wfcb(new Error("Failed to poll experiment ", exp.id, ", err: ", error));
            } else {
               // Update status
               db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

               // Callback status
               wfcb(null, status);
            }
         });
      }
   ],
   function(error, status){
      if(error){
         pollCallback(error);
      } else {
         pollCallback(null, status);
      }
   });
}

var _pollExperimentLogs = function(minion, exp_id, system, image, log_files, pollCallback){
   // Get minion RPC
   var minionRPC = minions[minion];

   var work_dir = image.workpath+"/"+exp_id;
   var logs = [];

   // Iterate logs
   var tasks = [];
   for(var i = 0; i < log_files.length; i++){
      (function(i){
         tasks.push(function(taskcb){
            var cmd = 'cat '+work_dir+'/'+log_files[i];
            minionRPC.invoke('executeCommand', cmd, system.master, function (error, content, more) {
               if(error){
                  taskcb(new Error("Failed to poll log ", log_files[i], ", error: ", error));
               } else {
                  // Add log
                  logs.push({name: log_files[i], content: content});
                  taskcb(null);
               }
            });
         });
      })(i);
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error){
         pollCallback(error);
      } else {
         // Callback
         pollCallback(null, logs);
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
   // Get minion RPC
   var minionRPC = minions[minion];

   // Select first image
   minionRPC.invoke('getImages', image_id, function (error, images, more) {
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
   // Get minion RPC
   var minionRPC = minions[minion];

   // Select sizes with this parameters
   minionRPC.invoke('getSizes', size_id, function (error, sizes, more) {
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

/**
 * Get instance
 */
var _getInstance = function(minion, instance_id, cb){
   // Get minion RPC
   var minionRPC = minions[minion];

   // Select sizes with this parameters
   minionRPC.invoke('getInstances', instance_id, function (error, instances, more) {
      if(error){
         cb(error);
      } else {
         if(instances instanceof Array){
            cb(new Error("Instance " + instance_id + " not found"));
         } else {
            cb(null, instances);
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
exports.destroyExperiment = destroyExperiment;
exports.searchExperiments = searchExperiments;

exports.getExperimentOutputFile = getExperimentOutputFile;
