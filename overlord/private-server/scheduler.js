var zerorpc = require('zerorpc');
var async = require('async');
var constants = require('./constants.json');
var codes = require('./error_codes.js');

/**
 * Load submodules
 */
var apps = require('./application.js');
var exps = require('./experiment.js');
var database = require('./database.js');
var storage = require('./storage.js');
var instmanager = require('./instance.js');
var taskmanager = require('./task.js');

/**
 * Module name
 */
var MODULE_NAME = "SC";


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
   storage.client.invoke("getExperimentOutputFile", exp_id, function(error, file){
      if (error) {
         getCallback(new Error("Failed to get experiment "+exp_id+" output data, error: "+error));
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
   // Check data
   async.waterfall([
      // Check experiment status
      function(wfcb){
         // Check image ID exists
         instmanager.getImage(image_id, wfcb);
      },
      // Check size ID exists
      function(image, wfcb){
         instmanager.getSize(size_id, wfcb);
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
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"launched"}});

         // Callback
         launchCallback(null);

         // Launch workflow
         _workflowExperiment(exp_id, nodes, image_id, size_id);
      }
   });
}

/**
 * Reset experiment to create status
 */
var resetExperiment = function(exp_id, hardreset, resetCallback){
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Update database
      function(exp, wfcb){
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"resetting"}});
         wfcb(null, exp);
      },
      // Clean job
      function(exp, wfcb){
         if(exp.system){
            console.log("["+exp_id+"] Reset: cleaning experiment");
            // If hardreset is set, experiment will be removed completely from the instance
            instmanager.cleanExperiment(exp_id, exp.system.master, true, true, false, hardreset, function(error){
               if (error) {
                  console.log('['+exp_id+'] Reset: Failed to clean experiment, error: '+error);
                  // If cleaning failed, better to do a hard reset
                  hardreset = true;
               }
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      },
   ],
   function(error){
      if(error){
         // Error trying to reset experiment
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"reset_failed"}});
         resetCallback(error);
      } else {
         // Update status
         if(hardreset){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", system:""}});
            console.log("["+exp_id+"] Reset: HARD reset done");
         } else {
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created"}});
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
         database.db.collection('experiments').remove({id: exp_id});
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

var _workflowExperiment = function(exp_id, nodes, image_id, size_id){

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
            _defineSystem(nodes, image_id, size_id, wfcb);
         },
         // Instance system
         function(system, wfcb){
            console.log("-----------------------\n",
                        "System configured with:\n",
                        "-- Image: ", system.image.name, " - ", system.image.id, "\n",
                        "-- Size: ", system.size.name, " - ", system.size.id, "\n");
                        _system = system;
                        _instanceSystem(system, wfcb);
         },
         // Show system info
         function(wfcb){
            msg = "-----------------------\nSystem instances:\n";
            for(i = 0; i < _system.nodes; i++){
               msg += "-- " + i + ": " + _system.instances[i] + "\n";
            }
            console.log(msg);
            wfcb(null);
         },
         // Prepare experiment for the system
         function(wfcb){
            console.log("["+exp_id+"] Workflow: Preparing...");

            // Add task
            var task = {
               type: "prepareExperiment",
               exp_id: exp_id,
               system: _system
            };
            taskmanager.pushTask(task);
            wfcb(null);
         }
      ],
      function(error){
         if(error) console.log("["+exp_id+"] Workflow: Failed, error: " + error);
      });
   });
}

/**
 * Define a system of instances
 */
var _defineSystem = function(nodes, image_id, size_id, defineCallback){
   // Get image and size IDs for the system
   instmanager.getImage(image_id, function(error, image){
      if(error){
         defineCallback(error);
         return;
      }
      instmanager.getSize(size_id, function(error, size){
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

var _instanceSystem = function(system, instanceCallback){
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
         instmanager.requestInstance(system.image.id, system.size.id, function (error, inst_id) {
            if(error){
               taskcb(error);
            } else {
               // Add instance to system
               system.instances.push(inst_id);
               taskcb(null);
            }
         });
      });
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error){
         instanceCallback(error);
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
   if(!system.instances || system.instances.length == 0){
      destroyCallback(null);
      return;
   }

   // Destroy instances
   var tasks = [];
   for(var inst in system.instances){
      // inst must be task independent
      (function(inst){
         tasks.push(function(taskcb){
            console.log("Instance: " + inst + " / " + system.instances[inst]);
            if(system.instances[inst]){
               console.log("["+system.instances[inst]+"] Destroying instance...");
               instmanager.destroyInstance(system.instances[inst], function (error) {
                  if(error){
                     taskcb(error);
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
         storage.client.invoke('prepareExperiment', app.id, exp.id, exp.labels, function(error){
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
var _deployExperiment = function(exp_id, system, deployCallback){
   // Check the system is instanced
   if(system.status != "instanced"){
      deployCallback(new Error("Target system is not instanced"));
      return;
   }

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Check if already deployed
      function(exp, wfcb){
         if(exp.status && exp.status == "compiling"){
            // Already deployed
            wfcb(true);
         } else {
            wfcb(null, exp);
         }
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
         storage.client.invoke('getApplicationURL', app.id, function(error, url){
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
         storage.client.invoke('getExperimentInputURL', exp_id, function(error, url){
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
         instmanager.getInstance(system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode);
            }
         });
      },
      // Get instance image
      function(app, exp, headnode, wfcb){
         instmanager.getImage(headnode.image_id, function(error, image){
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
         instmanager.executeCommand(headnode.id, cmd, function (error, result) {
            if (error) {
               wfcb(error);
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
         instmanager.executeCommand(headnode.id, cmd, function (error, result) {
            if (error) {
               wfcb(error);
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
         instmanager.executeCommand(headnode.id, cmd, function (error, result) {
            if (error) {
               wfcb(error);
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

         // Execute job
         instmanager.executeJob(headnode.id, exp.id, exe_script, work_dir, 1, function (error) {
            if (error) {
               wfcb(error);
            } else {
               // Set experiment system
               database.db.collection('experiments').updateOne({id: exp.id},{$set:{system:system}})
               console.log("["+exp_id+"] Deployed!");
               wfcb(null);
            }
         });
      },
   ],
   function(error){
      if(error && error != true){
         deployCallback(error);
      }
      // Wait experiment for compilation
      _waitExperimentCompilation(exp_id, system, deployCallback);
   });
}

var _waitExperimentCompilation = function(exp_id, system, waitCallback){
   // Poll experiment
   _pollExperiment(exp_id, system, function(error, status){
      if(error){
         waitCallback(new Error("Failed to poll experiment ", exp_id, ", err: ", error));
         return;
      }

      // Check status
      if(status == "launched" || status == "deployed" || status == "compiling"){
         // Recheck later
         setTimeout(_waitExperimentCompilation, 5000, exp_id, system, waitCallback);
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
var _executeExperiment = function(exp_id, system, executionCallback){
   if(system.status != "instanced"){
      executionCallback(new Error("Target system is not instanced"));
      return;
   }

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Check if already executed
      function(exp, wfcb){
         if(exp.status && exp.status == "executing"){
            // Already executing
            wfcb(true);
         } else {
            wfcb(null, exp);
         }
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
         instmanager.getInstance(system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, headnode);
            }
         });
      },
      // Get instance image
      function(app, exp, headnode, wfcb){
         instmanager.getImage(headnode.image_id, function(error, image){
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
         'echo -n "executing" > EXPERIMENT_STATUS \n'+
         './'+app.execution_script+' &>EXECUTION_LOG \n'+
         'RETVAL=\$? \n'+
         'if [ \$RETVAL -eq 0 ]; then \n'+
         'echo -n "done" > EXPERIMENT_STATUS \n'+
         'else \n'+
         'echo -n "failed_execution" > EXPERIMENT_STATUS \n'+
         'fi \n'+
         'echo -n \$RETVAL > EXECUTION_EXIT_CODE\n';

         // Execute command
         instmanager.executeJob(headnode.id, exp.id, exe_script, work_dir, system.nodes, function (error) {
            if (error) {
               wfcb(error);
            } else {
               console.log("["+exp_id+"] Executed!");
               wfcb(null);
            }
         });
      }
   ],
   function(error){
      if(error && error != true){
         executionCallback(error);
      }
      // Wait experiment for execution
      _waitExperimentExecution(exp_id, system, executionCallback);
   });
}

var _waitExperimentExecution = function(exp_id, system, waitCallback){
   // Poll experiment
   _pollExperiment(exp_id, system, function(error, status){
      if(error){
         waitCallback(new Error("Failed to poll experiment ", exp_id, ", err: ", error));
         return;
      }

      // Check status
      if(status == "compiled" || status == "executing"){
         // Recheck later
         setTimeout(_waitExperimentExecution, 5000, exp_id, system, waitCallback);
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
var _retrieveExperimentOutput = function(exp_id, system, retrieveCallback){
   if(system.status != "instanced"){
      retrieveCallback(new Error("Target system is not instanced"));
      return;
   }

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         instmanager.getInstance(system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode);
            }
         });
      },
      // Get instance image
      function(exp, headnode, wfcb){
         instmanager.getImage(headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Execute excution script
      function(exp, headnode, image, wfcb){
         console.log("["+exp_id+"] Getting experiment output data path");
         var output_file = image.workpath+"/"+exp.id+"/output.tar.gz";
         var net_path = headnode.hostname + ":" + output_file;

         // Execute command
         storage.client.invoke('retrieveExperimentOutput', exp.id, net_path, function (error) {
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

var _pollExperiment = function(exp_id, system, pollCallback){

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         instmanager.getInstance(system.master, function(error, headnode){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode);
            }
         });
      },
      // Get instance image
      function(exp, headnode, wfcb){
         instmanager.getImage(headnode.image_id, function(error, image){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Poll experiment logs
      function(exp, headnode, image, wfcb){
         _pollExperimentLogs(exp.id, system, image, ['COMPILATION_LOG','EXECUTION_LOG'], function (error, logs) {
            if(error){
               wfcb(error);
            } else {
               // Update status
               database.db.collection('experiments').updateOne({id: exp.id},{$set:{logs:logs}});

               // Callback status
               wfcb(null, exp, headnode, image);
            }
         });
      },
      // Poll experiment status
      function(exp, headnode, image, wfcb){
         var work_dir = image.workpath+"/"+exp.id;
         var cmd = 'cat '+work_dir+'/EXPERIMENT_STATUS';
         instmanager.executeCommand(system.master, cmd, function (error, status) {
            if(error){
               wfcb(error);
            } else {
               // Update status
               database.db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});

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

var _pollExperimentLogs = function(exp_id, system, image, log_files, pollCallback){
   // Working directory
   var work_dir = image.workpath+"/"+exp_id;
   var logs = [];

   // Iterate logs
   var tasks = [];
   for(var i = 0; i < log_files.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var cmd = 'cat '+work_dir+'/'+log_files[i];
            instmanager.executeCommand(system.master, cmd, function (error, content) {
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
 * Prepare experiment handler
 */
taskmanager.setTaskHandler("prepareExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _prepareExperiment(exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] prepareExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);

      // Add deploy task
      var task = {
         type: "deployExperiment",
         exp_id: exp_id,
         system: system
      };
      taskmanager.pushTask(task);
   });
});

/**
 * Deploy experiment handler
 */
taskmanager.setTaskHandler("deployExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _deployExperiment(exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] deployExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);

      // Add deploy task
      var task = {
         type: "executeExperiment",
         exp_id: exp_id,
         system: system
      };
      taskmanager.pushTask(task);
   });
});

/**
 * Execute experiment handler
 */
taskmanager.setTaskHandler("executeExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _executeExperiment(exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] executeExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);

      // Add deploy task
      var task = {
         type: "retrieveExperimentOutput",
         exp_id: exp_id,
         system: system
      };
      taskmanager.pushTask(task);
   });
});

/**
 * Retrieve data handler
 */
taskmanager.setTaskHandler("retrieveExperimentOutput", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _retrieveExperimentOutput(exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] retrieveExperimentOutput error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);
   });
});

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
