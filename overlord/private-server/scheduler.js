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
var resetExperiment = function(exp_id, resetCallback){
   // Abort all tasks
   taskmanager.abortQueue(exp_id);

   // Add reset task
   var task = {
      type: "resetExperiment",
      exp_id: exp_id
   };
   taskmanager.pushTask(task);

   return resetCallback(null);
}

var _resetExperiment = function(exp_id, task, resetCallback){
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
            var job_id = null;
            if(task) job_id = task.job_id;
            // Experiment will be removed completely from the instance
            instmanager.cleanExperimentSystem(exp_id, job_id, exp.system, true, true, true, true, function(error){
               if (error) {
                  console.log('['+exp_id+'] Reset: Failed to clean experiment, error: '+error);
               }
               wfcb(null, exp);
            });
         } else {
            wfcb(null, exp);
         }
      },
      // Clean system
      function(exp, wfcb){
         if(exp.system){
            console.log("["+exp_id+"] Reset: cleaning system");
            instmanager.cleanSystem(exp.system, function(error){
               if (error) {
                  console.log('['+exp.id+'] Reset: Failed to clean system, error: '+error);
               }
               wfcb(error, exp);
            });
         } else {
            wfcb(null, exp);
         }
      },
   ],
   function(error, exp){
      if(error){
         // Error trying to reset experiment
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"reset_failed"}});
         resetCallback(error);
      } else {
         // Update status
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", system: exp.system}});
         console.log("["+exp_id+"] Reset: done");

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
         _resetExperiment(exp_id, null, wfcb);
      },
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Clean system
      function(exp, wfcb){
         if(exp.system){
            instmanager.cleanExperimentSystem(exp_id, null, exp.system, true, true, true, true, function(error, system){
               wfcb(error, exp);
            });
         } else {
            wfcb(null, exp);
         }
      },
      // Destroy system
      function(exp, wfcb){
         if(exp.system){
            instmanager.cleanSystem(exp.system, function(error, system){
               wfcb(error);
            });
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

   getExperiment(exp_id, null, function(error, exp){
      // Execute operations on experiment
      async.waterfall([
         // First, define a system where execution will take place
         function(wfcb){
            console.log('['+exp_id+'] Workflow: Defining system...');
            instmanager.defineSystem(nodes, image_id, size_id, wfcb);
         },
         // Instance system
         function(system, wfcb){
            console.log('['+exp_id+'] Workflow: Instancing system...');
            instmanager.instanceSystem(system, function(error){
               wfcb(error, system);
            });
         },
         // Add experiment to instances
         function(system, wfcb){
            for(var i = 0; i < system.nodes; i++){
               console.log('['+exp_id+'] Workflow: Adding experiment to instance "'+system.instances[i]+'"...');
               // Add experiment to instance
               instmanager.addExperiment(exp_id, system.instances[i]);
            }
            wfcb(null, system);
         },
         // Prepare experiment for the system
         function(system, wfcb){
            console.log("["+exp_id+"] Workflow: Launching preparing task...");

            // Add prepare task
            var task = {
               type: "prepareExperiment",
               exp_id: exp_id,
               system: system
            };
            taskmanager.pushTask(task, exp_id);

            // Add deploy task
            var task = {
               type: "deployExperiment",
               exp_id: exp_id,
               system: system
            };
            taskmanager.pushTask(task, exp_id);

            // Add compilation task
            var task = {
               type: "compileExperiment",
               exp_id: exp_id,
               system: system
            };
            taskmanager.pushTask(task, exp_id);

            // Add execution task
            var task = {
               type: "executeExperiment",
               exp_id: exp_id,
               system: system
            };
            taskmanager.pushTask(task, exp_id);

            // Add retrieve task
            var task = {
               type: "retrieveExperimentOutput",
               exp_id: exp_id,
               system: system
            };
            taskmanager.pushTask(task, exp_id);

            wfcb(null);
         }
      ],
      function(error){
         if(error) console.log("["+exp_id+"] Workflow: Failed, error: " + error);
      });
   });
}

/**
 * Prepare an experiment to be deployed.
 * Labels will be applied.
 */
var _prepareExperiment = function(task, exp_id, system, prepareCallback){
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
var _deployExperiment = function(task, exp_id, system, deployCallback){
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
         instmanager.getInstance(system.instances[0], function(error, headnode){
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
      // Abort previous job
      function(app, exp, headnode, image, wfcb){
         if(task.job_id){
            console.log("["+exp.id+"] Aborting previous jobs...");
            instmanager.abortJob(task.job_id, headnode.id, function(error){
               if(error) {return wfcb(error);}
               wfcb(null, app, exp, headnode, image);
            });
         } else {
            wfcb(null, app, exp, headnode, image);
         }
      },
      // Copy experiment in FS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Cloning experiment into instance");
         var cmd = "git clone -b "+exp.id+"-L "+exp.exp_url+" "+image.workpath+"/"+exp.id;
         // Execute command
         instmanager.executeJob(headnode.id, cmd, image.workpath, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               wfcb(null, app, exp, headnode, image);
            });
         });
      },
      // Copy inputdata in FS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Making inputdata dir");
         var cmd = "mkdir -p "+image.inputpath+"/"+exp.id+"; rsync -Lr "+exp.input_url+"/* "+image.inputpath+"/"+exp.id;
         // Execute command
         instmanager.executeJob(headnode.id, cmd, image.workpath, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               wfcb(null, app, exp, headnode, image);
            });
         });
      },
      // Init EXPERIMENT_STATUS
      function(app, exp, headnode, image, wfcb){
         console.log("["+exp.id+"] Initializing EXPERIMENT_STATUS");
         var cmd = 'echo -n "deployed" > '+image.workpath+'/'+exp.id+'/EXPERIMENT_STATUS';
         // Execute command
         instmanager.executeJob(headnode.id, cmd, image.workpath, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               wfcb(null, headnode);
            });
         });
      },
   ],
   function(error, headnode){
      if(error){return deployCallback(error);}

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         // Update status if the file exists
         if(status != ""){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:status}});
         }
      });

      console.log("["+exp_id+"] Deployed!");
      deployCallback(null);
   });
}

var _compileExperiment = function(task, exp_id, system, compileCallback){
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
      // Get instance
      function(app, exp, wfcb){
         instmanager.getInstance(system.instances[0], function(error, headnode){
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
      // Execute compilation script
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

         // Check if already compiling
         if(task.job_id){
            // Already compiling
            wfcb(true, headnode);
         } else {
            // Execute job
            instmanager.executeJob(headnode.id, exe_script, work_dir, 1, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               console.log("["+exp_id+"] Compiling...");
               wfcb(null, headnode);
            });
         }
      },
   ],
   function(error, headnode){
      if(error && error != true){ return compileCallback(error);}

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         // Update status if the file exists
         if(status != ""){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:status}});
         }
      });

      // Wait for command completion
      instmanager.waitJob(task.job_id, headnode.id, function(error){
         if(error){return compileCallback(error);}

         // Update task and DB
         task.job_id = null;
         database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

         // Poll experiment status
         _pollExperiment(exp_id, system, function(error, status){
            // Update status if the file exists
            if(status != ""){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:status}});
            }
         });

         // End compile task
         console.log("["+exp_id+"] Compiled!");
         compileCallback(null);
      });
   });
}

/**
 * Execute an experiment in target system
 */
var _executeExperiment = function(task, exp_id, system, executionCallback){

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
      // Get instance
      function(app, exp, wfcb){
         instmanager.getInstance(system.instances[0], function(error, headnode){
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

         // Check if already executing
         if(task.job_id){
            // Already executing
            wfcb(true, headnode);
         } else {
            // Execute job
            instmanager.executeJob(headnode.id, exe_script, work_dir, system.nodes, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               console.log("["+exp_id+"] Executing...");
               wfcb(null, headnode);
            });
         }
      }
   ],
   function(error, headnode){
      if(error && error != true){return executionCallback(error);}

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         // Update status if the file exists
         if(status != ""){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:status}});
         }
      });

      // Wait for command completion
      instmanager.waitJob(task.job_id, headnode.id, function(error){
         if(error){return executionCallback(error);}

         // Update task and DB
         task.job_id = null;
         database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

         // Poll experiment status
         _pollExperiment(exp_id, system, function(error, status){
            // Update status if the file exists
            if(status != ""){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:status}});
            }
         });

         // End compile task
         console.log("["+exp_id+"] Executed!");
         executionCallback(null);
      });
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
         instmanager.getInstance(system.instances[0], function(error, headnode){
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
         instmanager.getInstance(system.instances[0], function(error, headnode){
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
         instmanager.executeCommand(system.instances[0], cmd, function (error, status) {
            if(error){
               wfcb(error);
            } else {
               // Update status if the file exists
               if(status != ""){
                  database.db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});
               }

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
            instmanager.executeCommand(system.instances[0], cmd, function (error, content) {
               if(error){
                  taskcb(new Error("Failed to poll log "+ log_files[i]+ ", error: "+ error));
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
   _prepareExperiment(task, exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] prepareExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);
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
   _deployExperiment(task, exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] deployExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean system
         instmanager.cleanExperimentSystem(exp_id, task.job_id, system, true, true, true, true, function(error, system){
            if(error) console.error("["+exp_id+"] deployExperiment clean system error: "+error);
            instmanager.cleanSystem(system, function(error, system){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
               if(error) console.error("["+exp_id+"] deployExperiment clean error: "+error);
            });
         });
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);
   });
});

/**
 * Deploy experiment handler
 */
taskmanager.setTaskHandler("compileExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _compileExperiment(task, exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] compileExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);
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
   _executeExperiment(task, exp_id, system, function(error){
      if(error){
         console.error("["+exp_id+"] executeExperiment error: "+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean system
         instmanager.cleanExperimentSystem(exp_id, task.job_id, system, true, true, true, true, function(error, system){
            if(error) console.error("["+exp_id+"] executeExperiment error: "+error);
            instmanager.cleanSystem(system, function(error, system){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
               if(error) console.error("["+exp_id+"] executeExperiment error: "+error);
            });
         });
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id);
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

      // Clean system
      instmanager.cleanExperimentSystem(exp_id, task.job_id, system, true, true, true, true, function(error, system){
         if(error) console.error("["+exp_id+"] retrieveExperimentOutput error: "+error);
         instmanager.cleanSystem(system, function(error, system){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
            if(error) console.error("["+exp_id+"] retrieveExperimentOutput error: "+error);
         });
      });
   });
});

/**
 * Reset experiment handler
 */
taskmanager.setTaskHandler("resetExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Prepare experiment
   _resetExperiment(exp_id, task, function(error){
      if(error){
         console.error("["+exp_id+"] resetExperiment error: "+error);
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
