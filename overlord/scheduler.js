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
 * Module vars
 */
var MODULE_NAME = "SC";
var pollInterval = 30000;

/***********************************************************
 * --------------------------------------------------------
 * APPLICATION METHODS
 * --------------------------------------------------------
 ***********************************************************/

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

/***********************************************************
 * --------------------------------------------------------
 * EXPERIMENT HANDLING METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get experiment metadata
 */
var getExperiment = function(exp_id, fields, getCallback){
   exps.getExperiment(exp_id, fields, getCallback);
}

/**
 * Get experiment source file
 */
var getExperimentCode = function(exp_id, fpath, getCallback){
   getExperiment(exp_id, null, function(error, exp){
      if(error) return getCallback(error);

      // Retrieve file contents
      storage.client.invoke('getExperimentCode', exp_id, exp.app_id, fpath, function(error, fcontent){
         if(error){
            getCallback(error);
         } else {
            getCallback(null, fcontent);
         }
      });
   })
}

/**
 * Save experiment source file
 */
var putExperimentCode = function(exp_id, fpath, fcontent, putCallback){
   getExperiment(exp_id, null, function(error, exp){
      if(error) return putCallback(error);

      // Save file contents
      storage.client.invoke('putExperimentCode', exp_id, exp.app_id, fpath, fcontent, function(error){
         if(error) return putCallback(error);
         putCallback(null);
      });
   })
}

/**
 * Save experiment input file
 */
var putExperimentInput = function(exp_id, fpath, src_file, putCallback){
   getExperiment(exp_id, null, function(error, exp){
      if(error) return putCallback(error);

      // Get remote path
      var src_path = constants.OVERLORD_USERNAME+'@'+constants.OVERLORD_IP+':'+src_file;

      // Save file contents
      storage.client.invoke('putExperimentInput', exp_id, exp.app_id, fpath, src_path, function(error){
         if(error) return putCallback(error);
         putCallback(null);
      });
   })
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
 * Reset experiment to "create" status
 */
var resetExperiment = function(exp_id, resetCallback){
   // Update status
   database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"resetting"}});

   // First get experiment
   getExperiment(exp_id, null, function(error, exp){
      if(error) return resetCallback(error);

      // Abort all tasks
      taskmanager.abortQueue(exp_id, function(task){
         if(task && task.job_id && exp.system && exp.system.instances[0]){
            instmanager.abortJob(task.job_id, exp.system.instances[0], function(error){
               console.error(error);
            });
         }
      }, function(error){
         // All task aborted and finished
         if(error) {
            console.error(error);
            return;
         }

         // Add reset task
         var task = {
            type: "resetExperiment",
            exp_id: exp_id
         };
         taskmanager.pushTask(task, exp_id);
      });

      // Callback
      resetCallback(null);
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
            instmanager.cleanExperimentSystem(exp_id, exp.system, true, true, true, true, function(error, system){
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
      // Remove experiment data from storage
      function(wfcb){
         storage.client.invoke('removeExperimentData', exp_id, function (error) {
            if(error) return wfcb(error);
            wfcb(null);
         });
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

/**
 * Reload experiments' file tree.
 */
var reloadExperimentTree = function(exp_id, reloadCallback){
   console.log("["+exp_id+"] Reloading trees...");
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Obtain experiment input data tree
      function(exp, wfcb){
         storage.client.invoke('getInputFolderTree', exp_id, function (error, tree) {
            if(error) return wfcb(error);

            database.db.collection('experiments').updateOne({id: exp_id},{$set: {input_tree: tree}});
            wfcb(null, exp);
         });
      },
      // Obtain experiment source code tree
      function(exp, wfcb){
         storage.client.invoke('getExperimentSrcFolderTree', exp_id, exp.app_id, function (error, tree) {
            if(error) return wfcb(error);

            database.db.collection('experiments').updateOne({id: exp_id},{$set: {src_tree: tree}});
            wfcb(null);
         });
      }
   ],
   function(error){
      if(error) return reloadCallback(error);
      console.log("["+exp_id+"] Trees reloaded");
      reloadCallback(error);
   });
}

/***********************************************************
 * --------------------------------------------------------
 * SCHEDULING EVENTS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Deploy a cluster
 */
taskmanager.setTaskHandler("instanceSystem", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var system = task.system;
   var task_id = task.id;

   // Define and create a system
   _instanceSystem(task, system, function(error, system){
      if(error){
         console.error('['+MODULE_NAME+']['+exp_id+'] instanceSystem error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Update DB
      database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});

      // Deploy task
      var next_task = {
         type: "prepareExperiment",
         exp_id: exp_id,
         system: system
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exp_id);
   });
});

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
         console.error('['+MODULE_NAME+']['+exp_id+'] prepareExperiment error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system, status: "failed_prepare"}});
         return;
      }

      // Deploy task
      var next_task = {
         type: "deployExperiment",
         exp_id: exp_id,
         system: system
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exp_id);
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
         console.error('['+MODULE_NAME+']['+exp_id+'] deployExperiment error: '+error.message);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean system
         instmanager.cleanExperimentSystem(exp_id, system, true, true, true, true, function(error, system){
            if(error) console.error("["+exp_id+"] deployExperiment clean system error: "+error);
            instmanager.cleanSystem(system, function(error, system){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system, status: "failed_deploy"}});
               if(error) console.error("["+exp_id+"] deployExperiment clean error: "+error);
            });
         });
         return;
      }

      // Compilation task
      var next_task = {
         type: "compileExperiment",
         exp_id: exp_id,
         system: system
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exp_id);
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
         console.error('['+MODULE_NAME+']['+exp_id+'] compileExperiment error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean system
         instmanager.cleanExperimentSystem(exp_id, system, true, true, true, true, function(error, system){
            if(error) console.error("["+exp_id+"] compileExperiment error: "+error);
            instmanager.cleanSystem(system, function(error, system){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
               if(error) console.error("["+exp_id+"] compileExperiment error: "+error);
            });
         });
         return;
      }

      // Execution task
      var next_task = {
         type: "executeExperiment",
         exp_id: exp_id,
         system: system
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exp_id);
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
         console.error('['+MODULE_NAME+']['+exp_id+'] executeExperiment error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean system
         instmanager.cleanExperimentSystem(exp_id, system, true, true, true, true, function(error, system){
            if(error) console.error("["+exp_id+"] executeExperiment error: "+error);
            instmanager.cleanSystem(system, function(error, system){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
               if(error) console.error("["+exp_id+"] executeExperiment error: "+error);
            });
         });
         return;
      }

      // Retrieve task
      var next_task = {
         type: "retrieveExperimentOutput",
         exp_id: exp_id,
         system: system
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exp_id);
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
   _retrieveExperimentOutput(task, exp_id, system, function(error){
      if(error){
         console.error('['+MODULE_NAME+']['+exp_id+'] retrieveExperimentOutput error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id, null, null);

      // Clean system
      instmanager.cleanExperimentSystem(exp_id, system, true, true, true, true, function(error, system){
         if(error) console.error("["+exp_id+"] retrieveExperimentOutput error: "+error);
         instmanager.cleanSystem(system, function(error, system){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{system: system}});
            if(error) console.error("["+exp_id+"] retrieveExperimentOutput error: "+error);
         });
      });

      // Set experiment to done status
      database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "done"}});
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
         console.error('['+MODULE_NAME+']['+exp_id+'] resetExperiment error: '+error);
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id, null, null);
   });
});


/***********************************************************
 * --------------------------------------------------------
 * PRIVATE METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Initiates the event chain for experiment workflow execution
 */
var _workflowExperiment = function(exp_id, nodes, image_id, size_id){

   console.log("["+exp_id+"] Workflow: Begin");

   getExperiment(exp_id, null, function(error, exp){
      // Execute operations on experiment
      async.waterfall([
         // First, define a system where execution will take place
         function(wfcb){
            console.log('['+exp_id+'] Workflow: Defining system...');
            instmanager.defineSystem(nodes, image_id, size_id, exp.name, wfcb);
         },
         // Prepare experiment for the system
         function(system, wfcb){
            // Update DB
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{system:system}});

            console.log("["+exp_id+"] Workflow: Launching preparing task...");

            // Add prepare task
            var task = {
               type: "instanceSystem",
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
 * Instance a system to use with an experiment
 */
var _instanceSystem = function(task, system, createCallback){
   // Check if task failed
   console.log('['+MODULE_NAME+'] Instancing cluster...');
   instmanager.instanceSystem(system, function(error){
      if(error) return createCallback(error);

      // Check task abort
      if(taskmanager.isTaskAborted(task.id)){
         // Clean system
         return createCallback(new Error("Task aborted"));
      }

      console.log('['+MODULE_NAME+'] Cluster created.');
      createCallback(null, system);
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
      // Add experiment to instances
      function(app, exp, headnode, image, wfcb){
         if(!task.job_id){
            for(var i = 0; i < system.nodes; i++){
               console.log('['+MODULE_NAME+']['+exp_id+'] Adding experiment to instance "'+system.instances[i]+'"...');
               // Add experiment to instance
               instmanager.addExperiment(exp_id, system.instances[i]);
            }
         }
         wfcb(null, app, exp, headnode, image);
      },
      // Copy experiment in FS
      function(app, exp, headnode, image, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         console.log("["+exp.id+"] Cloning experiment into instance");
         var cmd = "mkdir -p "+image.workpath+"; git clone -b "+exp.id+"-L "+exp.exp_url+" "+image.workpath+"/"+exp.id;
         var work_dir = image.workpath + "/" + exp.id;

         // Execute command
         instmanager.executeJob(headnode.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, app, exp, headnode, image);
            });
         });
      },
      // Copy inputdata in FS
      function(app, exp, headnode, image, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         console.log("["+exp.id+"] Making inputdata dir");
         var cmd = "mkdir -p "+image.inputpath+"/"+exp.id+"; sshpass -p 'devstack' rsync -Lr "+exp.input_url+"/* "+image.inputpath+"/"+exp.id;
         var work_dir = image.workpath + "/" + exp.id;

         // Execute command
         instmanager.executeJob(headnode.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, app, exp, headnode, image);
            });
         });
      },
      // Init EXPERIMENT_STATUS
      function(app, exp, headnode, image, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         console.log("["+exp.id+"] Initializing EXPERIMENT_STATUS");
         var cmd = 'echo -n "deployed" > '+image.workpath+'/'+exp.id+'/EXPERIMENT_STATUS';
         var work_dir = image.workpath + "/" + exp.id;

         // Execute command
         instmanager.executeJob(headnode.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            instmanager.waitJob(job_id, headnode.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, headnode);
            });
         });
      },
   ],
   function(error, headnode){
      if(error){return deployCallback(error);}

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         // Check status
         if(status != "deployed"){
            return deployCallback(new Error("Failed to deploy experiment, status: "+status));
         }

         // Deployed
         console.log("["+exp_id+"] Deployed!");
         deployCallback(null);
      });
   });
}

/**
 * Launch compilation script of an experiment
 */
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
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

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
            console.log("["+exp.id+"] Executing compiling script");
            instmanager.executeJob(headnode.id, exe_script, work_dir, 1, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, headnode);
            });
         }
      },
   ],
   function(error, headnode){
      if(error && error != true){ return compileCallback(error);}

      console.log("["+exp_id+"] Compiling...");

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         if(error){return compileCallback(error);}

         // Wait for command completion
         instmanager.waitJob(task.job_id, headnode.id, function(error){
            if(error){return compileCallback(error);}

            // Poll experiment status
            _pollExperiment(exp_id, system, function(error, status){
               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return compileCallback(new Error("Task aborted"));}

               // Check status
               if(status != "compiled"){
                  return compileCallback(new Error("Failed to compile experiment, status: "+status));
               }

               // End compile task
               console.log("["+exp_id+"] Compiled!");
               compileCallback(null);
            });
         });
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
      // Execute execution script
      function(app, exp, headnode, image, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var work_dir = image.workpath+"/"+exp.id;
         var exe_script = ''+
         '#!/bin/sh \n'+
         'cd '+work_dir+'\n'+
         'echo -n "executing" > EXPERIMENT_STATUS \n'+
         './'+app.execution_script+' &>EXECUTION_LOG \n'+
         'RETVAL=\$? \n'+
         'if [ \$RETVAL -eq 0 ]; then \n'+
         'echo -n "executed" > EXPERIMENT_STATUS \n'+
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
            console.log("["+exp_id+"] Launching execution script");
            instmanager.executeJob(headnode.id, exe_script, work_dir, system.nodes, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, headnode);
            });
         }
      }
   ],
   function(error, headnode){
      if(error && error != true){return executionCallback(error);}

      console.log("["+exp_id+"] Executing...");

      // Poll experiment status
      _pollExperiment(exp_id, system, function(error, status){
         if(error){return executionCallback(error);}

         // Wait for command completion
         instmanager.waitJob(task.job_id, headnode.id, function(error){
            if(error){return executionCallback(error);}

            // Poll experiment status
            _pollExperiment(exp_id, system, function(error, status){
               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return executionCallback(new Error("Task aborted"));}

               // Check status
               if(status != "executed"){
                  return executionCallback(new Error("Failed to execute experiment, status: "+status));
               }

               // End execution task
               console.log("["+exp_id+"] Executed!");
               executionCallback(null);
            });
         });
      });
   });
}

/**
 * Retrieve experiment output data
 */
var _retrieveExperimentOutput = function(task, exp_id, system, retrieveCallback){
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
      // Get storage URL
      function(exp, headnode, image, wfcb){
         storage.client.invoke('getExperimentOutputURL', exp_id, function(error, url){
            if(error) return wfcb(error);
            wfcb(null, exp, headnode, image, url);
         });
      },
      // Execute excution script
      function(exp, headnode, image, url, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         console.log("["+exp_id+"] Getting experiment output data path");
         var output_file = image.workpath+"/"+exp.id+"/output.tar.gz";

         // Execute command
         var cmd = "sshpass -p 'devstack' scp -o StrictHostKeyChecking=no "+output_file+" "+url+"/";
         instmanager.executeCommand(headnode.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}
            wfcb(null);
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

/**
 * Resets an experiment
 */
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
            instmanager.cleanExperimentSystem(exp_id, exp.system, true, true, true, true, function(error){
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
 * Get experiment status from target system and update in DB.
 */
var _pollExperiment = function(exp_id, system, pollCallback){
   // Check system
   if(!system || !system.instances || system.instances.length == 0){
      // Nothing to poll
      getExperiment(exp_id, null, function(error, exp){
         if(error) return pollCallback(error);
         pollCallback(null, exp.status);
      });
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
      // Poll experiment logs
      function(exp, headnode, image, wfcb){
         _pollExperimentLogs(exp.id, headnode.id, image, ['COMPILATION_LOG','EXECUTION_LOG','*.log', '*.log.*', '*.bldlog.*'], function (error, logs) {
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
         instmanager.executeCommand(headnode.id, cmd, function (error, output) {
            if(error){
               wfcb(error);
            } else {
               // Get status
               var status = output.stdout;

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
      if(error) return pollCallback(error);
      pollCallback(null, status);
   });
}

/**
 * Cache to avoid polling again
 */
var _polling = {};
/**
 * Update experiment logs in DB.
 */
var _pollExperimentLogs = function(exp_id, inst_id, image, log_files, pollCallback){
   // Working directory
   var work_dir = image.workpath+"/"+exp_id;
   var logs = [];

   // Avoid multiple polling
   if(!_polling[exp_id]){
      _polling[exp_id] = true;

      // Get log files list
      _findExperimentLogs(exp_id, inst_id, image, log_files, function(error, loglist){
         if(error){
            _polling[exp_id] = false;
            return pollCallback(error);
         }

         // Iterate logs
         var tasks = [];
         for(var i = 0; i < loglist.length; i++){
            // var i must be independent between tasks
            (function(i){
               tasks.push(function(taskcb){
                  var cmd = 'zcat -f '+loglist[i];
                  instmanager.executeCommand(inst_id, cmd, function (error, output) {
                     if(error) return taskcb(new Error("Failed to poll log "+ loglist[i]+ ", error: "+ error));

                     // Get log content
                     var content = output.stdout;
                     var log_filename = loglist[i].split('\\').pop().split('/').pop();

                     // Add log
                     //console.log('['+MODULE_NAME+']['+exp_id+'] Updating log content: "'+log_filename);
                     logs.push({name: log_filename, content: content});
                     taskcb(null);
                  });
               });
            })(i);
         }

         // Execute tasks
         async.series(tasks, function(error){
            _polling[exp_id] = false;
            if(error) return pollCallback(error);
            // Sort
            logs.sort(function(a,b){return (a.name > b.name) ? 1 : ((b.name > a.name) ? -1 : 0);});
            pollCallback(null, logs);
         });
      });
   }
}

/**
 * Get log paths
 */
var _findExperimentLogs = function(exp_id, inst_id, image, log_files, findCallback){
   // No log files
   if(!log_files || log_files.length <= 0) return findCallback(new Error("No log files specified."));

   // Working directory
   var work_dir = image.workpath+"/"+exp_id;
   var logs = [];

   // Prepare command to find logs
   var cmd = 'find '+work_dir+ ' -name "'+log_files[0]+'"';
   for(var i = 1; i < log_files.length; i++){
      // Add option to find
      cmd = cmd + ' -o -name "'+log_files[i]+'"';
   }

   // Search logs in instance
   instmanager.executeCommand(inst_id, cmd, function (error, output) {
      if(error) return findCallback(new Error("Failed to poll log "+ log_files[i]+ ", error: "+ error));

      // Get logs path, filter empty
      var loglist = output.stdout.split("\n");
      loglist = loglist.filter(function(elem){ return elem && elem != "" });

      // Add logs
      logs = logs.concat(loglist);
      findCallback(null, logs);
   });
}

/**
 * Remove invalid status experiments in instances
 */
var _cleanInstances = function(){
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_cleanInstances, 1000);
   }

   // Iterate instances
   // TODO: Move this functionality to instance manager
   database.db.collection('instances').find().forEach(function(inst){
      // Iterate experiments
      var exps = inst.exps;
      if(exps){
         for(var i = 0; i < exps.length; i++){
            var exp_id = exps[i].exp_id;
            (function(exp_id){
               getExperiment(exp_id, null, function(error, exp){
                  if(!exp || exp.status == "created" || exp.status == "done" || exp.status == "failed_compilation" || exp.status == "failed_execution"){
                     // Remove experiment from this instance
                     instmanager.cleanExperiment(exp_id, inst.id, true, true, true, true, function(error){
                        if(error) console.error("["+exp_id+"] Failed to clean experiment from instance '"+inst.id+"'");
                        console.log("["+exp_id+"] Cleaned experiment from instance '"+inst.id+"'");
                     });
                  }
               });
            })(exp_id);
         }
      }
   });
}

/**
 * Poll experiments
 */
var _pollExecutingExperiments = function(){
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_cleanInstances, 1000);
   }

   // Iterate experiments
   database.db.collection('experiments').find({
      status: { $in: ["deployed", "compiling", "executing"]}
   }).forEach(function(exp){
      // Poll experiment status
      _pollExperiment(exp.id, exp.system, function(error, status){
         if(error) console.error("["+MODULE_NAME+"] Failed to automatic poll: "+error);
      });
   });
}

/***********************************************************
 * --------------------------------------------------------
 * MODULE INITIALIZATION
 * --------------------------------------------------------
 ***********************************************************/
// Remove non executing experiments from instances
_cleanInstances();
setInterval(_pollExecutingExperiments, pollInterval, function(error){
   if(error) return console.error(error);
});

/***********************************************************
 * --------------------------------------------------------
 * PUBLIC INTERFACE
 * --------------------------------------------------------
 ***********************************************************/

exports.getApplication = getApplication;
exports.createApplication = createApplication;
exports.searchApplications = searchApplications;

exports.getExperiment = getExperiment;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
exports.resetExperiment = resetExperiment;
exports.destroyExperiment = destroyExperiment;
exports.searchExperiments = searchExperiments;
exports.launchExperiment = launchExperiment;

exports.getExperimentCode = getExperimentCode;
exports.putExperimentCode = putExperimentCode;
exports.putExperimentInput = putExperimentInput;
exports.getExperimentOutputFile = getExperimentOutputFile;
exports.reloadExperimentTree = reloadExperimentTree;