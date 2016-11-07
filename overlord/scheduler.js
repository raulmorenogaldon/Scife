var zerorpc = require('zerorpc');
var async = require('async');
var fs = require('fs');
var codes = require('./error_codes.js');
var logger = require('./utils.js').logger;

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
var cfg = process.argv[2];
var constants = {};

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
 * Delete experiment source file
 */
var deleteExperimentCode = function(exp_id, fpath, deleteCallback){
   getExperiment(exp_id, null, function(error, exp){
      if(error) return deleteCallback(error);

      // Save file contents
      storage.client.invoke('deleteExperimentCode', exp_id, exp.app_id, fpath, function(error){
         return deleteCallback(error);
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
      var src_path = null;
      if(src_file) src_path = constants.OVERLORD_USERNAME+'@'+constants.OVERLORD_IP+':'+src_file;

      // Save file contents
      storage.client.invoke('putExperimentInput', exp_id, exp.app_id, fpath, src_path, function(error){
         if(error) return putCallback(error);
         putCallback(null);
      });
   })
}

/**
 * Delete experiment input file
 */
var deleteExperimentInput = function(exp_id, fpath, deleteCallback){
   getExperiment(exp_id, null, function(error, exp){
      if(error) return deleteCallback(error);

      // Save file contents
      storage.client.invoke('deleteExperimentInput', exp_id, exp.app_id, fpath, function(error){
         return deleteCallback(error);
      });
   })
}

/**
 * Get experiment output data file path
 */
var getExperimentOutputFile = function(exp_id, fpath, getCallback){
   storage.client.invoke("getExperimentOutputFile", exp_id, fpath, function(error, file){
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
var searchExperiments = function(fields, searchCallback){
   exps.searchExperiments(fields, searchCallback);
}

/**
 * Entry point for experiment execution
 */
var launchExperiment = function(exp_id, nodes, image_id, size_id, debug, launchCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Launch: Launching experiment...');

   var _exp = null;
   var _image = null;
   var _size = null;

   // Check data
   async.waterfall([
      // Check experiment status
      function(wfcb){
         // Check image ID exists
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Checking image existence - '+image_id);
         instmanager.getImage(image_id, function(error, image){
            if(error) return wfcb(error);
            _image = image;
            wfcb(null);
         });
      },
      // Check size ID exists
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Checking size existence - '+size_id);
         instmanager.getSize(size_id, function(error, size){
            if(error) return wfcb(error);
            _size = size;
            wfcb(null);
         });
      },
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            _exp = exp;
            wfcb(null);
         });
      },
      // Check if already launched
      function(wfcb){
         if(_exp.status && _exp.status != "created"){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Already launched.');
            wfcb(new Error("Experiment " + _exp.id + " is already launched!, status: " + _exp.status));
         } else {
            wfcb(null);
         }
      },
      // Check quotas
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Getting quotas - '+image_id);
         instmanager.getImageQuotas(image_id, function(error, quotas){
            if(error) return wfcb(error);

            // Enough quotas?
            if(quotas.instances.in_use + nodes > quotas.instances.limit) return wfcb(new Error('Not enough instances quota.'));
            if(quotas.cores.in_use + (nodes * _size.cpus) > quotas.cores.limit) return wfcb(new Error('Not enough cores quota.'));
            if(quotas.ram.in_use + _size.ram > quotas.ram.limit) return wfcb(new Error('Not enough RAM quota.'));

            // Continue
            wfcb(null);
         });
      },
      // Setup instance configuration
      function(wfcb){
         var inst_cfg = {
            name: _exp.name,
            image_id: image_id,
            size_id: size_id,
            nodes: nodes,
            debug: debug
         };
         wfcb(null, inst_cfg);
      }
   ],
   function(error, inst_cfg){
      if(error){
         // Error trying to launch experiment
         logger.error('['+MODULE_NAME+']['+exp_id+'] Launch: Error launching - '+error);
         return launchCallback(error);
      }

      // Update status
      database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"launched", debug:debug}});

      // Add instancing task
      var task = {
         type: "instanceExperiment",
         exp_id: exp_id,
         inst_cfg: inst_cfg
      };
      taskmanager.pushTask(task, exp_id);

      logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Experiment launched.');
      launchCallback(null);
   });
}

/**
 * Reset experiment to "create" status
 */
var resetExperiment = function(exp_id, resetCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Reset: Resetting experiment...');

   // Update status
   database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"resetting"}});

   // First get experiment
   getExperiment(exp_id, null, function(error, exp){
      if(error) return resetCallback(error);

      // Abort all tasks
      logger.debug('['+MODULE_NAME+']['+exp_id+'] Reset: Aborting queue...');
      taskmanager.abortQueue(exp_id, function(task){
         if(task && task.job_id && exp.inst_id){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Reset: Aborting job - '+task.job_id);
            instmanager.abortJob(task.job_id, exp.inst_id, function(error){
               logger.error(error);
            });
         }
      }, function(error){
         // All task aborted and finished
         if(error) return logger.error('['+MODULE_NAME+']['+exp_id+'] Reset: Error aborting queue - '+error);

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
   logger.info('['+MODULE_NAME+']['+exp_id+'] Destroy: Destroying experiment...');
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Reset it
      function(exp, wfcb){
         _resetExperiment(exp_id, null, wfcb);
      },
      // Get experiment again
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Clean instance
      function(exp, wfcb){
         if(exp.inst_id){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Destroy: Cleaning instance...');
            instmanager.cleanExperiment(exp_id, exp.inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               return wfcb(error);
            });
         } else {
            wfcb(null, exp);
         }
      },
      // Remove experiment data from storage
      function(exp, wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Destroy: Removing experiment data from storage...');
         storage.client.invoke('removeExperiment', exp.app_id, exp_id, function (error) {
            if(error) return wfcb(error);
            wfcb(null, exp);
         });
      },
      // Remove from DB
      function(exp, wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Destroy: Removing experiment from DB...');
         database.db.collection('experiments').remove({id: exp_id});
         wfcb(null);
      },
   ],
   function(error){
      if(error){
         logger.error('['+MODULE_NAME+']['+exp_id+'] Destroy: Error destroying experiment - '+error);
         return destroyCallback(error);
      }
      // Log success
      logger.info('['+MODULE_NAME+']['+exp_id+'] Destroy: Deleted experiment.');
      destroyCallback(null);
   });
}

/**
 * Reload experiments' file tree.
 */
var reloadExperimentTree = function(exp_id, b_input, b_output, b_sources, reloadCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Reloading experiment trees...');
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Obtain experiment input data tree
      function(exp, wfcb){
         if(b_input){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Getting input folder tree...');
            storage.client.invoke('getInputFolderTree', exp_id, function (error, tree) {
               if(error) return wfcb(error);

               database.db.collection('experiments').updateOne({id: exp_id},{$set: {input_tree: tree}});
               wfcb(null, exp);
            });
         } else {
            wfcb(null, exp);
         }
      },
      // Obtain experiment output data tree
      function(exp, wfcb){
         if(b_output){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Getting output folder tree...');
            storage.client.invoke('getOutputFolderTree', exp_id, function (error, tree) {
               if(error) return wfcb(error);

               database.db.collection('experiments').updateOne({id: exp_id},{$set: {output_tree: tree}});
               wfcb(null, exp);
            });
         } else {
            wfcb(null, exp);
         }
      },
      // Obtain experiment source code tree
      function(exp, wfcb){
         if(b_sources){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Getting sources folder tree...');
            storage.client.invoke('getExperimentSrcFolderTree', exp_id, exp.app_id, function (error, tree) {
               if(error) return wfcb(error);

               database.db.collection('experiments').updateOne({id: exp_id},{$set: {src_tree: tree}});
               wfcb(null);
            });
         } else {
            wfcb(null, exp);
         }
      }
   ],
   function(error){
      if(error) return reloadCallback(error);
      logger.info('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Trees reloaded.');
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
taskmanager.setTaskHandler("instanceExperiment", function(task){
   // Get vars
   var exp_id = task.exp_id;
   var inst_cfg = task.inst_cfg;
   var task_id = task.id;

   // Define and create a instance
   _instanceExperiment(task, inst_cfg, function(error, inst_id){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "failed_instance"}});
         return;
      }

      // Update DB
      database.db.collection('experiments').updateOne({id: exp_id},{$set:{inst_id: inst_id}});
      instmanager.addExperiment(exp_id, inst_id);

      // Prepare task
      var next_task = {
         type: "prepareExperiment",
         exp_id: exp_id,
         inst_id: inst_id,
         debug: inst_cfg.debug
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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Prepare experiment
   _prepareExperiment(task, exp_id, inst_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "failed_prepare"}});
         return;
      }

      // Deploy task
      var next_task = {
         type: "deployExperiment",
         exp_id: exp_id,
         inst_id: inst_id,
         debug: task.debug
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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Prepare experiment
   _deployExperiment(task, exp_id, inst_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         if(task.debug){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "failed_deploy"}});
         } else {
            instmanager.cleanExperiment(exp_id, inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{inst_id: null, status: "failed_deploy"}});
            });
         }
         return;
      }

      // Compilation task
      var next_task = {
         type: "compileExperiment",
         exp_id: exp_id,
         inst_id: inst_id,
         debug: task.debug
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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Prepare experiment
   _compileExperiment(task, exp_id, inst_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         if(task.debug){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "failed_compilation"}});
         } else {
            instmanager.cleanExperiment(exp_id, inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{inst_id: null, status: "failed_compilation"}});
            });
         }
         return;
      }

      // Execution task
      var next_task = {
         type: "executeExperiment",
         exp_id: exp_id,
         inst_id: inst_id,
         debug: task.debug
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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Prepare experiment
   _executeExperiment(task, exp_id, inst_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         if(task.debug){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{status: "failed_execution"}});
         } else {
            instmanager.cleanExperiment(exp_id, inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               database.db.collection('experiments').updateOne({id: exp_id},{$set:{inst_id: null, status: "failed_execution"}});
            });
         }
         return;
      }

      // Retrieve task
      var next_task = {
         type: "retrieveExperimentOutput",
         exp_id: exp_id,
         inst_id: inst_id,
         debug: task.debug
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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Retrieve experiment output data to storage
   _retrieveExperimentOutput(task, exp_id, inst_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id, null, null);

      // Clean instance
      if(!task.debug){
         instmanager.cleanExperiment(exp_id, inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{inst_id: null}});
         });
      }

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
   var inst_id = task.inst_id;
   var task_id = task.id;

   // Prepare experiment
   _resetExperiment(exp_id, task, function(error){
      if(error){
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
 * Request an instance to use with an experiment
 */
var _instanceExperiment = function(task, inst_cfg, cb){
   logger.info('['+MODULE_NAME+'] InstanceExperiment: Instancing...');

   // Checks
   if(!inst_cfg.name) return cb(new Error('Attribute "name" not found in inst_cfg.'));
   if(!inst_cfg.image_id) return cb(new Error('Attribute "image_Id" not found in inst_cfg.'));
   if(!inst_cfg.size_id) return cb(new Error('Attribute "size_id" not found in inst_cfg.'));
   if(!inst_cfg.nodes) return cb(new Error('Attribute "nodes" not found in inst_cfg.'));

   // Check if task failed
   instmanager.requestInstance(inst_cfg.name, inst_cfg.image_id, inst_cfg.size_id, inst_cfg.nodes, function(error, inst_id){
      if(error) return cb(error);

      // Check task abort
      if(taskmanager.isTaskAborted(task.id)){
         // Clean instance
         return cb(new Error("Task aborted"));
      }

      logger.info('['+MODULE_NAME+'] InstanceExperiment: Instance created.');
      cb(null, inst_id);
   });
}

/**
 * Prepare an experiment to be deployed.
 * Labels will be applied.
 */
var _prepareExperiment = function(task, exp_id, inst_id, prepareCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Prepare: Preparing...');

   // Requested vars
   var cfg = {};

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            cfg.exp = exp;
            wfcb(null);
         });
      },
      // Get application
      function(wfcb){
         getApplication(cfg.exp.app_id, function(error, app){
            if(error) return wfcb(error);
            cfg.app = app;
            wfcb(null);
         });
      },
      // Get instance with image and size
      function(wfcb){
         instmanager.getInstance(inst_id, true, true, function(error, inst){
            if(error) return wfcb(error);
            cfg.inst = inst;
            wfcb(null);
         });
      },
      // Update labels for this instance
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Prepare: Preparing labels...');
         // Get application labels and join with experiment ones
         for(var i in cfg.app.labels){
            if(!cfg.exp.labels[cfg.app.labels[i]]){
               cfg.exp.labels[cfg.app.labels[i]] = "";
            }
         }

         // Numeric vars
         var nodes = cfg.inst.nodes + '';
         var cpus = cfg.inst.size.cpus + '';
         var totalcpus = cfg.inst.size.cpus * cfg.inst.nodes;
         totalcpus = totalcpus + '';

         // Set instance labels
         cfg.exp.labels['#EXPERIMENT_ID'] = cfg.exp.id;
         cfg.exp.labels['#EXPERIMENT_NAME'] = cfg.exp.name.replace(/ /g, "_");
         cfg.exp.labels['#APPLICATION_ID'] = cfg.app.id;
         cfg.exp.labels['#APPLICATION_NAME'] = cfg.exp.name.replace(/ /g, "_");
         cfg.exp.labels['#INPUTPATH'] = cfg.inst.image.inputpath + "/" + cfg.exp.id;
         cfg.exp.labels['#OUTPUTPATH'] = cfg.inst.image.outputpath + "/" + cfg.exp.id;
         cfg.exp.labels['#LIBPATH'] = cfg.inst.image.libpath;
         cfg.exp.labels['#TMPPATH'] = cfg.inst.image.tmppath;
         cfg.exp.labels['#CPUS'] = cpus;
         cfg.exp.labels['#NODES'] = nodes;
         cfg.exp.labels['#TOTALCPUS'] = totalcpus;

         // Apply labels
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Prepare: Storage call.');
         storage.client.invoke('prepareExperiment', cfg.app.id, cfg.exp.id, cfg.exp.labels, function(error){
            if(error) return wfcb(error);
            wfcb(null);
         });
      }
   ],
   function(error){
      if(error) return prepareCallback(error);
      logger.info('['+MODULE_NAME+']['+exp_id+'] Prepare: Prepared for deployment.');
      prepareCallback(null);
   });
}

/**
 * Deploy an experiment in target instance
 */
var _deployExperiment = function(task, exp_id, inst_id, deployCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Deploy: Deploying...');

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
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Getting application URL...');
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
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Getting experiment URL...');
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
         instmanager.getInstance(inst_id, true, false, function(error, inst){
            if(error){
               wfcb(error);
            } else {
               wfcb(null, app, exp, inst);
            }
         });
      },
      // Abort previous job
      function(app, exp, inst, wfcb){
         if(task.job_id){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Aborting previous jobs...');
            instmanager.abortJob(task.job_id, inst.id, function(error){
               if(error) {return wfcb(error);}
               wfcb(null, app, exp, inst);
            });
         } else {
            wfcb(null, app, exp, inst);
         }
      },
      // Copy experiment in FS
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+inst.image.workpath+"; git clone -b "+exp.id+"-L "+exp.exp_url+" "+inst.image.workpath+"/"+exp.id;
         var work_dir = inst.image.workpath + "/" + exp.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Cloning experiment...');
         instmanager.executeJob(inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, app, exp, inst);
            });
         });
      },
      // Copy inputdata in FS
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+inst.image.inputpath+"/"+exp.id+"; sshpass -p '"+constants.STORAGE_PASSWORD+"' rsync -Lr "+exp.input_url+"/* "+inst.image.inputpath+"/"+exp.id;
         var work_dir = inst.image.workpath + "/" + exp.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Making inputdata directory...');
         instmanager.executeJob(inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, app, exp, inst);
            });
         });
      },
      // Create outputdata in FS
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+inst.image.outputpath+"/"+exp.id;
         var work_dir = inst.image.workpath + "/" + exp.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Creating outputdata directory...');
         instmanager.executeJob(inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, app, exp, inst);
            });
         });
      },
      // Init EXPERIMENT_STATUS
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = 'echo -n "deployed" > '+inst.image.workpath+'/'+exp.id+'/EXPERIMENT_STATUS';
         var work_dir = inst.image.workpath + "/" + exp.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Initializing EXPERIMENT_STATUS...');
         instmanager.executeJob(inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null);
            });
         });
      },
   ],
   function(error){
      if(error){return deployCallback(error);}

      // Poll experiment status
      logger.debug('['+MODULE_NAME+']['+exp_id+'] Deploy: Polling...');
      _pollExperiment(exp_id, inst_id, false, function(error, status){
         // Check status
         if(status != "deployed"){
            return deployCallback(new Error("Failed to deploy experiment, status: "+status));
         }

         // Deployed
         logger.info('['+MODULE_NAME+']['+exp_id+'] Deploy: Done.');
         deployCallback(null);
      });
   });
}

/**
 * Launch compilation script of an experiment
 */
var _compileExperiment = function(task, exp_id, inst_id, compileCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Compile: Begin.');

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get application
      function(exp, wfcb){
         getApplication(exp.app_id, function(error, app){
            if(error) return wfcb(error);
            wfcb(null, app, exp);
         });
      },
      // Get instance
      function(app, exp, wfcb){
         instmanager.getInstance(inst_id, true, false, function(error, inst){
            if(error) return wfcb(error);
            wfcb(null, app, exp, inst);
         });
      },
      // Execute compilation script
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var work_dir = inst.image.workpath+"/"+exp.id;
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
            wfcb(true);
         } else {
            // Execute job
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Compile: Executing compiling script...');
            instmanager.executeJob(inst.id, exe_script, work_dir, 1, function (error, job_id) {
               if (error) {return wfcb(error);}
               logger.debug('['+MODULE_NAME+']['+exp_id+'] Compile: Job ID - ' + job_id);

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null);
            });
         }
      },
   ],
   function(error){
      if(error && error != true) return compileCallback(error);
      logger.info('['+MODULE_NAME+']['+exp_id+'] Compile: Compiling...');

      // Poll experiment status
      _pollExperiment(exp_id, inst_id, false, function(error, status){
         if(error) return compileCallback(error);

         // Wait for command completion
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Compile: Waiting - ' + task.job_id);
         instmanager.waitJob(task.job_id, inst_id, function(error){
            if(error) return compileCallback(error);

            // Poll experiment status
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Compile: Job done, polling...');
            _pollExperiment(exp_id, inst_id, true, function(error, status){
               if(error) return compileCallback(error);

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
               logger.info('['+MODULE_NAME+']['+exp_id+'] Compile: Done.');
               compileCallback(null);
            });
         });
      });
   });
}

/**
 * Execute an experiment in target instance
 */
var _executeExperiment = function(task, exp_id, inst_id, executionCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Execute: Begin.');

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
         instmanager.getInstance(inst_id, true, false, function(error, inst){
            if(error) return wfcb(error);
            wfcb(null, app, exp, inst);
         });
      },
      // Execute execution script
      function(app, exp, inst, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var work_dir = inst.image.workpath+"/"+exp.id;
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
            wfcb(true, inst);
         } else {
            // Execute job
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Execute: Launching execution script...');
            instmanager.executeJob(inst.id, exe_script, work_dir, inst.nodes, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null);
            });
         }
      }
   ],
   function(error){
      if(error && error != true){return executionCallback(error);}
      logger.info('['+MODULE_NAME+']['+exp_id+'] Execute: Executing...');

      // Poll experiment status
      _pollExperiment(exp_id, inst_id, false, function(error, status){
         if(error){return executionCallback(error);}

         // Wait for command completion
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Execute: Waiting - ' + task.job_id);
         instmanager.waitJob(task.job_id, inst_id, function(error){
            if(error){return executionCallback(error);}

            // Poll experiment status
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Execute: Job done, polling...');
            _pollExperiment(exp_id, inst_id, true, function(error, status){
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
               logger.info('['+MODULE_NAME+']['+exp_id+'] Execute: Done.');
               executionCallback(null);
            });
         });
      });
   });
}

/**
 * Retrieve experiment output data
 */
var _retrieveExperimentOutput = function(task, exp_id, inst_id, retrieveCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Retrieve: Begin.');

   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Get instance
      function(exp, wfcb){
         instmanager.getInstance(inst_id, true, false, function(error, inst){
            if(error) return wfcb(error);
            wfcb(null, exp, inst);
         });
      },
      // Get storage URL
      function(exp, inst, wfcb){
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Retrieve: Getting output URL...');
         storage.client.invoke('getExperimentOutputURL', exp_id, function(error, url){
            if(error) return wfcb(error);
            wfcb(null, exp, inst, url);
         });
      },
      // Execute excution script
      function(exp, inst, url, wfcb){
         // Check task abort
         if(task && taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         logger.debug("["+exp_id+"] Getting experiment output data path");
         var output_files = inst.image.outputpath+"/"+exp.id+"/*";

         // Execute command
         var cmd = "sshpass -p '"+constants.STORAGE_PASSWORD+"' rsync -Lre 'ssh -o StrictHostKeyChecking=no' "+output_files+" "+url+"/ --delete-after";
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Retrieve: Copying output files to storage...');
         instmanager.executeCommand(inst.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            // Check task abort
            if(task && taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Reload output tree
            reloadExperimentTree(exp_id, false, true, false, wfcb);
         });
      }
   ],
   function(error){
      if(error) return retrieveCallback(error);
      logger.info('['+MODULE_NAME+']['+exp_id+'] Retrieve: Done.');
      retrieveCallback(null);
   });
}

/**
 * Resets an experiment
 */
var _resetExperiment = function(exp_id, task, resetCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Reset: Begin.');
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, wfcb);
      },
      // Update database
      function(exp, wfcb){
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"resetting", debug:false}});
         wfcb(null, exp);
      },
      // Clean job
      function(exp, wfcb){
         if(exp.inst_id){
            var job_id = null;
            if(task) job_id = task.job_id;
            // Experiment will be removed completely from the instance
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Reset: Cleaning experiment...');
            instmanager.cleanExperiment(exp_id, exp.inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
               if (error) logger.error('['+MODULE_NAME+']['+exp_id+'] Reset: Failed to clean experiment, error: ' + error);
               wfcb(null, exp);
            });
         } else {
            wfcb(null, exp);
         }
      }
   ],
   function(error, exp){
      if(error){
         // Error trying to reset experiment
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"reset_failed"}});
         resetCallback(error);
      } else {
         // Update status
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{status:"created", inst_id: null, logs:[]}});

         // Callback
         logger.debug('['+MODULE_NAME+']['+exp_id+'] Reset: Done.');
         resetCallback(null);
      }
   });
}

/**
 * Cache to avoid polling again
 */
var _polling = {};
/**
 * Get experiment status from target instance and update in DB.
 */
var _pollExperiment = function(exp_id, inst_id, force, pollCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Poll: Begin.');

   // Check instance param
   if(!inst_id){
      // Nothing to poll
      logger.debug('['+MODULE_NAME+']['+exp_id+'] Poll: nothing to poll.');
      getExperiment(exp_id, null, function(error, exp){
         if(error) return pollCallback(error);
         pollCallback(null, exp.status);
      });
      return;
   }

   // Avoid multiple polling
   if(!_polling[exp_id]){
      _polling[exp_id] = true;

      async.waterfall([
         // Get experiment
         function(wfcb){
            getExperiment(exp_id, null, wfcb);
         },
         // Get instance
         function(exp, wfcb){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Poll: Getting instance...');
            instmanager.getInstance(inst_id, true, false, function(error, inst){
               if(error) return wfcb(error);
               wfcb(null, exp, inst);
            });
         },
         // Poll experiment logs
         function(exp, inst, wfcb){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Poll: Polling logs...');
            _pollExperimentLogs(exp.id, inst.id, inst.image, ['COMPILATION_LOG','EXECUTION_LOG','*.log', '*.log.*', '*.bldlog.*'], function (error, logs) {
               if(error) return wfcb(error);
               // Update status
               database.db.collection('experiments').updateOne({id: exp.id},{$set:{logs:logs}});

               // Callback status
               wfcb(null, exp, inst);
            });
         },
         // Retrieve experiment output data
         function(exp, inst, wfcb){
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Poll: Polling output data...');
            _retrieveExperimentOutput(null, exp.id, inst.id, function (error) {
               if(error) return wfcb(error);

               // Callback
               wfcb(null, exp, inst);
            });
         },
         // Poll experiment status
         function(exp, inst, wfcb){
            var work_dir = inst.image.workpath+"/"+exp.id;
            var cmd = 'cat '+work_dir+'/EXPERIMENT_STATUS';
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Poll: Polling status...');
            instmanager.executeCommand(inst.id, cmd, function (error, output) {
               if(error) return wfcb(error);

               // Get status
               var status = output.stdout;

               // Update status if the file exists
               if(status != ""){
                  database.db.collection('experiments').updateOne({id: exp.id},{$set:{status:status}});
               }

               // Callback status
               wfcb(null, status);
            });
         }
      ],
      function(error, status){
         _polling[exp_id] = false;
         if(error) return pollCallback(error);
         logger.info('['+MODULE_NAME+']['+exp_id+'] Poll: Done - ' + status);
         pollCallback(null, status);
      });
   } else {
      // If force, full polling must be done
      if(force) return setTimeout(_pollExperiment, 1000, exp_id, inst_id, true, pollCallback);
      // If not, just wait current polling to end
      else return setTimeout(_waitPollFinish, 3000, exp_id, pollCallback);
   }
}

/**
 * Wait until polling is done and return status.
 */
var _waitPollFinish = function(exp_id, pollCallback){
   // Wait poll to end
   if(!_polling[exp_id]){
      // Finished!
      getExperiment(exp_id, null, function(error, exp){
         if(error) return pollCallback(error);
         return pollCallback(null, exp.status);
      });
   } else {
      return setTimeout(_waitPollFinish, 3000, exp_id, pollCallback);
   }
}

/**
 * Update experiment logs in DB.
 */
var _pollExperimentLogs = function(exp_id, inst_id, image, log_files, pollCallback){
   // Working directory
   var work_dir = image.workpath+"/"+exp_id;
   var logs = [];

   // Experiment data
   logger.debug('['+MODULE_NAME+']['+exp_id+'] PollLogs: Getting experiment data...');
   getExperiment(exp_id, {logs:1}, function(error, exp){
      if(error) return pollCallback(error);

      // Get previous logs
      var prev_logs = exp.logs;
      if(!prev_logs) prev_logs = [];

      // Get log files list
      logger.debug('['+MODULE_NAME+']['+exp_id+'] PollLogs: Finding logs - ' + log_files);
      _findExperimentLogs(exp_id, inst_id, image, log_files, function(error, loglist){
         if(error){
            _polling[exp_id] = false;
            logger.error('['+MODULE_NAME+']['+exp_id+'] PollLogs: Error finding logs.');
            return pollCallback(error);
         }

         // Iterate logs
         var tasks = [];
         for(var i = 0; i < loglist.length; i++){
            // var i must be independent between tasks
            (function(i){
               tasks.push(function(taskcb){
                  var prev_log = null;
                  var log_filename = loglist[i].split('\\').pop().split('/').pop();

                  // Check if previous log exists
                  for(var j = 0; j < prev_logs.length; j++){
                     prev_log = prev_logs[j];
                     if(prev_log.name == log_filename){
                        // Coincidence
                        break;
                     }
                     prev_log = null;
                  }

                  // Check modified date
                  var cmd = 'echo -n "$(stat -c %y '+loglist[i]+')"';
                  instmanager.executeCommand(inst_id, cmd, function (error, output) {
                     if(error) return taskcb(new Error("Failed to get modified date of "+ loglist[i]+ ", error: "+ error));

                     // Get date
                     var log_date = output.stdout;

                     // Get date from previous data
                     if(prev_log && prev_log.last_modified && prev_log.last_modified == log_date){
                        // Do not update, its the same log
                        logger.debug('['+MODULE_NAME+']['+exp_id+'] PollLogs: No changes in log content - ' + log_filename);
                        logs.push({name: log_filename, content: prev_log.content, last_modified: log_date});
                        taskcb(null);
                     } else {
                        // Update log
                        var cmd = 'zcat -f '+loglist[i];
                        instmanager.executeCommand(inst_id, cmd, function (error, output) {
                           if(error) return taskcb(new Error("Failed to poll log "+ loglist[i]+ ", error: "+ error));

                           // Get log content
                           var content = output.stdout;

                           // Add log
                           logger.debug('['+MODULE_NAME+']['+exp_id+'] PollLogs: Updating log content - ' + log_filename + ' : ' + log_date);
                           logs.push({name: log_filename, content: content, last_modified: log_date});
                           taskcb(null);
                        });
                     }
                  });
               });
            })(i);
         }

         // Execute tasks
         async.series(tasks, function(error){
            if(error) return pollCallback(error);
            // Sort
            logs.sort(function(a,b){return (a.name > b.name) ? 1 : ((b.name > a.name) ? -1 : 0);});
            logger.debug('['+MODULE_NAME+']['+exp_id+'] PollLogs: Done.');
            pollCallback(null, logs);
         });
      });

   });
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
   var cmd = 'find -L '+work_dir+ ' -name "'+log_files[0]+'"';
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
                     // Do not remove from instance in debug mode
                     if(!exp || !exp.debug){
                        // Remove experiment from this instance
                        instmanager.cleanExperiment(exp_id, inst.id, {b_input: true, b_output: true, b_sources: true, b_remove: true, b_force: true}, function(error){
                           if(error) logger.error('['+MODULE_NAME+']['+exp_id+'] CleanInstances: Failed to clean experiment from instance - ' + inst.id + ' : ' + error);
                           logger.info('['+MODULE_NAME+']['+exp_id+'] CleanInstances: Cleaned experiment from instance - ' + inst.id);
                        });
                     }
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
      return setTimeout(_pollExecutingExperiments, 1000);
   }

   // Iterate experiments
   database.db.collection('experiments').find({
      status: { $in: ["deployed", "compiling", "executing"]}
   }).forEach(function(exp){
      // Poll experiment status
      logger.debug('['+MODULE_NAME+']['+exp.id+'] PollExecuting: Polling...');
      _pollExperiment(exp.id, exp.inst_id, false, function(error, status){
         if(error) logger.error('['+MODULE_NAME+']['+exp.id+'] Failed to automatic poll: '+error);
      });
   });
}

/***********************************************************
 * --------------------------------------------------------
 * MODULE INITIALIZATION
 * --------------------------------------------------------
 ***********************************************************/
// Get config file
if(!cfg) throw new Error('No CFG file has been provided.');

// Steps
async.waterfall([
   // Read config file
   function(wfcb){
      logger.info('['+MODULE_NAME+'] Reading config file: '+cfg);
      fs.readFile(cfg, function(error, fcontent){
         if(error) return wfcb(error);
         wfcb(null, fcontent);
      });
   },
   // Load cfg
   function(fcontent, wfcb){
      logger.info('['+MODULE_NAME+'] Loading config file...');

      // Parse cfg
      constants = JSON.parse(fcontent);
      wfcb(null);
   },
   // Init database
   function(wfcb){
      logger.info('['+MODULE_NAME+'] Initializing DB...');
      database.init(constants, wfcb);
   },
   // Init storage
   function(wfcb){
      logger.info('['+MODULE_NAME+'] Initializing storage...');
      storage.init(constants, wfcb);
   },
   // Init instance manager
   function(wfcb){
      logger.info('['+MODULE_NAME+'] Initializing instance...');
      instmanager.init(constants, wfcb);
   }
],
function(error){
   if(error) throw error;
   logger.info('['+MODULE_NAME+'] Initialization completed.');

   // Remove non executing experiments from instances
   _cleanInstances();
   setInterval(_pollExecutingExperiments, pollInterval, function(error){
      if(error) return logger.error(error);
   });
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
exports.deleteExperimentCode = deleteExperimentCode;
exports.putExperimentInput = putExperimentInput;
exports.deleteExperimentInput = deleteExperimentInput;
exports.getExperimentOutputFile = getExperimentOutputFile;
exports.reloadExperimentTree = reloadExperimentTree;
