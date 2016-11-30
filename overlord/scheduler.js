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
var execmanager = require('./execution.js');

/**
 * Module vars
 */
var MODULE_NAME = "SC";
var pollInterval = 60000;
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
   exps.getExperiment(exp_id, fields, function(error, exp){
      if(error) return getCallback(error);

      // Add last execution status
      if(exp.last_execution){
         execmanager.getExecution(exp.last_execution, {status: 1}, function(error, exec){
            // If error, just return experiment
            if(!error) exp.last_execution_status = exec.status;
            return getCallback(null, exp);
         });
      } else {
         return getCallback(null, exp);
      }
   });
}

/**
 * Perform a maintenance operation over experiment
 */
var maintainExperiment = function(exec_id, operation, maintainCallback){
   exps.maintainExperiment(exec_id, operation, maintainCallback);
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
var getExecutionOutputFile = function(exp_id, fpath, getCallback){
   storage.client.invoke("getExecutionOutputFile", exp_id, fpath, function(error, file){
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
   exps.searchExperiments(fields, function(error, exps){
      if(error) return searchCallback(error);

      // Add last execution status
      var tasks = [];
      for(var e = 0; e < exps.length; e++){
         if(exps[e].last_execution){
            (function(exp, i){
               tasks.push(function(taskcb){
                  execmanager.getExecution(exp.last_execution, {status: 1}, function(error, exec){
                     // If error, just return experiment
                     if(!error) exps[i].last_execution_status = exec.status;
                     return taskcb(null);
                  });
               });
            })(exps[e],e);
         }
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         return searchCallback(error, exps);
      });
   });
}

/**
 * Entry point for experiment execution
 */
var launchExperiment = function(exp_id, nodes, image_id, size_id, launch_opts, launchCallback){
   logger.info('['+MODULE_NAME+']['+exp_id+'] Launch: Launching experiment...');

   var _exec = null;
   var _exp = null;
   var _image = null;
   var _size = null;

   // Check data
   async.waterfall([
      // Check image ID exists
      function(wfcb){
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
      // Create execution
      function(wfcb){
         if(!_exp.times_executed) _exp.times_executed = 0;
         _exp.times_executed += 1;
         launch_opts.nodes = nodes;
         launch_opts.image = {
            name: _image.name,
            minion: _image.minion
         };
         launch_opts.size = {
            name: _size.name,
            cpus: _size.cpus,
            ram: _size.ram
         };
         execmanager.createExecution(exp_id, _exp.name+" #"+_exp.times_executed, null, launch_opts, _exp.labels, function(error, exec){
            if(error) return wfcb(error);
            logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Initialized execution data '+exec.id);
            // Update status
            database.db.collection('experiments').updateOne({id: exp_id},{$set:{last_execution:exec.id, times_executed: _exp.times_executed}});
            database.db.collection('executions').updateOne({id: exec.id},{$set:{status:"launched"}});
            _exec = exec;
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

      // Add instancing task
      var task = {
         type: "instanceExecution",
         exec_id: _exec.id,
         inst_cfg: inst_cfg
      };
      taskmanager.pushTask(task, _exec.id);

      logger.debug('['+MODULE_NAME+']['+exp_id+'] Launch: Experiment launched.');
      launchCallback(null);
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
         getExperiment(exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            wfcb(null, exp);
         });
      },
      // Destroy executions associated with this experiment
      function(exp, wfcb){
         // Update DB
         database.db.collection('experiments').updateOne({id: exp_id},{$set:{last_execution:null}});

         // Get executions
         execmanager.searchExecutions({exp_id: exp.id},function(error, execs){
            if(error) return wfcb(error);
            // Destroy executions
            _destroyExecutions(execs, function(error){
               if(error) return wfcb(error);
               wfcb(null, exp);
            });
         });
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
var reloadExperimentTree = function(exp_id, b_input, b_sources, reloadCallback){
   logger.debug('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Reloading experiment trees...');
   async.waterfall([
      // Get experiment
      function(wfcb){
         getExperiment(exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            wfcb(null, exp);
         });
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
      logger.debug('['+MODULE_NAME+']['+exp_id+'] ReloadTree: Trees reloaded.');
      reloadCallback(error);
   });
}

/**
 * Reload executions' file tree.
 */
var reloadExecutionOutputTree = function(exec_id, reloadCallback){
   execmanager.reloadExecutionOutputTree(exec_id, reloadCallback);
}

/**
 * Clean experiment execution
 */
var cleanExecution = function(exec_id, cb){
   // Get execution
   execmanager.getExecution(exec_id, null, function(error, exec){
      if(error) return cb(error);
      if(!exec.inst_id) return cb(null);

      // Clean instance
      instmanager.cleanExecution(exec_id, exec.inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
         return cb(error);
      });
   });
}

/**
 * Abort experiment execution
 */
var abortExecution = function(exec_id, cb){
   // Get execution
   execmanager.getExecution(exec_id, null, function(error, exec){
      if(exec.status == "aborting" || exec.status == "deleting" || exec.status == "deleted") return cb(null);

      // Set execution status
      database.db.collection('executions').updateOne({id: exec_id},{$set:{status: "aborting"}});

      // Abort all tasks for this execution
      taskmanager.abortQueue(exec_id, function(task){
         // Abort job
         if(task.job_id){
            logger.error('['+MODULE_NAME+']['+exec_id+'] AbortExec: Aborting job: '+task.job_id);
            instmanager.abortJob(task.job_id, exec.inst_id, function(error){
               if(error) logger.error('['+MODULE_NAME+']['+inst_id+'] AbortExec: Failed to abort job: '+error);
            });
         }
      }, function(error){
         if(error){
            logger.error('['+MODULE_NAME+']['+exec_id+'] AbortExec: Failed to abort queue.');
            return cb(error);
         }
         // Set execution status
         database.db.collection('executions').updateOne({id: exec_id},{$set:{status: "aborted"}});
         return cb(null);
      });
   });
}

/**
 * Destroy experiment execution
 */
var destroyExecution = function(exec_id, cb){
   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, function(error, exec){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Abort execution first
      function(exec, wfcb){
         abortExecution(exec.id, function(error){
            return wfcb(error, exec);
         });
      }
   ],
   function(error){
      // Add destroy task
      var task = {
         type: "destroyExecution",
         exec_id: exec_id
      };
      taskmanager.pushTask(task, exec_id);
      return cb(null);
   });
}

/**
 * Destroy experiment execution task handler
 */
var _destroyExecution = function(task, exec_id, cb){
   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, function(error, exec){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Destroy execution
      function(exec, wfcb){
         execmanager.destroyExecution(exec_id, false, function(error){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Update last execution
      function(exec, wfcb){
         getExperiment(exec.exp_id, null, function(error, exp){
            if(exp.last_execution == exec.id){
               database.db.collection('experiments').updateOne({id: exp.id},{$set: {last_execution: null}});
            }
            wfcb(null);
         });
      }
   ],
   function(error){
      return cb(error);
   });
}

/***********************************************************
 * --------------------------------------------------------
 * SCHEDULING EVENTS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Instance a cluster
 */
taskmanager.setTaskHandler("instanceExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;
   var inst_cfg = task.inst_cfg;

   // Define and create a instance
   _instanceExecution(task, inst_cfg, function(error, inst_id){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         database.db.collection('executions').updateOne({id: exec_id},{$set:{status: "failed_instance"}});
         return;
      }

      // Update DB
      database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: inst_id}});
      instmanager.addExecution(exec_id, inst_id);

      // Prepare task
      var next_task = {
         type: "prepareExecution",
         exec_id: exec_id
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exec_id);
   });
});

/**
 * Prepare experiment handler
 */
taskmanager.setTaskHandler("prepareExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Prepare experiment
   _prepareExecution(task, exec_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Deploy task
      var next_task = {
         type: "deployExecution",
         exec_id: exec_id,
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exec_id);
   });
});

/**
 * Deploy experiment handler
 */
taskmanager.setTaskHandler("deployExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Deploy into instance
   _deployExecution(task, exec_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         cleanExecution(exec_id, function(error){
            database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: null, status: "failed_deploy"}});
         });

         return;
      }

      // Update execution launch date
      database.db.collection('executions').updateOne({id: task.exec_id},{$set:{launch_date: new Date().toString()}});

      // Compilation task
      var next_task = {
         type: "compileExecution",
         exec_id: exec_id,
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exec_id);
   });
});

/**
 * Compile experiment handler
 */
taskmanager.setTaskHandler("compileExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Compile
   _compileExecution(task, exec_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         cleanExecution(exec_id, function(error){
            database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: null, status: "failed_compilation"}});
         });

         // Update execution finish date
         database.db.collection('executions').updateOne({id: task.exec_id},{$set:{finish_date: new Date().toString()}});

         return;
      }

      // Execution task
      var next_task = {
         type: "executeExecution",
         exec_id: exec_id,
         retries: 3,
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exec_id);
   });
});

/**
 * Execute experiment handler
 */
taskmanager.setTaskHandler("executeExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Execute
   _executeExecution(task, exec_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);

         // Clean instance
         cleanExecution(exec_id, function(error){
            database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: null, status: "failed_execution"}});
         });

         // Update execution finish date
         database.db.collection('executions').updateOne({id: task.exec_id},{$set:{finish_date: new Date().toString()}});

         return;
      }

      // Retrieve task
      var next_task = {
         type: "retrieveExecutionOutput",
         exec_id: exec_id,
      };

      // Set task to done and setup next task
      taskmanager.setTaskDone(task_id, next_task, exec_id);
   });
});

/**
 * Retrieve data handler
 */
taskmanager.setTaskHandler("retrieveExecutionOutput", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Retrieve execution output data to storage
   _retrieveExecutionOutput(task, exec_id, function(error){
      if(error){
         // Set task failed
         taskmanager.setTaskFailed(task_id, error);
         return;
      }

      // Set task to done
      taskmanager.setTaskDone(task_id, null, null);

      // Clean instance
      cleanExecution(exec_id, function(error){
         database.db.collection('executions').updateOne({id: exec_id},{$set:{inst_id: null}});
      });

      // Update execution finish date
      database.db.collection('executions').updateOne({id: task.exec_id},{$set:{finish_date: new Date().toString()}});

      // Set execution to done status
      database.db.collection('executions').updateOne({id: exec_id},{$set:{status: "done"}});
   });
});

/**
 * Destroy execution
 */
taskmanager.setTaskHandler("destroyExecution", function(task){
   // Get vars
   var task_id = task.id;
   var exec_id = task.exec_id;

   // Retrieve execution output data to storage
   _destroyExecution(task, exec_id, function(error){
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
var _instanceExecution = function(task, inst_cfg, cb){
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
 * Prepare an execution to be deployed.
 * Labels will be applied.
 */
var _prepareExecution = function(task, exec_id, prepareCallback){
   logger.info('['+MODULE_NAME+']['+exec_id+'] Prepare: Preparing...');

   // Requested vars
   var _cfg = {};

   async.waterfall([
      // Get execution
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Getting execution...');
         execmanager.getExecution(exec_id, null, function(error, exec){
            if(error) return wfcb(error);
            _cfg.exec = exec;
            wfcb(null);
         });
      },
      // Get experiment
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Getting experiment...');
         getExperiment(_cfg.exec.exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            _cfg.exp = exp;
            wfcb(null);
         });
      },
      // Get application
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Getting application...');
         getApplication(_cfg.exp.app_id, function(error, app){
            if(error) return wfcb(error);
            _cfg.app = app;
            wfcb(null);
         });
      },
      // Get instance
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Getting instance...');
         instmanager.getInstance(_cfg.exec.inst_id, function(error, inst){
            if(error) return wfcb(error);
            _cfg.inst = inst;
            wfcb(null);
         });
      },
      // Update labels for this instance
      function(wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Preparing labels...');
         // Get application labels and join with experiment ones
         for(var i in _cfg.app.labels){
            if(!_cfg.exp.labels[_cfg.app.labels[i]]){
               _cfg.exp.labels[_cfg.app.labels[i]] = "";
            }
         }

         // Numeric vars
         var nodes = _cfg.inst.nodes + '';
         var cpus = _cfg.inst.size.cpus + '';
         var totalcpus = _cfg.inst.size.cpus * _cfg.inst.nodes;
         totalcpus = totalcpus + '';

         // Set instance labels
         _cfg.exp.labels['#EXPERIMENT_ID'] = _cfg.exp.id;
         _cfg.exp.labels['#EXPERIMENT_NAME'] = _cfg.exp.name.replace(/ /g, "_");
         _cfg.exp.labels['#APPLICATION_ID'] = _cfg.app.id;
         _cfg.exp.labels['#APPLICATION_NAME'] = _cfg.exp.name.replace(/ /g, "_");
         _cfg.exp.labels['#INPUTPATH'] = _cfg.inst.image.inputpath + "/" + _cfg.exec.id;
         _cfg.exp.labels['#OUTPUTPATH'] = _cfg.inst.image.outputpath + "/" + _cfg.exec.id;
         _cfg.exp.labels['#LIBPATH'] = _cfg.inst.image.libpath;
         _cfg.exp.labels['#TMPPATH'] = _cfg.inst.image.tmppath;
         _cfg.exp.labels['#CPUS'] = cpus;
         _cfg.exp.labels['#NODES'] = nodes;
         _cfg.exp.labels['#TOTALCPUS'] = totalcpus;

         // Apply labels
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Prepare: Storage call.');
         storage.client.invoke('prepareExecution', _cfg.app.id, _cfg.exp.id, _cfg.exec.id, _cfg.exp.labels, function(error){
            if(error) return wfcb(error);
            wfcb(null);
         });
      }
   ],
   function(error){
      if(error) return prepareCallback(error);
      logger.info('['+MODULE_NAME+']['+exec_id+'] Prepare: Prepared for deployment.');
      prepareCallback(null);
   });
}

/**
 * Deploy an experiment in target instance
 */
var _deployExecution = function(task, exec_id, deployCallback){
   logger.info('['+MODULE_NAME+']['+exec_id+'] Deploy: Deploying...');

   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, function(error, exec){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Get experiment
      function(exec, wfcb){
         getExperiment(exec.exp_id, null, function(error, exp){
            exec.exp = exp;
            wfcb(error, exec);
         });
      },
      // Get application
      function(exec, wfcb){
         if(!exec.status || exec.status != "launched") return wfcb(new Error("["+exec_id+"] Aborting deployment, status:"+exec.status));
         getApplication(exec.exp.app_id, function(error, app){
            exec.app = app;
            wfcb(error, exec);
         });
      },
      // Get instance
      function(exec, wfcb){
         instmanager.getInstance(exec.inst_id, function(error, inst){
            exec.inst = inst;
            wfcb(error, exec);
         });
      },
      // Get application URL
      function(exec, wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Getting application URL...');
         storage.client.invoke('getApplicationURL', exec.app.id, function(error, url){
            exec.app_url = url;
            wfcb(error, exec);
         });
      },
      // Get input URL
      function(exec, wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Getting experiment URL...');
         storage.client.invoke('getExperimentInputURL', exec.exp_id, function(error, url){
            exec.input_url = url;
            wfcb(error, exec);
         });
      },
      // Abort previous job
      function(exec, wfcb){
         if(task.job_id){
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Aborting previous jobs...');
            instmanager.abortJob(task.job_id, exec.inst.id, function(error){
               wfcb(error, exec);
            });
         } else {
            wfcb(null, exec);
         }
      },
      // Copy experiment in FS
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+exec.inst.image.workpath+"; git clone -b "+exec.id+" "+exec.app_url+" "+exec.inst.image.workpath+"/"+exec.id;
         var work_dir = exec.inst.image.workpath+"/"+exec.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Cloning experiment...');
         instmanager.executeJob(exec.inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, exec.inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         });
      },
      // Copy inputdata in FS
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+exec.inst.image.inputpath+"/"+exec.id+
            "; sshpass -p '"+constants.STORAGE_PASSWORD+"' rsync -Lr "+exec.input_url+"/* "+exec.inst.image.inputpath+"/"+exec.id;
         var work_dir = exec.inst.image.workpath + "/" + exec.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Making inputdata directory...');
         instmanager.executeJob(exec.inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, exec.inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         });
      },
      // Create outputdata in FS
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = "mkdir -p "+exec.inst.image.outputpath+"/"+exec.id;
         var work_dir = exec.inst.image.workpath + "/" + exec.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Creating outputdata directory...');
         instmanager.executeJob(exec.inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, exec.inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         });
      },
      // Init EXPERIMENT_STATUS
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var cmd = 'echo -n "deployed" > '+exec.inst.image.workpath+'/'+exec.id+'/EXPERIMENT_STATUS';
         var work_dir = exec.inst.image.workpath + "/" + exec.id;

         // Execute command
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Initializing EXPERIMENT_STATUS...');
         instmanager.executeJob(exec.inst.id, cmd, work_dir, 1, function (error, job_id) {
            if (error) {return wfcb(error);}

            // Update task and DB
            task.job_id = job_id;
            database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

            // Check task abort
            if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Wait for command completion
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Waiting - ' + job_id);
            instmanager.waitJob(job_id, exec.inst.id, function(error){
               if(error){return wfcb(error);}

               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         });
      },
   ],
   function(error, exec){
      if(error){return deployCallback(error);}

      // Poll experiment status
      logger.debug('['+MODULE_NAME+']['+exec_id+'] Deploy: Polling...');
      _pollExecution(exec_id, false, function(error, status){
         // Check status
         if(status != "deployed"){
            return deployCallback(new Error("Failed to deploy execution, status: "+status));
         }

         // Deployed
         logger.info('['+MODULE_NAME+']['+exec_id+'] Deploy: Done.');
         deployCallback(null);
      });
   });
}

/**
 * Launch compilation script of an experiment
 */
var _compileExecution = function(task, exec_id, compileCallback){
   logger.info('['+MODULE_NAME+']['+exec_id+'] Compile: Begin.');

   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, wfcb);
      },
      // Get experiment
      function(exec, wfcb){
         getExperiment(exec.exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            exec.exp = exp;
            wfcb(null, exec);
         });
      },
      // Get application
      function(exec, wfcb){
         getApplication(exec.exp.app_id, function(error, app){
            if(error) return wfcb(error);
            exec.app = app;
            wfcb(null, exec);
         });
      },
      // Get instance
      function(exec, wfcb){
         instmanager.getInstance(exec.inst_id, function(error, inst){
            if(error) return wfcb(error);
            exec.inst = inst;
            wfcb(null, exec);
         });
      },
      // Execute compilation script
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var work_dir = exec.inst.image.workpath+"/"+exec.id;
         var exe_script = ''+
         '#!/bin/sh \n'+
         'cd '+work_dir+'\n'+
         'echo -n "compiling" > EXPERIMENT_STATUS \n'+
         './'+exec.app.creation_script+' >> COMPILATION_LOG 2>&1 \n'+
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
            wfcb(true, exec);
         } else {
            // Execute job
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Compile: Executing compiling script...');
            instmanager.executeJob(exec.inst.id, exe_script, work_dir, 1, function (error, job_id) {
               if (error) {return wfcb(error);}
               logger.debug('['+MODULE_NAME+']['+exec_id+'] Compile: Job ID - ' + job_id);

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         }
      },
   ],
   function(error, exec){
      if(error && error != true) return compileCallback(error);
      logger.info('['+MODULE_NAME+']['+exec_id+'] Compile: Compiling...');

      // Poll execution status
      _pollExecution(exec_id, false, function(error, status){
         if(error) return compileCallback(error);

         // Wait for command completion
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Compile: Waiting - ' + task.job_id);
         instmanager.waitJob(task.job_id, exec.inst_id, function(error){
            if(error) return compileCallback(error);

            // Poll execution status
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Compile: Job done, polling...');
            _pollExecution(exec_id, true, function(error, status){
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
               logger.info('['+MODULE_NAME+']['+exec_id+'] Compile: Done.');
               compileCallback(null);
            });
         });
      });
   });
}

/**
 * Execute an experiment in target instance
 */
var _executeExecution = function(task, exec_id, executionCallback){
   logger.info('['+MODULE_NAME+']['+exec_id+'] Execute: Begin.');

   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, wfcb);
      },
      // Get experiment
      function(exec, wfcb){
         getExperiment(exec.exp_id, null, function(error, exp){
            if(error) return wfcb(error);
            exec.exp = exp;
            wfcb(null, exec);
         });
      },
      // Get application
      function(exec, wfcb){
         getApplication(exec.exp.app_id, function(error, app){
            if(error) return wfcb(error);
            exec.app = app;
            wfcb(null, exec);
         });
      },
      // Get instance
      function(exec, wfcb){
         instmanager.getInstance(exec.inst_id, function(error, inst){
            if(error) return wfcb(error);
            exec.inst = inst;
            wfcb(null, exec);
         });
      },
      // Load checkpoint
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         if(!task.loaded_checkpoint && exec.launch_opts && exec.launch_opts.checkpoint_load){
            logger.info('['+MODULE_NAME+']['+exec_id+'] Execute: Loading checkpoint...');
            _loadCheckpointExecution(exec_id, function(error){
               if(error) return wfcb(error);
               // Checkpoint load success
               task.loaded_checkpoint = true;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{loaded_checkpoint: true}});
               wfcb(null, exec);
            });
         } else {
            // Skip
            wfcb(null, exec);
         }
      },
      // Execute execution script
      function(exec, wfcb){
         // Check task abort
         if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         var work_dir = exec.inst.image.workpath+"/"+exec.id;
         var exe_script = ''+
         '#!/bin/sh \n'+
         'cd '+work_dir+'\n'+
         'echo -n "executing" > EXPERIMENT_STATUS \n'+
         './'+exec.app.execution_script+' >> EXECUTION_LOG 2>&1 \n'+
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
            wfcb(true, exec);
         } else {
            // Execute job
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Execute: Launching execution script...');
            instmanager.executeJob(exec.inst.id, exe_script, work_dir, exec.inst.nodes, function (error, job_id) {
               if (error) {return wfcb(error);}

               // Update task and DB
               task.job_id = job_id;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: job_id}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

               wfcb(null, exec);
            });
         }
      }
   ],
   function(error, exec){
      if(error && error != true){return executionCallback(error);}
      logger.info('['+MODULE_NAME+']['+exec_id+'] Execute: Executing...');

      // Poll execution status
      _pollExecution(exec_id, false, function(error, status){
         if(error){return executionCallback(error);}

         // Wait for command completion
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Execute: Waiting - ' + task.job_id);
         instmanager.waitJob(task.job_id, exec.inst_id, function(error){
            if(error){return executionCallback(error);}

            // Poll execution status
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Execute: Job done, polling...');
            _pollExecution(exec_id, true, function(error, status){
               // Update task and DB
               task.job_id = null;
               database.db.collection('tasks').updateOne({id: task.id},{$set:{job_id: null}});

               // Check task abort
               if(taskmanager.isTaskAborted(task.id)) {return executionCallback(new Error("Task aborted"));}

               // Check status
               if(status != "executed"){
                  // Retry?
                  if(task.retries > 0){
                     logger.info('['+MODULE_NAME+']['+exec_id+'] Execute: Retries = '+task.retries+', retrying...');
                     // Decrease retry counter
                     task.retries--;
                     database.db.collection('tasks').updateOne({id: task.id},{$set:{retries: task.retries}});
                     // Relaunch execution
                     return _executeExecution(task, exec_id, executionCallback);
                  } else {
                     return executionCallback(new Error("Failed to execute experiment, status: "+status));
                  }
               }

               // End execution task
               logger.info('['+MODULE_NAME+']['+exec_id+'] Execute: Done.');
               executionCallback(null);
            });
         });
      });
   });
}

/**
 * Load experiment's checkpoint in an instance
 */
var _loadCheckpointExecution = function(exec_id, loadCallback){
   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, wfcb);
      },
      // Get instance
      function(exec, wfcb){
         instmanager.getInstance(exec.inst_id, function(error, inst){
            if(error) return wfcb(error);
            exec.inst = inst;
            wfcb(null, exec);
         });
      },
      // Get storage URL
      function(exec, wfcb){
         storage.client.invoke('getExecutionOutputURL', exec_id, function(error, url){
            if(error) return wfcb(error);
            exec.output_url = url;
            wfcb(null, exec);
         });
      },
      // Download checkpoint file
      function(exec, wfcb){
         var checkpoint_file = exec.output_url+"/checkpoint.tar.gz";

         // Execute command
         var cmd = "sshpass -p '"+constants.STORAGE_PASSWORD+"' rsync -e 'ssh -o StrictHostKeyChecking=no' "+url+" "+exec.inst.image.workpath+"/checkpoint.tar.gz";
         logger.debug('['+MODULE_NAME+']['+exec_id+'] LoadCheckpoint: Copying checkpoint file to working folder...');
         instmanager.executeCommand(exec.inst.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Decompress file
      function(exec, wfcb){
         // Execute command
         var cmd = "tar zxvf "+exec.inst.image.workpath+"/checkpoint.tar.gz";
         logger.debug('['+MODULE_NAME+']['+exec_id+'] LoadCheckpoint: Decompressing...');
         instmanager.executeCommand(exec.inst.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      }
   ],
   function(error){
      if(error){return loadCallback(error);}
      logger.debug('['+MODULE_NAME+']['+exec_id+'] LoadCheckpoint: Success.');
      loadCallback(null);
   });
}

/**
 * Retrieve experiment output data
 */
var _retrieveExecutionOutput = function(task, exec_id, retrieveCallback){
   logger.debug('['+MODULE_NAME+']['+exec_id+'] Retrieve: Begin.');

   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, function(error, exec){
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Get instance
      function(exec, wfcb){
         instmanager.getInstance(exec.inst_id, function(error, inst){
            if(error) return wfcb(error);
            exec.inst = inst;
            wfcb(null, exec);
         });
      },
      // Get storage URL
      function(exec, wfcb){
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Retrieve: Getting output URL...');
         storage.client.invoke('getExecutionOutputURL', exec_id, function(error, url){
            if(error) return wfcb(error);
            exec.output_url = url;
            wfcb(null, exec);
         });
      },
      // Update output files in storage
      function(exec, wfcb){
         // Check task abort
         if(task && taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

         logger.debug('['+MODULE_NAME+']['+exec_id+'] Getting execution output data path.');
         var output_files = exec.inst.image.outputpath+"/"+exec.id+"/*";

         // Execute command
         var cmd = "sshpass -p '"+constants.STORAGE_PASSWORD+"' rsync -Lre 'ssh -o StrictHostKeyChecking=no' "+output_files+" "+exec.output_url+"/ --delete-after";
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Retrieve: Copying output files to storage...');
         instmanager.executeCommand(exec.inst.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            // Check task abort
            if(task && taskmanager.isTaskAborted(task.id)) {return wfcb(new Error("Task aborted"));}

            // Reload output tree
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Retrieve: Copy completed.');
            reloadExecutionOutputTree(exec_id, wfcb);
         });
      }
   ],
   function(error){
      if(error) return retrieveCallback(error);
      logger.debug('['+MODULE_NAME+']['+exec_id+'] Retrieve: Done.');
      retrieveCallback(null);
   });
}

/**
 * Destroy an array of executions
 */
var _destroyExecutions = function(execs, cb){
   // The list is empty?
   if(!execs || execs.length == 0) return cb(null);

   // Iterate and destroy
   var tasks = [];
   for(var e = 0; e < execs.length; e++){
      // Destroy this execution
      (function(exec_id){
         tasks.push(function(taskcb){
            logger.debug('['+MODULE_NAME+']['+exec_id+'] DestroyExecutions: Destroying...');
            execmanager.destroyExecution(exec_id, false, function(error){
               return taskcb(error);
            });
         });
      })(execs[e].id);
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      return cb(error);
   });
}

/**
 * Cache to avoid polling again
 */
var _polling = {};

/**
 * Get execution status from target instance and update in DB.
 */
var _pollExecution = function(exec_id, force, pollCallback){
   // Check params
   if(!exec_id) return new Error('Parameter "exec_id" must be valid.');

   // Avoid multiple polling
   if(!_polling[exec_id]){
      _polling[exec_id] = true;

      logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Begin.');

      async.waterfall([
         // Get execution
         function(wfcb){
            execmanager.getExecution(exec_id, null, function(error, exec){
               if(error) return wfcb(error);
               wfcb(null, exec);
            });
         },
         // Get experiment
         function(exec, wfcb){
            getExperiment(exec.exp_id, null, function(error, exp){
               if(error) return wfcb(error);
               exec.exp = exp;
               wfcb(null, exec);
            });
         },
         // Get instance
         function(exec, wfcb){
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Getting instance...');
            instmanager.getInstance(exec.inst_id, function(error, inst){
               if(error) return wfcb(error);
               exec.inst = inst;
               wfcb(null, exec);
            });
         },
         // Checkpoint if activated
         function(exec, wfcb){
            if(exec.launch_opts && exec.launch_opts.checkpoint_interval > 0 && exec.status == "executing"){
               // Get current epoch
               var curr_date = Math.floor(new Date() / 1000.0);

               // Initialize in case
               if(!exec.checkpoint_last_date){
                  exec.checkpoint_last_date = curr_date;
                  database.db.collection('executions').updateOne({id: exec_id},{$set:{checkpoint_last_date:curr_date}});
               }

               // Check last checkpoint time
               logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Last checkpoint: '+exec.checkpoint_last_date+ ' - Curr: '+curr_date);
               if(exec.checkpoint_last_date + exec.launch_opts.checkpoint_interval < curr_date){
                  logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Beginning checkpointing...');
                  // Checkpoint
                  _checkpointExecution(exec_id, function(error){
                     // Update checkpoint time
                     exec.checkpoint_last_date = curr_date;
                     database.db.collection('executions').updateOne({id: exec_id},{$set:{checkpoint_last_date:curr_date}});
                     wfcb(error, exec);
                  });
               } else {
                  // No checkpoint needed
                  wfcb(null, exec);
               }
            } else {
               // No checkpoint activated
               wfcb(null, exec);
            }
         },
         // Poll experiment logs
         function(exec, wfcb){
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Polling logs...');
            var work_dir = exec.inst.image.workpath+"/"+exec_id;
            _pollExecutionLogs(exec_id, exec.inst_id, work_dir, ['COMPILATION_LOG','EXECUTION_LOG','*.log', '*.log.*', '*.bldlog.*'], function (error, logs) {
               if(error) return wfcb(error, exec);
               // Update status
               database.db.collection('executions').updateOne({id: exec_id},{$set:{logs:logs}});

               // Callback status
               wfcb(null, exec);
            });
         },
         // Retrieve experiment output data
         function(exec, wfcb){
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Polling output data...');
            _retrieveExecutionOutput(null, exec_id, function (error) {
               if(error) return wfcb(error, exec);
               wfcb(null, exec);
            });
         },
         // Poll experiment status
         function(exec, wfcb){
            var work_dir = exec.inst.image.workpath+"/"+exec.id;
            var cmd = 'cat '+work_dir+'/EXPERIMENT_STATUS';
            logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Polling status...');
            instmanager.executeCommand(exec.inst.id, cmd, function (error, output) {
               if(error) return wfcb(error, exec);

               // Get status
               var status = output.stdout;

               // Update status if the file exists
               if(status != "" && exec.status != "aborting"){
                  database.db.collection('executions').updateOne({id: exec_id},{$set:{status:status}});
               }

               // Callback status
               exec.status = status;
               wfcb(null, exec);
            });
         }
      ],
      function(error, exec){
         _polling[exec_id] = false;
         if(error) return pollCallback(error);
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Poll: Done - ' + exec.status);
         pollCallback(null, exec.status);
      });
   } else {
      // If force, full polling must be done
      if(force) return setTimeout(_pollExecution, 10000, exec_id, true, pollCallback);
      // If not, just wait current polling to end
      else return setTimeout(_waitPollFinish, 10000, exec_id, pollCallback);
   }
}

/**
 * Wait until polling is done and return status.
 */
var _waitPollFinish = function(exec_id, pollCallback){
   // Wait poll to end
   if(!_polling[exec_id]){
      // Finished!
      execmanager.getExecution(exec_id, null, function(error, exec){
         if(error) return pollCallback(error);
         return pollCallback(null, exec.status);
      });
   } else {
      return setTimeout(_waitPollFinish, 5000, exec_id, pollCallback);
   }
}

/**
 * Update experiment logs in DB.
 */
var _pollExecutionLogs = function(exec_id, inst_id, work_dir, log_files, pollCallback){
   var logs = [];

   // Experiment data
   logger.debug('['+MODULE_NAME+']['+exec_id+'] PollLogs: Getting experiment data...');
   execmanager.getExecution(exec_id, {logs:1}, function(error, exec){
      if(error) return pollCallback(error);

      // Get previous logs
      var prev_logs = exec.logs;
      if(!prev_logs) prev_logs = [];

      // Get log files list
      logger.debug('['+MODULE_NAME+']['+exec_id+'] PollLogs: Finding logs - ' + log_files);
      _findExecutionLogs(exec_id, inst_id, work_dir, log_files, function(error, loglist){
         if(error){
            _polling[exec_id] = false;
            logger.error('['+MODULE_NAME+']['+exec_id+'] PollLogs: Error finding logs.');
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
                        logger.debug('['+MODULE_NAME+']['+exec_id+'] PollLogs: No changes in log content - ' + log_filename);
                        logs.push({name: log_filename, content: prev_log.content, last_modified: log_date});
                        taskcb(null);
                     } else {
                        // Update log
                        var cmd = 'zcat -f '+loglist[i];
                        logger.debug('['+MODULE_NAME+']['+exec_id+'] PollLogs: Updating log content - ' + log_filename + ' : ' + log_date + " ...");
                        instmanager.executeCommand(inst_id, cmd, function (error, output) {
                           if(error) return taskcb(new Error("Failed to poll log "+ loglist[i]+ ", error: "+ error));

                           // Get log content
                           var content = output.stdout;

                           // Add log
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
            logger.debug('['+MODULE_NAME+']['+exec_id+'] PollLogs: Done.');
            pollCallback(null, logs);
         });
      });

   });
}

/**
 * Get log paths
 */
var _findExecutionLogs = function(exec_id, inst_id, work_dir, log_files, findCallback){
   // No log files
   if(!log_files || log_files.length <= 0) return findCallback(new Error("No log files specified."));

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
 * Remove invalid status executions in instances
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
      var execs = inst.execs;
      if(execs){
         for(var i = 0; i < execs.length; i++){
            var exec_id = execs[i].exec_id;
            (function(exec_id, inst_id){
               execmanager.getExecution(exec_id, null, function(error, exec){
                  if(!exec || exec.status == "created" || exec.status == "done" || exec.status == "failed_compilation" || exec.status == "failed_execution"){
                     // Do not remove from instance in debug mode
                     if(!exec || !exec.launch_opts || !exec.launch_opts.debug){
                        // Remove experiment from this instance
                        cleanExecution(exec_id, function(error){
                           if(error){
                              logger.error('['+MODULE_NAME+']['+exec_id+'] CleanInstances: Error cleaning execution from instance - ' + inst.id + ' : ' + error);
                           }
                           // Clean orphan instance
                           instmanager.cleanExecution(exec_id, inst_id, {b_input: true, b_output: true, b_sources: true, b_remove: true}, function(error){
                              logger.info('['+MODULE_NAME+']['+exec_id+'] CleanInstances: Cleaned instance - ' + inst.id);
                           });
                        });
                     }
                  }
               });
            })(exec_id, inst.id);
         }
      }
   });
}

/**
 * Remove data from inexistent executions
 */
var _cleanStorage = function(){
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_cleanStorage, 1000);
   }

   // Get list of IDs in output storage
   storage.client.invoke('getOutputIDs', function(error, ids){
      if(error) return logger.error('['+MODULE_NAME+'] CleanStorage: Failed to automatic get output folder IDs: '+error);

      // Iterate IDs and check if they exists
      for(var i = 0; i < ids.length; i++){
         (function(exec_id){
            execmanager.getExecution(exec_id, null, function(error, exec){
               if(error || exec.status == "deleted"){
                  // This folder should not exists
                  storage.client.invoke('deleteExecutionOutput', exec_id, null, function(error){
                     if(error) return logger.error('['+MODULE_NAME+'] CleanStorage: Failed to clean execution "'+exec_id+'" output folder: '+error);
                     return logger.debug('['+MODULE_NAME+'] CleanStorage: Execution "'+exec_id+'" output folder has been removed.');
                  });
               }
            });
         })(ids[i]);
      }
   });

   // Get list of IDs in input storage
   storage.client.invoke('getInputIDs', function(error, ids){
      if(error) return logger.error('['+MODULE_NAME+'] CleanStorage: Failed to automatic get input folder IDs: '+error);

      // Iterate IDs and check if they exists
      for(var i = 0; i < ids.length; i++){
         (function(exp_id){
            getExperiment(exp_id, null, function(error, exp){
               if(error){
                  // Is App?
                  getApplication(exp_id, function(error, app){
                     if(error){
                        // This folder should not exists
                        storage.client.invoke('deleteExperimentInput', exp_id, null, null, function(error){
                           if(error) return logger.error('['+MODULE_NAME+'] CleanStorage: Failed to clean experiment "'+exp_id+'" input folder: '+error);
                           return logger.debug('['+MODULE_NAME+'] CleanStorage: Experiment "'+exp_id+'" input folder has been removed.');
                        });
                     }
                  });
               }
            });
         })(ids[i]);
      }
   });
}

/**
 * Clean orphan executions in DB
 */
var _cleanExecutions = function(cb){
   // Do not search deleted executions
   var query = {
      status: {
         "$ne": "deleted"
      }
   };

   // Return only IDs and status
   var projection = {
      id: 1,
      exp_id: 1,
      status: 1
   }

   // Get executions
   database.db.collection('executions').find(query, projection).toArray(function(error, execs){
      // No matches
      if(error || !execs) return cb(error);

      // Iterate executions
      var tasks = [];
      for(var e = 0; e < execs.length; e++){
         // Destroy this execution
         (function(exec){
            tasks.push(function(taskcb){
               // Get experiment
               getExperiment(exec.exp_id, null, function(error, exp){
                  if(error && exec.status != "deleted"){
                     // Experiment does not exists
                     // Status must be "deleted"
                     logger.debug('['+MODULE_NAME+']['+exec.id+'] CleanExecutions: Execution "'+exec.id+'" status is not "deleted", fixing...');
                     database.db.collection('executions').updateOne({id: exec.id},{$set:{status:"deleted"}});
                  }
                  return taskcb(null);
               });
            });
         })(execs[e]);
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         return cb(error);
      });
   });
}


/**
 * Poll executions
 */
var _pollExecutingExperiments = function(){
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_pollExecutingExperiments, 1000);
   }

   // Iterate executions
   database.db.collection('executions').find({
      status: { $in: ["deployed", "compiling", "executing"]}
   }).forEach(function(exec){
      // Poll execution status
      // logger.debug('['+MODULE_NAME+']['+exec.id+'] PollExecuting: Polling...');
      _pollExecution(exec.id, false, function(error, status){
         if(error) logger.error('['+MODULE_NAME+']['+exec.id+'] Failed to automatic poll: '+error);
      });
   });
}


/***********************************************************
 * --------------------------------------------------------
 * CHECKPOINTING
 * --------------------------------------------------------
 ***********************************************************/
var _checkpointExecution = function(exec_id, cb){
   logger.info('['+MODULE_NAME+']['+exec_id+'] Checkpoint: Begin.');
   async.waterfall([
      // Get execution
      function(wfcb){
         execmanager.getExecution(exec_id, null, wfcb);
      },
      // Get instance
      function(exec, wfcb){
         if(exec.inst_id){
            instmanager.getInstance(exec.inst_id, function(error, inst){
               if(error) return wfcb(error);
               exec.inst = inst;
               wfcb(null, exec);
            });
         } else {
            logger.warning('['+MODULE_NAME+']['+exec_id+'] Checkpoint: No instance for this execution.');
            // Skip to end
            wfcb(0, exec);
         }
      },
      // Compress working directory
      function(exec, wfcb){
         // Checkpointing command (tar.gz)
         var work_dir = exec.inst.image.workpath+"/"+exec.id;
         var cmd = "cd "+work_dir+" && tar czvf checkpoint.tar.gz *";
         logger.info('['+MODULE_NAME+']['+exec_id+'] Checkpoint: Compressing data...');
         instmanager.executeCommand(exec.inst_id, cmd, function (error, output) {
            if(error) return wfcb(error);
            wfcb(null, exec);
         });
      },
      // Save checkpoint to output
      function(exec, wfcb){
         var work_dir = exec.inst.image.workpath+"/"+exec.id;
         var cmd = "cd "+work_dir+" && mv checkpoint.tar.gz "+exec.inst.image.outputpath+"/"+exec.id+"/checkpoint.tar.gz";
         logger.info('['+MODULE_NAME+']['+exec_id+'] Checkpoint: Moving to output path...');
         instmanager.executeCommand(exec.inst.id, cmd, function (error, output) {
            if(error) return wfcb(error);
            wfcb(null);
         });
      }
   ],
   function(error){
      if(error){
         // Error trying to checkpoint execution
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Checkpoint: Error.');
         cb(error);
      } else {
         // Callback
         logger.debug('['+MODULE_NAME+']['+exec_id+'] Checkpoint: Done, saved into output folder.');
         cb(null);
      }
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
   },
   // Init execution manager
   function(wfcb){
      logger.info('['+MODULE_NAME+'] Cleaning orphan executions...');
      _cleanExecutions(wfcb);
   }
],
function(error){
   if(error) throw error;
   logger.info('['+MODULE_NAME+'] Initialization completed.');

   // Remove non executing experiments from instances
   _cleanInstances();
   // Remove orphan output folders
   _cleanStorage();
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
exports.maintainExperiment = maintainExperiment;
exports.createExperiment = createExperiment;
exports.updateExperiment = updateExperiment;
exports.destroyExperiment = destroyExperiment;
exports.searchExperiments = searchExperiments;
exports.launchExperiment = launchExperiment;

exports.getExperimentCode = getExperimentCode;
exports.putExperimentCode = putExperimentCode;
exports.deleteExperimentCode = deleteExperimentCode;
exports.putExperimentInput = putExperimentInput;
exports.deleteExperimentInput = deleteExperimentInput;
exports.reloadExperimentTree = reloadExperimentTree;
exports.reloadExecutionOutputTree = reloadExecutionOutputTree;
exports.getExecutionOutputFile = getExecutionOutputFile;

exports.cleanExecution = cleanExecution;
exports.abortExecution = abortExecution;
exports.destroyExecution = destroyExecution;
