/**
 * Copyright 2017 University of Castilla - La Mancha
 */
var zerorpc = require('zerorpc');
var async = require('async');
var database = require('./database.js');
var logger = require('./utils.js').logger;

/**
 * Module name
 */
var MODULE_NAME = "IM";

/**
 * Module vars
 */
var dMinionClients = {};
var vMinionClients = [];
var destroyInterval = 30000;
var constants = {};

/**
 * Retrieves available sizes
 * @param {String} - OPTIONAL - Sizes for this minion
 */
var getAvailableSizes = function(minion, getCallback){
   var sizes = [];
   getCallback(null, sizes);
}

/**
 * Obtains a dedicated instance.
 * It could be an existing one or a newly instanced one
 * @param {String} - Size ID.
 * @param {String} - Image ID.
 */
var requestInstance = function(name, image_id, size_id, nodes, requestCallback){
   // Get image
   getImage(image_id, function(error, image){
      if(error) return requestCallback(error);

      // Get size
      getSize(size_id, function(error, size){
         if(error) return requestCallback(error);

         // Get minion
         var minion = _getMinion(image.minion, false, function(error, minion){
            if(error) return requestCallback(error);

            // Minion must be online
            if(minion.online == false) {
               logger.error('['+MODULE_NAME+'] requestInstance: Minion "'+id+'" is offline.');
               return requestCallback(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');
            }

            // Check quotas
            getImageQuotas(image_id, function(error, quotas){
               if(error) return requestCallback(error);

               // Enough quotas?
               if(quotas.instances.in_use + nodes > quotas.instances.limit) return requestCallback(new Error('Not enough instances quota.'));
               if(quotas.cores.in_use + (nodes * size.cpus) > quotas.cores.limit) return requestCallback(new Error('Not enough cores quota.'));
               if(quotas.ram.in_use + size.ram > quotas.ram.limit) return requestCallback(new Error('Not enough RAM quota.'));

               // Instance
               minion.invoke('createInstance', {
                  name: name,
                  image_id: image_id,
                  size_id: size_id,
                  nodes: nodes,
                  publicIP: true
               }, function (error, instance_id) {
                  // Heartbeat? Connection to minion has been lost
                  if(error && error.name == "HeartbeatError") return requestCallback(new Error('Minion cannot instance because is OFFLINE (HeartbeatError).').name = 'OfflineMinion');
                  if(error) return requestCallback(error);

                  // Wait until instance is ready
                  _waitInstanceReady(instance_id, requestCallback);
               });
            });
         });
      });
   });
}

/**
 * Wait until instance is ready or failed
 */
var _waitInstanceReady = function(inst_id, readyCallback){
   logger.debug('['+MODULE_NAME+']['+inst_id+'] _waitInstanceReady: Waiting...');
   getInstance(inst_id, function(error, inst){
      if(error || !inst) return readyCallback(new Error('Instance with ID "'+inst_id+'" does not exists'));
      if(inst.ready == true) return readyCallback(null, inst_id);
      if(inst.failed == true) return readyCallback(new Error('Failed to instance "'+inst_id+'".'));
      setTimeout(_waitInstanceReady, 10000, inst_id, readyCallback);
   });
}


/**
 * Get instance metadata
 * @param {String} - Instance ID
 */
var getInstance = function(inst_id, getCallback){
   // Get instance
   database.db.collection('instances').findOne({_id: inst_id}, function(error, inst){
      if(error || !inst) return getCallback(new Error('Instance with ID "'+inst_id+'" does not exists'));
      getCallback(null, inst);
   });
}

/**
 * Get image metadata
 * @param {String} - Image ID
 */
var getImage = function(image_id, getCallback){
   database.db.collection('images').findOne({_id: image_id}, function(error, image){
      if(error || !image){
         getCallback(new Error('Image with ID "'+image_id+'" does not exists'));
      } else {
         getCallback(null, image);
      }
   });
}

/**
 * Get size metadata
 * @param {String} - Size ID
 */
var getSize = function(size_id, getCallback){
   database.db.collection('sizes').findOne({_id: size_id}, function(error, size){
      if(error || !size){
         getCallback(new Error('Size with ID "'+size_id+'" does not exists'));
      } else {
         getCallback(null, size);
      }
   });
}

/**
 * Get images list
 */
var getImagesList = function(getCallback){
   // Iterate minions
   var tasks = [];
   var list = [];
   for(var i = 0; i < vMinionClients.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var minion = vMinionClients[i];
            // Check if minion is online
            if(minion.online == true){
               minion.invoke('getImages', null, function (error, minion_images) {
                  if(!error){
                     // Add to list
                     list = list.concat(minion_images);
                  }
                  taskcb(null);
               });
            } else {
               taskcb(null);
            }
         });
      })(i);
   }
   async.parallel(tasks, function(error){
      if(error) return getCallback(error);
      getCallback(null, list);
   });
}

/**
 * Get sizes list
 */
var getSizesList = function(getCallback){
   // Iterate minions
   var tasks = [];
   var list = [];
   for(var i = 0; i < vMinionClients.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var minion = vMinionClients[i];
            // Check if minion is online
            if(minion.online == true){
               minion.invoke('getSizes', null, function (error, minion_sizes) {
                  if(!error){
                     // Add to list
                     list = list.concat(minion_sizes);
                  }
                  taskcb(null);
               });
            } else {
               taskcb(null);
            }
         });
      })(i);
   }
   async.parallel(tasks, function(error){
      if(error) return getCallback(error);
      getCallback(null, list);
   });
}

/**
 * Get instances list
 */
var getInstancesList = function(getCallback){
   // Iterate minions
   var tasks = [];
   var list = [];
   for(var i = 0; i < vMinionClients.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var minion = vMinionClients[i];
            if(minion.online == true){
               minion.invoke('getInstances', null, function (error, minion_insts) {
                  if(!error){
                     // Add to list
                     list = list.concat(minion_insts);
                  }
                  taskcb(null);
               });
            } else {
               taskcb(null);
            }
         });
      })(i);
   }
   async.parallel(tasks, function(error){
      if(error) return getCallback(error);
      getCallback(null, list);
   });
}

/**
 * Get quotas for an image
 */
var getImageQuotas = function(image_id, getCallback){
   getImage(image_id, function(error, image){
      if(error) return getCallback(error);

      // Get minion
      var minion = _getMinion(image.minion, false, function(error, minion){
         if(error) return getCallback(error);

         // Minion must be online
         if(minion.online == false) return getCallback(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

         // Get quotas
         minion.invoke('getQuotas', function (error, quotas) {
            if(error && error.name == "HeartbeatError") return getCallback(new Error('Minion cannot get image quotas because is OFFLINE (HeartbeatError).').name = 'OfflineMinion');
            if(error) return getCallback(error);
            getCallback(null, quotas);
         });
      });
   });
}

/**
 * Destroy instance
 */
var destroyInstance = function(inst_id, destroyCallback){
   // Get instance
   getInstance(inst_id, function(error, inst){
      if(error) return destroyCallback(error);

      // Get minion
      var minion = _getMinion(inst.minion, false, function(error, minion){
         if(error) return destroyCallback(error);

         // Minion must be online
         if(minion.online == false) return destroyCallback(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

         // Destroy instance
         minion.invoke('destroyInstance', inst_id, function (error) {
            if(error && error.name == "HeartbeatError") return destroyCallback(new Error('Minion cannot destroy instance because is OFFLINE (HeartbeatError).').name = 'OfflineMinion');
            if(error) return destroyCallback(new Error("Failed to destroy instance " + inst_id + ", err: " + error));
            destroyCallback(null);
         });
      });
   });
}

/**
 * Remove experiment from instance
 */
var cleanExecution = function(exec_id, inst_id, b_flags, cleanCallback){
   if(!b_flags) b_flags = {};

   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // Get minion for this instance
      function(inst, wfcb){
         var minion = _getMinion(inst.minion, false, function(error, minion){
            if(error) return wfcb(error);

            // Minion must be online
            if(minion.online == false) return wfcb(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

            wfcb(null, inst, minion);
         });
      },
      // Clean jobs in this instance
      function(inst, minion, wfcb){
         logger.debug('['+MODULE_NAME+']['+inst.id+'] Clean: Aborting jobs...');
         _abortInstanceJobs(exec_id, inst_id, function(error){
            wfcb(error, inst, minion);
         });
      },
      // Clean code
      function(inst, minion, wfcb){
         if(b_flags.b_remove || b_flags.b_code){
            logger.debug('['+MODULE_NAME+']['+inst.id+'] Clean: Cleaning code...');
            _cleanExecutionCode(minion, exec_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null, inst, minion);
            });
         } else {
            wfcb(null, inst, minion);
         }
      },
      // Clean input
      function(inst, minion, wfcb){
         if(b_flags.b_remove || b_flags.b_input){
            logger.debug('['+MODULE_NAME+']['+inst.id+'] Clean: Cleaning input...');
            _cleanExecutionInput(minion, exec_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null, inst, minion);
            });
         } else {
            wfcb(null, inst, minion);
         }
      },
      // Clean output
      function(inst, minion, wfcb){
         if(b_flags.b_remove || b_flags.b_output){
            logger.debug('['+MODULE_NAME+']['+inst.id+'] Clean: Cleaning output...');
            _cleanExecutionOutput(minion, exec_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      }
   ],
   function(error){
      // If error and not force
      if(error && !b_flags.b_force) return cleanCallback(error);

      if(b_flags.b_remove){
         // Remove from database
         logger.debug('['+MODULE_NAME+']['+inst_id+'] Clean: Removing execution from instance...');
         database.db.collection('instances').updateOne({id: inst_id},{
            $pull: {execs: {exec_id: exec_id}}
         });
         database.db.collection('instances').updateOne({id: inst_id},{
            $set: {in_use: false}
         });
      }
      cleanCallback(error);
   });
}

/**
 * Add experiment to instance experiments
 */
var addExecution = function(exec_id, inst_id){
   // Get instance
   getInstance(inst_id, function(error, inst){
      if(error) return logger.error('['+MODULE_NAME+']['+inst_id+'] AddExperiment: Error - ' + error);

      // Check if already added
      if(!inst.execs[exec_id]){
         // Add experiment to instance
         database.db.collection('instances').updateOne({
            'id': inst_id,
         },{
            $push: {'execs': {exec_id: exec_id, jobs: []}}
         });
      } else {
         logger.info('['+MODULE_NAME+']['+inst_id+'] Execution "'+exec_id+'" is already in instance.');
      }
   });
}

/**
 * Execute a command in an instance and wait for return
 */
var executeCommand = function(inst_id, cmd, executeCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // Get minion for this instance
      function(inst, wfcb){
         var minion = _getMinion(inst.minion, true, function(error, minion){
            if(error) return wfcb(error);

            // Minion must be online
            if(minion.online == false) return executeCallback(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

            wfcb(null, inst, minion);
         });
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeCommand', inst_id, cmd, function (error, result) {
            if (error) {
               wfcb(error);
            } else {
               wfcb(null, result);
            }
         });
      }
   ],
   function(error, result){
      // Heartbeat? Connection to minion has been lost
      if(error && error.name == "HeartbeatError") return executeCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
      if(error) return executeCallback(error);
      // Callback
      executeCallback(null, result);
   });
}

/**
 * Execute a command in an instance but as a job
 */
var executeJob = function(inst_id, cmd, work_dir, nodes, executeCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // Get minion for this instance
      function(inst, wfcb){
         var minion = _getMinion(inst.minion, true, function(error, minion){
            if(error){
               logger.error('['+MODULE_NAME+']['+inst_id+'] ExecuteJob: Failed to get minion data - ' + error);
               return wfcb(error);
            }

            // Minion must be online
            if(minion.online == false) return wfcb(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

            wfcb(null, inst, minion);
         });
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeScript', inst_id, cmd, work_dir, nodes, function (error, job_id) {
            if (error) {
               logger.error('['+MODULE_NAME+']['+inst_id+'] ExecuteJob: Failed to execute job in instance - ' + error);
               return wfcb(error);
            } else {
               if(!job_id) return wfcb(new Error('Minion did not return a valid Job ID - '+job_id));
               logger.debug('['+MODULE_NAME+']['+inst_id+'] ExecuteJob: ID returned - ' + job_id);
               return wfcb(null, job_id);
            }
         });
      }
   ],
   function(error, job_id){
      // Heartbeat? Connection to minion has been lost
      if(error && error.name == "HeartbeatError") return executeCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
      if(error) return executeCallback(error);

      // Callback
      return executeCallback(null, job_id);
   });
}

var waitJob = function(job_id, inst_id, waitCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // Get minion for this instance
      function(inst, wfcb){
         var minion = _getMinion(inst.minion, true, function(error, minion){
            if(error) return wfcb(error);

            // Minion must be online
            if(minion.online == false) return wfcb(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

            wfcb(null, inst, minion);
         });
      },
      // Get job status
      function(inst, minion, wfcb){
         minion.invoke('getJobStatus', job_id, inst_id, function (error, status) {
            // Heartbeat? Connection to minion has been lost
            if(error && error.name == "HeartbeatError") return wfcb(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
            if (error) {
               wfcb(new Error("Failed to get job status from instance '" + inst_id + "': " + error));
            } else {
               if(status == "finished" || status == "unknown"){
                  // Done
                  wfcb(null, inst, minion);
               } else {
                  // Running
                  wfcb(true);
               }
            }
         });
      },
      // Get job stdout
      function(inst, minion, wfcb){
         var output = {
            stdout: null,
            stderr: null,
            code: null
         };

         // Retrieve output
         var cmd = 'cat '+inst.image.tmppath+'/'+ job_id + '.stdout';
         minion.invoke('executeCommand', inst_id, cmd, function (error, result) {
            // Heartbeat? Connection to minion has been lost
            if(error && error.name == "HeartbeatError") return wfcb(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
            if (error) return wfcb(error);
            output.stdout = result.stdout;

            // Retrieve error
            var cmd = 'cat '+inst.image.tmppath+'/'+ job_id + '.stderr';
            minion.invoke('executeCommand', inst_id, cmd, function (error, result) {
               // Heartbeat? Connection to minion has been lost
               if(error && error.name == "HeartbeatError") return wfcb(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
               if (error) return wfcb(error);
               output.stderr = result.stdout;

               // Retrieve code
               var cmd = 'cat '+inst.image.tmppath+'/'+ job_id + '.code';
               minion.invoke('executeCommand', inst_id, cmd, function (error, result) {
                  // Heartbeat? Connection to minion has been lost
                  if(error && error.name == "HeartbeatError") return wfcb(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
                  if (error) return wfcb(error);
                  output.code = result.stdout;

                  // Remove files
                  var cmd = 'rm '+inst.image.tmppath+'/'+ job_id + '.*';
                  minion.invoke('executeCommand', inst_id, cmd, function (error, result) {
                     // Heartbeat? Connection to minion has been lost
                     if(error && error.name == "HeartbeatError") return wfcb(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
                     if (error) return wfcb(error);
                     return wfcb(null, output);
                  });
               });
            });
         });
      },
   ],
   function(error, output){
      if(error == true) return setTimeout(waitJob, 1000, job_id, inst_id, waitCallback);
      return waitCallback(error, output);
   });
}

/**
 * Clean experiment Job in instance
 */
var abortJob = function(job_id, inst_id, abortCallback){
   if(job_id && inst_id){
      // Get instance
      getInstance(inst_id, function(error, inst){
         if(error) return abortCallback(error);

         // Get minion
         var minion = _getMinion(inst.minion, false, function(error, minion){
            if(error) return abortCallback(error);

            // Minion must be online
            if(minion.online == false) return abortCallback(new Error('Minion for this image is OFFLINE.').name = 'OfflineMinion');

            // Abort job
            logger.debug('['+MODULE_NAME+']['+inst_id+'] Aborting job - ' + job_id);
            minion.invoke('cleanJob', job_id, inst_id, function (error, result) {
               // Heartbeat? Connection to minion has been lost
               if(error && error.name == "HeartbeatError") return abortCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
               if(error) return abortCallback(error);

               // Callback
               logger.debug('['+MODULE_NAME+']['+inst_id+'] Aborted job - ' + job_id);
               abortCallback(null);
            });
         });
      });
   }
}

/**
 * Get minion.
 * If the minion does not response, wait.
 */
var _getMinion = function(id, wait, cb){
   var minion = dMinionClients[id];

   // Check if minion is loaded
   if(!minion){
      // Wait
      if(wait == true) return setTimeout(_getMinion, 10000, id, wait, cb);
      else {
         var error = new Error('_getMinion: Minion "'+id+'" is not LOADED.');
         logger.error('['+MODULE_NAME+'] '+error);
         return cb(error);
      }
   }

   return cb(null, minion);
}

/**
 * Clean experiment code in instance
 */
var _cleanExecutionCode = function(minion, exec_id, inst, cleanCallback){
   var work_dir = inst.image.workpath+"/"+exec_id;
   var cmd = 'rm -rf '+work_dir;
   // Execute command
   minion.invoke('executeCommand', inst.id, cmd, function (error, output) {
      // Heartbeat? Connection to minion has been lost
      if(error && error.name == "HeartbeatError") return cleanCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
      return cleanCallback(error);
   });
}

/**
 * Clean experiment input data in instance
 */
var _cleanExecutionInput = function(minion, exec_id, inst, cleanCallback){
   // Remove experiment code folder
   var input_dir = inst.image.inputpath+"/"+exec_id;
   var cmd = 'rm -rf '+input_dir;
   // Execute command
   minion.invoke('executeCommand', inst.id, cmd, function (error, output) {
      // Heartbeat? Connection to minion has been lost
      if(error && error.name == "HeartbeatError") return cleanCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
      return cleanCallback(error);
   });
}

/**
 * Clean experiment output data in instance
 */
var _cleanExecutionOutput = function(minion, exec_id, inst, cleanCallback){
   // Remove experiment code folder
   var output_dir = inst.image.outputpath+"/"+exec_id;
   var cmd = 'rm -rf '+output_dir;
   // Execute command
   minion.invoke('executeCommand', inst.id, cmd, function (error, output) {
      // Heartbeat? Connection to minion has been lost
      if(error && error.name == "HeartbeatError") return cleanCallback(new Error('Minion for this instance is OFFLINE.').name = 'OfflineMinion');
      return cleanCallback(error);
   });
}

/**
 * List empty instances using scalability algorithms
 */
var _getSuperfluousInstances = function(listCallback){
   // TODO: Select instances based on usability/scalability algorithms
   // ...
   //
   // Select all empty for now
   getInstancesList(function(error, insts){
      if(error) return listCallback(error);

      // Result list
      var retList = [];

      // Iterate list
      for(var i = 0; i < insts.length; i++){
         // Empty?
         if(insts[i].ready && !insts[i].in_use && (!insts[i].execs || insts[i].execs.length == 0)){
            retList.push(insts[i]);
         }
      }

      // Return list
      listCallback(null, retList);
   });
}

/**
 * Abort experiment jobs in an instance
 */
var _abortInstanceJobs = function(exec_id, inst_id, abortCallback){
   // Get instance data
   getInstance(inst_id, function(error, inst){
      if(error) return abortCallback(error);

      // Iterate experiments in instance
      var tasks = [];
      for(var e = 0; e < inst.execs.length; e++){
         if(inst.execs[e].exec_id == exec_id){
            // Experiment found in this instance
            // Iterate jobs and abort
            var jobs = inst.execs[e].jobs;
            for(var i = 0; i< inst.execs.length; i++){
               var job_id = jobs[i];
               // Abort this job
               (function(job_id){
                  if(job_id){
                     tasks.push(function(taskcb){
                        abortJob(job_id, inst_id, function(error){
                           return taskcb(error);
                        });
                     });
                  }
               })(job_id);
            }
         }
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         return abortCallback(error);
      });

   });
}

/**
 * Destroy empty instances
 */
var _destroyEmptyInstances = function(destroyCallback){
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_destroyEmptyInstances, 1000, destroyCallback);
   }

   // Scalability code
   _getSuperfluousInstances(function(error, list){
      if(error) return destroyCallback(error);

      if(list.length > 0){
         logger.info('['+MODULE_NAME+'] DestroyEmpty: Destroying empty instances: '+ list.length);
      } else {
         return destroyCallback(null);
      }

      // Destroy instances task
      var tasks = [];
      for(var i = 0; i < list.length; i++){
         (function(i){
            tasks.push(function(taskcb){
               // Destroy instance
               logger.info('['+MODULE_NAME+']['+list[i].id+'] DestroyEmpty: Destroying...');
               destroyInstance(list[i].id, function(error){
                  //if(error){
                  //   logger.error('['+MODULE_NAME+']['+list[i].id+'] DestroyEmpty: Failed to destroy instance - ' + error);
                  //} else {
                  //   logger.info('['+MODULE_NAME+']['+list[i].id+'] DestroyEmpty: Done.');
                  //}

                  // Always return no error
                  taskcb(null);
               });
            });
         })(i);
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         logger.info('['+MODULE_NAME+'] DestroyEmpty: Finished.');
         if(destroyCallback) return destroyCallback(error);
      });
   });
}

/**
 * Poll minion
 */
var _pollMinionLock = {};
var _pollMinionOnline = function(minion){
   // Avoid multiple polling
   if(_pollMinionLock[minion.minion_url] == true) return;
   _pollMinionLock[minion.minion_url] = true;

   // Get minion name
   minion.invoke('getMinionName', function (error, name) {
      if(error){
         logger.info('['+MODULE_NAME+'] Minion in '+minion.minion_url+' if offline.');
         // Mark minion as offline
         minion.online = false;
         // Retry connection
         minion.connect(minion.minion_url);
      }
      // Swap online status
      else if(minion.online == false){
         // Is online
         logger.info('['+MODULE_NAME+'] Minion in '+minion.minion_url+' is online.');
         minion.online = true;
         dMinionClients[name] = minion;
      }

      _pollMinionLock[minion.minion_url] = false;
   });
}

/**
 * Update minions.
 */
var autoupdate = function(cb){
   logger.info('['+MODULE_NAME+'] Autoupdate: Begin.');
   var tasks = [];
   // Iterate minions
   for(var i in vMinionClients){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var minion = vMinionClients[i];
            if(minion.online == true){
               minion.invoke('autoupdate', function (error) {
                  if(error){
                     logger.error('['+MODULE_NAME+'] Failed to autoupdate minion '+minion.minion_url+'.');
                  }
                  return taskcb(null);
               });
            } else {
               return taskcb(null);
            }
         });
      })(i);
   }

   // Restart minions
   async.parallel(tasks, function(error){
      // Callback
      return cb(error);
   });
}

/**
 * Initialize minions
 */
var init = function(cfg, initCallback){
   // Set constants
   constants = cfg;

   var tasks = [];
   for(var i = 0; i < constants.MINION_URL.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var minion = new zerorpc.Client({
               heartbeatInterval: 30000,
               timeout: 3600
            });

            // Connect
            minion.minion_url = constants.MINION_URL[i];
            minion.online = false;
            minion.connect(minion.minion_url);
            _pollMinionOnline(minion);

            // Poll minions' status (5 sec)
            setInterval(function(){
               _pollMinionOnline(minion);
            }, 5000);

            // Add to minions list
            logger.debug('['+MODULE_NAME+'] Registered minion in '+minion.minion_url);
            vMinionClients.push(minion);
            taskcb(null);
         });
      })(i);
   }
   async.parallel(tasks, function(error){
      if(error) initCallback(error);
      initCallback(null);

      /**
       * Task: Remove empty instances from the system
       */
      var _lock_destroy = true;
      _destroyEmptyInstances(function(error){
         if(error) logger.error('['+MODULE_NAME+'] DestroyEmpty: Failed - ' + error);
         _lock_destroy = false;
      });
      // Destroy empty instances periodically
      setInterval(function(){
         if(_lock_destroy == true) return;
         _lock_destroy = true;
         _destroyEmptyInstances(function(error){
            _lock_destroy = false;
            if(error) console.error('['+MODULE_NAME+'] DestroyEmpty: Failed - ' + error);
         });
      }, destroyInterval);
   });
}

module.exports.init = init;
module.exports.autoupdate = autoupdate;

module.exports.getInstance = getInstance;
module.exports.getImage = getImage;
module.exports.getSize = getSize;

module.exports.getImagesList = getImagesList;
module.exports.getSizesList = getSizesList;
module.exports.getInstancesList = getInstancesList;

module.exports.getImageQuotas = getImageQuotas;

module.exports.requestInstance = requestInstance;
module.exports.destroyInstance = destroyInstance;

module.exports.addExecution = addExecution;
module.exports.executeCommand = executeCommand;
module.exports.executeJob = executeJob;
module.exports.waitJob = waitJob;
module.exports.abortJob = abortJob;

module.exports.cleanExecution = cleanExecution;
