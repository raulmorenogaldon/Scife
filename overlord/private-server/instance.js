var zerorpc = require('zerorpc');
var async = require('async');
var constants = require('./constants.json');
var database = require('./database.js');

/**
 * Module name
 */
var MODULE_NAME = "IM";

/**
 * Module vars
 */
var minionClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});
var destroyInterval = 10000;

/**
 * Retrieves available sizes
 * @param {String} - OPTIONAL - Sizes for this minion
 */
var getAvailableSizes = function(minion, getCallback){
   var sizes = [];
   getCallback(null, sizes);
}

/**
 * Defines a system
 */
var defineSystem = function(nodes, image_id, size_id, defineCallback){
   getImage(image_id, function(error, image){
      if(error) defineCallback(error, null);
      getSize(size_id, function(error, size){
         if(error) defineCallback(error, null);

         // Define system data
         var system = {
            status: "defined",
            nodes: nodes,
            image: image,
            size: size,
            instances: []
         };

         // Return
         return defineCallback(null, system);
      });
   });
}

/**
 * Instance a system
 */
var instanceSystem = function(system, instanceCallback){
   // TODO: Set first instance as master
   // Master tasks...

   // Create instances tasks
   var tasks = [];
   for(i = 0; i < system.nodes; i++){
      tasks.push(function(taskcb){
         requestInstance(system.image.id, system.size.id, function (error, inst_id) {
            if(error){
               taskcb(error);
            } else {
               // Add instance to system
               system.instances.push(inst_id);
               // Add system to instance
               database.db.collection('instances').updateOne({id: inst_id},{
                  $set: { system: true}
               });
               taskcb(null);
            }
         });
      });
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error){
         instanceCallback(error, null);
      } else {
         // Callback with instanced system
         system.status = "instanced";
         instanceCallback(null, system);
      }
   });
}

/**
 * Remove instances from the system
 */
var cleanSystem = function(system, cleanCallback){
   // Remove system from instances
   if(system.instances){
      for(var inst_id in system.instances){
         database.db.collection('instances').updateOne({id: inst_id},{
            $set: { system: false}
         });
      }
   }

   // Clean instance list
   system.instances = [];
   system.status = "defined";

   return cleanCallback(null, system);
}

/**
 * Obtains a dedicated instance.
 * It could be an existing one or a newly instanced one
 * @param {String} - Size ID.
 * @param {String} - Image ID.
 */
var requestInstance = function(image_id, size_id, requestCallback){
   // TODO: Select a minion
   var minion = minionClient;

   // Instance
   minion.invoke('createInstance', {
      name:"Unnamed",
      image_id: image_id,
      size_id: size_id
   }, function (error, instance_id) {
      if(error){
         requestCallback(new Error("Failed to create instance, err: " + error));
      } else {
         requestCallback(null, instance_id);
      }
   });
}


/**
 * Get instance metadata
 * @param {String} - Instance ID
 */
var getInstance = function(inst_id, getCallback){
   database.db.collection('instances').findOne({_id: inst_id}, function(error, inst){
      if(error || inst == undefined){
         getCallback(new Error('Instance with ID "'+inst_id+'" does not exists'));
      } else {
         getCallback(null, inst);
      }
   });
}

/**
 * Get image metadata
 * @param {String} - Image ID
 */
var getImage = function(image_id, getCallback){
   database.db.collection('images').findOne({_id: image_id}, function(error, image){
      if(error || image == undefined){
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
      if(error || size == undefined){
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
   minionClient.invoke('getImages', null, function (error, images) {
      if(error){
         getCallback(new Error('Failed to retrieve images'));
      } else {
         getCallback(null, images);
      }
   });
}

/**
 * Get sizes list
 */
var getSizesList = function(getCallback){
   minionClient.invoke('getSizes', null, function (error, sizes) {
      if(error){
         getCallback(new Error('Failed to retrieve sizes'));
      } else {
         getCallback(null, sizes);
      }
   });
}

/**
 * Get instances list
 */
var getInstancesList = function(getCallback){
   minionClient.invoke('getInstances', null, function (error, insts) {
      if(error){
         getCallback(new Error('Failed to retrieve instances'));
      } else {
         getCallback(null, insts);
      }
   });
}

/**
 * Destroy instance
 */
var destroyInstance = function(inst_id, destroyCallback){
   // Destroy instance
   minionClient.invoke('destroyInstance', inst_id, function (error) {
      if(error){
         destroyCallback(new Error("Failed to destroy instance " + inst_id + ", err: " + error));
      } else {
         destroyCallback(null);
      }
   });
}

/**
 * Remove experiment from instance
 */
var cleanExperiment = function(exp_id, inst_id, b_job, b_code, b_input, b_remove, cleanCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Clean jobs in this instance
      function(inst, minion, wfcb){
         _abortInstanceJobs(exp_id, inst_id, function(error){
            wfcb(error, inst, minion);
         });
      },
      // Clean code
      function(inst, minion, wfcb){
         if(b_remove || b_code){
            _cleanExperimentCode(minion, exp_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null, inst, minion);
            });
         } else {
            wfcb(null, inst, minion);
         }
      },
      // Clean input
      function(inst, minion, wfcb){
         if(b_remove || b_input){
            _cleanExperimentInput(minion, exp_id, inst, function(error){
               if(error) return wfcb(error);
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      }
   ],
   function(error){
      if(error) return cleanCallback(error);

      if(b_remove){
         // Remove from database
         console.log("Removing from DB");
         database.db.collection('instances').updateOne({id: inst_id},{
            $pull: {exps: {exp_id: exp_id}}
         });
      }
      cleanCallback(null);
   });
}

/**
 * Clean experiment from instances in the system
 */
var cleanExperimentSystem = function(exp_id, system, b_job, b_code, b_input, b_remove, cleanCallback){
   // Clean from all system's instances
   var tasks = [];
   for(var inst in system.instances){
      // inst must be task independent
      (function(inst){
         tasks.push(function(taskcb){
            if(system.instances[inst]){
               console.log("["+system.instances[inst]+"] Cleaning instance...");
               cleanExperiment(exp_id, system.instances[inst], b_job, b_code, b_input, b_remove, function (error) {
                  if(error) console.error('['+MODULE_NAME+'] Failed to clean instance, error: '+error);
                  return taskcb(null);
               });
            } else {
               return taskcb(null);
            }
         });
      })(inst);
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      return cleanCallback(error, system);
   });
}

/**
 * Add experiment to instance experiments
 */
var addExperiment = function(exp_id, inst_id){
   // Add experiment to instance
   database.db.collection('instances').updateOne({
      'id': inst_id,
   },{
      $push: {'exps': {exp_id: exp_id, jobs: []}}
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
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeCommand', cmd, inst_id, function (error, result) {
            if (error) {
               wfcb(new Error("Failed to execute command: "+ cmd+ "\nError: "+ error));
            } else {
               wfcb(null, result);
            }
         });
      }
   ],
   function(error, result){
      if(error){
         executeCallback(error);
         return;
      }
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
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeScript', cmd, work_dir, inst_id, nodes, function (error, job_id) {
            if (error) {
               wfcb(new Error("Failed to execute job command:\n", cmd, "\nError: ", error));
            } else {
               wfcb(null, job_id);
            }
         });
      }
   ],
   function(error, job_id){
      if(error){return executeCallback(error);}

      // Callback
      executeCallback(null, job_id);
   });
}

var waitJob = function(job_id, inst_id, waitCallback){
   async.waterfall([
      // Get instance
      function(wfcb){
         getInstance(inst_id, wfcb);
      },
      // TODO: Get minion for this instance
      function(inst, wfcb){
         // minion = ...
         wfcb(null, inst, minionClient);
      },
      // Get job status
      function(inst, minion, wfcb){
         minion.invoke('getJobStatus', job_id, inst_id, function (error, status) {
            if (error) {
               wfcb(new Error("Failed to get job status: ", inst_id, "\nError: ", error));
            } else {
               if(status == "finished" || status == "unknown"){
                  // Done
                  waitCallback(null);
               } else {
                  // Running
                  setTimeout(waitJob, 1000, job_id, inst_id, waitCallback);
               }
               wfcb(null);
            }
         });
      }
   ],
   function(error){
      if(error){
         waitCallback(error);
         return;
      }
   });
}

/**
 * Clean experiment Job in instance
 */
var abortJob = function(job_id, inst_id, cleanCallback){
   if(job_id && inst_id){
      // Abort job
      console.log("["+MODULE_NAME+"] Aborted job: "+job_id+" from instance "+inst_id);
      minionClient.invoke('cleanJob', job_id, inst_id, function (error, result) {
         if (error) {
            return cleanCallback(new Error('Failed to abort job: "'+job_id+'", error: '+error));
         }

         // Callback
         cleanCallback(null);
      });
   }
}

/**
 * Clean experiment code in instance
 */
var _cleanExperimentCode = function(minion, exp_id, inst, cleanCallback){
   var work_dir = inst.workpath+"/"+exp_id;
   var cmd = 'rm -rf '+work_dir;
   // Execute command
   minion.invoke('executeCommand', cmd, inst.id, function (error, result, more) {
      if (error) {
         cleanCallback(new Error("Failed to clean experiment code, error: ", error));
      } else {
         cleanCallback(null);
      }
   });
}

/**
 * Clean experiment input data in instance
 */
var _cleanExperimentInput = function(minion, exp_id, inst, cleanCallback){
   // Remove experiment code folder
   var input_dir = inst.inputpath+"/"+exp_id;
   var cmd = 'rm -rf '+input_dir;
   // Execute command
   minion.invoke('executeCommand', cmd, inst.id, function (error, result, more) {
      if (error) {
         cleanCallback(new Error("Failed to clean experiment input data, error: ", error));
      } else {
         cleanCallback(null);
      }
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
         if(!insts[i].exps || insts[i].exps.length == 0){
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
var _abortInstanceJobs = function(exp_id, inst_id, abortCallback){
   // Get instance data
   getInstance(inst_id, function(error, inst){
      if(error) return abortCallback(error);

      // Iterate experiments in instance
      var tasks = [];
      for(var e = 0; e < inst.exps.length; e++){
         if(inst.exps[e].exp_id == exp_id){
            // Experiment found in this instance
            // Iterate jobs and abort
            var jobs = inst.exps[e].jobs;
            for(var i = 0; i< inst.exps.length; i++){
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
   // Scalability code
   _getSuperfluousInstances(function(error, list){
      if(error) return destroyCallback(error);

      // Destroy instances task
      var tasks = [];
      for(var i = 0; i < list.length; i++){
         (function(i){
            tasks.push(function(taskcb){
               // Destroy instance
               destroyInstance(list[i].id, function(error){
                  if(error){
                     console.error('['+MODULE_NAME+'] Failed to destroy instance "'+list[i].id+'", error: '+error);
                  } else {
                     // Remove instance from DB
                     database.db.collection('instances').remove({_id: list[i].id});
                     console.log('['+MODULE_NAME+'] Instance "'+list[i].id+'" destroyed');
                  }

                  // Always return no error
                  taskcb(null);
               });
            });
         })(i);
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         if(destroyCallback) destroyCallback(error);
      });
   });
}

/**
 * Initialize minions
 */
console.log("["+MODULE_NAME+"] Connecting to minion in: " + constants.MINION_URL);
minionClient.connect(constants.MINION_URL);


/**
 * Task: Remove empty instances from the system
 */
_destroyEmptyInstances(function(error){
   if(error) console.error(error);
});
setInterval(_destroyEmptyInstances, destroyInterval, function(error){
   if(error) console.error(error);
});


module.exports.getInstance = getInstance;
module.exports.getImage = getImage;
module.exports.getSize = getSize;

module.exports.getImagesList = getImagesList;
module.exports.getSizesList = getSizesList;
module.exports.getInstancesList = getInstancesList;

module.exports.defineSystem = defineSystem;
module.exports.instanceSystem = instanceSystem;
module.exports.cleanSystem = cleanSystem;

module.exports.requestInstance = requestInstance;
module.exports.destroyInstance = destroyInstance;

module.exports.addExperiment = addExperiment;
module.exports.executeCommand = executeCommand;
module.exports.executeJob = executeJob;
module.exports.waitJob = waitJob;
module.exports.abortJob = abortJob;

module.exports.cleanExperiment = cleanExperiment;
module.exports.cleanExperimentSystem = cleanExperimentSystem;
