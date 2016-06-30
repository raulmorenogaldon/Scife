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
var dMinionClients = {};
var vMinionClients = [];
var destroyInterval = 30000;

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
var defineSystem = function(nodes, image_id, size_id, prefix, defineCallback){
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
            prefix: prefix,
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
         (function(i){
            requestInstance(system.prefix+"_"+i, system.image.id, system.size.id, function (error, inst_id) {
               if(error) return taskcb(error);
               // Add instance to system
               system.instances.push(inst_id);
               // Add system to instance
               database.db.collection('instances').updateOne({id: inst_id},{
                  $set: { system: true}
               });
               taskcb(null);
            });
         })(i);
      });
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error) return instanceCallback(error, null);
      // Callback with instanced system
      system.status = "instanced";
      instanceCallback(null, system);
   });
}

/**
 * Remove instances from the system
 */
var cleanSystem = function(system, cleanCallback){
   // Remove system from instances
   if(system.instances){
      for(var i = 0; i < system.instances.length; i++){
         database.db.collection('instances').updateOne({id: system.instances[i]},{
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
var requestInstance = function(name, image_id, size_id, requestCallback){
   // Get image
   getImage(image_id, function(error, image){
      if(error) return requestCallback(error);

      // Get minion
      var minion = dMinionClients[image.minion];

      // Connected to minion?
      if(!minion) return requestCallback(new Error("Minion "+image.minion+" not found."));

      // Instance
      minion.invoke('createInstance', {
         name: name,
         image_id: image_id,
         size_id: size_id,
         publicIP: true
      }, function (error, instance_id) {
         if(error) return requestCallback(error);
         requestCallback(null, instance_id);
      });
   });
}


/**
 * Get instance metadata
 * @param {String} - Instance ID
 */
var getInstance = function(inst_id, getCallback){
   database.db.collection('instances').findOne({_id: inst_id}, function(error, inst){
      if(error || !inst){
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
            minion.invoke('getImages', null, function (error, minion_images) {
               if(error){
                  taskcb(new Error('Failed to retrieve images'));
               } else {
                  // Add to list
                  list = list.concat(minion_images);
                  taskcb(null);
               }
            });
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
            minion.invoke('getSizes', null, function (error, minion_sizes) {
               if(error){
                  taskcb(new Error('Failed to retrieve sizes'));
               } else {
                  // Add to list
                  list = list.concat(minion_sizes);
                  taskcb(null);
               }
            });
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
            minion.invoke('getInstances', null, function (error, minion_insts) {
               if(error){
                  taskcb(new Error('Failed to retrieve instances'));
               } else {
                  // Add to list
                  list = list.concat(minion_insts);
                  taskcb(null);
               }
            });
         });
      })(i);
   }
   async.parallel(tasks, function(error){
      if(error) return getCallback(error);
      getCallback(null, list);
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
      var minion = dMinionClients[inst.minion];
      if(!minion) return destroyCallback(new Error("Minion "+inst.minion+" is not loaded."))

      // Destroy instance
      minion.invoke('destroyInstance', inst_id, function (error) {
         if(error) return destroyCallback(new Error("Failed to destroy instance " + inst_id + ", err: " + error));
         destroyCallback(null);
      });
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
      // Get minion for this instance
      function(inst, wfcb){
         var minion = dMinionClients[inst.minion];
         if(!minion) return wfcb(new Error("Minion "+inst.minion+" is not loaded."))
         wfcb(null, inst, minion);
      },
      // Clean jobs in this instance
      function(inst, minion, wfcb){
         console.log("["+inst_id+"] Aborting jobs...");
         _abortInstanceJobs(exp_id, inst_id, function(error){
            wfcb(error, inst, minion);
         });
      },
      // Clean code
      function(inst, minion, wfcb){
         if(b_remove || b_code){
            console.log("["+inst_id+"] Cleaning code...");
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
            console.log("["+inst_id+"] Cleaning input...");
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
         console.log("["+inst_id+"] Removing experiment from DB...");
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
   // Get instance
   getInstance(inst_id, function(error, inst){
      if(error) return console.error(error);

      // Check if already added
      if(!inst.exps[exp_id]){
         // Add experiment to instance
         database.db.collection('instances').updateOne({
            'id': inst_id,
         },{
            $push: {'exps': {exp_id: exp_id, jobs: []}}
         });
      } else {
         console.log('['+MODULE_NAME+']['+exp_id+'] Experiment is already in instance "'+inst_id+'".');
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
         var minion = dMinionClients[inst.minion];
         if(!minion) return wfcb(new Error("Minion "+inst.minion+" is not loaded."))
         wfcb(null, inst, minion);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeCommand', cmd, inst_id, function (error, result) {
            if (error) {
               wfcb(error);
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
      // Get minion for this instance
      function(inst, wfcb){
         var minion = dMinionClients[inst.minion];
         if(!minion) return wfcb(new Error("Minion "+inst.minion+" is not loaded."))
         wfcb(null, inst, minion);
      },
      // Execute command
      function(inst, minion, wfcb){
         minion.invoke('executeScript', cmd, work_dir, inst_id, nodes, function (error, job_id) {
            if (error) {
               return wfcb(error);
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
      // Get minion for this instance
      function(inst, wfcb){
         var minion = dMinionClients[inst.minion];
         if(!minion) return wfcb(new Error("Minion "+inst.minion+" is not loaded."))
         wfcb(null, inst, minion);
      },
      // Get job status
      function(inst, minion, wfcb){
         minion.invoke('getJobStatus', job_id, inst_id, function (error, status) {
            if (error) {
               wfcb(new Error("Failed to get job status: " + inst_id + ": " + error));
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
      // Get instance
      getInstance(inst_id, function(error, inst){
         if(error) return cleanCallback(error);

         // Get minion
         var minion = dMinionClients[inst.minion];
         if(!minion) return cleanCallback(new Error("Minion "+inst.minion+" is not loaded."))

         // Abort job
         console.log("["+MODULE_NAME+"]["+inst_id+"] Aborting job: "+job_id);
         minion.invoke('cleanJob', job_id, inst_id, function (error, result) {
            if (error) {
               return cleanCallback(error);
            }

            // Callback
            cleanCallback(null);
         });
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
   minion.invoke('executeCommand', cmd, inst.id, function (error, output) {
      if (error) {
         cleanCallback(error);
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
   minion.invoke('executeCommand', cmd, inst.id, function (error, output) {
      if (error) {
         cleanCallback(error);
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
         if(!insts[i].system && (!insts[i].exps || insts[i].exps.length == 0)){
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

      if(list.length > 0) console.log('['+MODULE_NAME+'] Destroying empty instances: '+ list.length);

      // Destroy instances task
      var tasks = [];
      for(var i = 0; i < list.length; i++){
         (function(i){
            tasks.push(function(taskcb){
               // Destroy instance
               console.log('['+MODULE_NAME+']['+list[i].id+'] Destroying...');
               destroyInstance(list[i].id, function(error){
                  if(error){
                     console.error('['+MODULE_NAME+'] Failed to destroy instance "'+list[i].id+'", error: '+error);
                  } else {
                     // Remove instance from DB
                     database.db.collection('instances').remove({_id: list[i].id});
                     console.log('['+MODULE_NAME+']['+list[i].id+'] Instance destroyed');
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
var tasks = [];
for(var i = 0; i < constants.MINION_URL.length; i++){
   // var i must be independent between tasks
   (function(i){
      tasks.push(function(taskcb){
         var minion = new zerorpc.Client({
            heartbeatInterval: 30000,
            timeout: 3600
         });
         minion.minion_url = constants.MINION_URL[i];
         minion.connect(minion.minion_url);
         console.log("["+MODULE_NAME+"] Connecting to minion in: " + minion.minion_url);
         minion.invoke('getMinionName', function (error, name) {
            if(error){
               console.error("["+MODULE_NAME+"] Failed to connect to minion "+minion.minion_url+".");
               return taskcb(null);
            }
            console.log("["+MODULE_NAME+"] Connected to minion "+name+".");
            dMinionClients[name] = minion;
            vMinionClients.push(minion);
            taskcb(null);
         });
      });
   })(i);
}
async.parallel(tasks, function(error){
   if(error) console.error(error);

   /**
    * Task: Remove empty instances from the system
    */
   _destroyEmptyInstances(function(error){
      if(error) console.error(error);
   });
   setInterval(_destroyEmptyInstances, destroyInterval, function(error){
      if(error) console.error(error);
   });
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
