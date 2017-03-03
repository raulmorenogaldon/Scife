/**
 * Copyright 2017 University of Castilla - La Mancha
 */
var zerorpc = require('zerorpc');
var async = require('async');
var request = require('request');
var fs = require('fs');
var exec = require('child_process').exec;
var sleep = require('sleep');
var ssh2 = require('ssh2').Client;
var mongo = require('mongodb').MongoClient;
var utils = require('../../overlord/utils');
var logger = utils.logger;

/***********************************************************
 * --------------------------------------------------------
 * MINION NAME
 * --------------------------------------------------------
 ***********************************************************/
var MINION_NAME = "ClusterMinion";

/***********************************************************
 * --------------------------------------------------------
 * GLOBAL VARS
 * --------------------------------------------------------
 ***********************************************************/
var zserver = null;
var cfg = process.argv[2];
var constants = {};

var ssh_conn = null;
var cmd_env = ". /etc/profile; . ~/.bash_profile";

/***********************************************************
 * --------------------------------------------------------
 * METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Login to provider.
 */
var login = function(cb){
   // Check if already connected
   if(ssh_conn){
      logger.info('['+MINION_NAME+'] Already connected...');
      return cb(null);
   }

   var private_key = null;
   if(constants.authkeyfile) private_key = fs.readFileSync(constants.authkeyfile);

   // Create connection
   utils.connectSSH(constants.username, constants.url, private_key, 30000, function(error, conn){
      if(error) return cb(error);

      // Save connection
      ssh_conn = conn;
      return cb(null);
   });
}

var getImages = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));

   // Retrieve images
   var query = {minion: MINION_NAME};
   if(filter) query.id = filter;
   database.collection('images').find(query).toArray().then(
      function(images){
         // Callback
         if(filter && images.length == 0) return getCallback(new Error("Image '"+filter+"' not found."));

         // Update quotas
         getQuotas(function(error, quotas){
            if(error) return getCallback(error);

            // Set quotas
            for(var i = 0; i < images.length; i++){
               images[i].quotas = quotas;
            }

            if(filter && images.length == 1) return getCallback(null, images[0]);
            getCallback(null, images);
         });
      }
   );
}

var getSizes = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));

   // Retrieve sizes
   var query = {minion: MINION_NAME};
   if(filter) query.id = filter;
   database.collection('sizes').find(query).toArray().then(
      function(sizes){
         // Callback
         if(filter && sizes.length == 0) return getCallback(new Error("Size '"+filter+"' not found."));
         if(filter && sizes.length == 1) return getCallback(null, sizes[0]);
         getCallback(null, sizes);
      }
   );
}

var getInstances = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));

   // Retrieve instances
   var query = {minion: MINION_NAME};
   if(filter) query.id = filter;
   database.collection('instances').find(query).toArray().then(
      function(instances){
         // Callback
         if(filter && instances.length == 0) return getCallback(new Error("Instance '"+filter+"' not found."));
         if(filter && instances.length == 1) return getCallback(null, instances[0]);
         getCallback(null, instances);
      }
   );
}

var createInstance = function(inst_cfg, createCallback){
   // Check params
   if(!inst_cfg) return createCallback(new Error("Null inst_cfg."));
   if(!inst_cfg.name) return createCallback(new Error("No name in inst_cfg."));
   if(!inst_cfg.image_id) return createCallback(new Error("No image_id in inst_cfg."));
   if(!inst_cfg.size_id) return createCallback(new Error("No size_id in inst_cfg."));
   if(!inst_cfg.nodes) return createCallback(new Error("No nodes in inst_cfg."));

   // Retrieve image
   getImages(inst_cfg.image_id, function(error, image){
      if(error) return createCallback(error);
      getSizes(inst_cfg.size_id, function(error, size){
         if(error) return createCallback(error);

         // Create instance metadata
         var id = utils.generateUUID();
         var inst = {
            _id: id,
            id: id,
            name: inst_cfg.name,
            image_id: inst_cfg.image_id,
            size_id: inst_cfg.size_id,
            execs: [],
            nodes: inst_cfg.nodes,
            size: {
               cpus: size['cpus'],
               ram: size['ram']
            },
            image: {
               workpath: image['workpath'],
               inputpath: image['inputpath'],
               outputpath: image['outputpath'],
               libpath: image['libpath'],
               tmppath: image['tmppath']
            },
            minion: MINION_NAME,
            hostname: constants.url,
            ip: constants.url,
            members: null,
            in_use: true,
            idle_time: Date.now(),
            ready: true
         };

         // Add to DB
         database.collection('instances').insert(inst, function(error){
            if(error) return createCallback(error);
            logger.log('['+MINION_NAME+']['+inst.id+'] Added instance to DB.');
            createCallback(null, inst.id);
         });
      });
   });
}

var destroyInstance = function(inst_id, destroyCallback){
   // Remove instance from DB
   database.collection('instances').remove({_id: inst_id});
   return destroyCallback(null);
}

var executeScript = function(inst_id, script, work_dir, nodes, executeCallback){
   // Available connection?
   if(!ssh_conn) return wfcb('No SSH connection is present');

   // Always have work_dir
   if(!work_dir) work_dir = "~";

   async.waterfall([
      // Get instance
      function(wfcb){
         getInstances(inst_id, wfcb);
      },
      // Execute script with queue system
      function(inst, wfcb){
         // Check if instance is ready
         if(inst.ready != true) return wfcb(new Error('Instance "'+inst_id+'" is not ready, unable to execute script.'));

         // QSUB launch command
         var qsub_cmd = "qsub -N "+inst.nodes+"-"+inst.size.cpus+"-"+inst.size.ram+" -l select="+inst.nodes+":ncpus="+inst.size.cpus+":mem="+inst.size.ram+"MB -e "+inst.image.tmppath+"/${PBS_JOBID}.stderr -o "+inst.image.tmppath+"/${PBS_JOBID}.stdout";
         var cmd = cmd_env+"; echo '"+script+"; echo -n $? > "+inst.image.tmppath+"/${PBS_JOBID}.code' | "+qsub_cmd+" | tr -d '\n'"; // Strip new line

         logger.debug("["+MINION_NAME+"]["+inst_id+"] Executing script: " + cmd);
         // Execute (blocking as we are using queue system)
         utils.execSSH(ssh_conn, cmd, work_dir, true, null, wfcb);
      }
   ],
   function(error, output){
      return executeCallback(error, output.stdout); // Return Job ID
   });
}

var executeCommand = function(inst_id, script, executeCallback){
   // Available connection?
   if(!ssh_conn) return wfcb('No SSH connection is present');

   async.waterfall([
      // Get instance
      function(wfcb){
         getInstances(inst_id, wfcb);
      },
      // Execute command
      function(inst, wfcb){
         // Check if instance is ready
         if(inst.ready != true) return wfcb(new Error('Instance "'+inst_id+'" is not ready, unable to execute script.'));

         // Just the command
         var cmd = cmd_env+"; "+script;

         // Execute
         logger.debug("["+MINION_NAME+"]["+inst_id+"] Executing command: " + cmd);
         utils.execSSH(ssh_conn, cmd, null, true, null, wfcb);
      }
   ],
   function(error, output){
      return executeCallback(error, output);
   });
}

var getJobStatus = function(job_id, inst_id, getCallback){
   // Available connection?
   if(!ssh_conn) return getCallback('No SSH connection is present');

   var status = "unknown";
   if(job_id){
      var cmd = cmd_env+"; qstat "+job_id;
      utils.execSSH(ssh_conn, cmd, null, true, null, function(error, output){
         // Parse output to get job info
         if(output.stderr.indexOf("Unknown") == -1){
            status = "running";
         } else {
            status = "finished";
         }
         return getCallback(null, status);
      });
   } else {
      return getCallback(null, status);
   }
}

var getQuotas = function(getCallback){
   // Return quotas
   var quotas = {
      cores: {
         in_use: 0,
         limit: 1000
      },
      floating_ips: {
         in_use: 0,
         limit: 1000
      },
      instances: {
         in_use: 0,
         limit: 1000
      },
      ram: {
         in_use: 0,
         limit: 1000000
      }
   };
   return getCallback(null, quotas);
}

var cleanJob = function(job_id, inst_id, cleanCallback){
   // QDEL
   var cmd = cmd_env+'; qdel -W force '+job_id;
   utils.execSSH(ssh_conn, cmd, null, true, null, function(error, output){
      return cleanCallback(error);
   })
}

var _loadConfig = function(config, loadCallback){
   if(!config.db) return loadCallback(new Error("No db field in CFG."));
   if(!config.listen) return loadCallback(new Error("No listen field in CFG."));
   if(!config.url) return loadCallback(new Error("No url field in CFG."));
   if(!config.username) return loadCallback(new Error("No username field in CFG."));

   async.waterfall([
      // Connect to MongoDB
      function(wfcb){
         // Connect to database
         logger.info("["+MINION_NAME+"] Connecting to MongoDB: " + config.db);
         mongo.connect(config.db, function(error, db){
            if(error) throw error;
            logger.info("["+MINION_NAME+"] Successfull connection to DB");

            // Set global var
            database = db;

            // Next
            return wfcb(null);
         });
      },
      // Connect to cluster
      function(wfcb){
         logger.info("["+MINION_NAME+"] Connecting to '"+config.username+"@"+config.url+"'...");
         login(function(error){
            if(error) return wfcb(error);

	    // Test connection
            //utils.execSSH(ssh_conn, "ls", "$HOME", true, function(error, output){
            //   logger.info("["+MINION_NAME+"] ls test: "+ error);
	    //});

            logger.info("["+MINION_NAME+"] Successfull connection to cluster.");

            // Next
            return wfcb(null);
         });
      },
      // Setup RPC
      function(wfcb){
         // Setup server
         zserver = new zerorpc.Server({
            login: login,
            autoupdate: autoupdate,
            getMinionName: getMinionName,
            getImages: getImages,
            getSizes: getSizes,
            getInstances: getInstances,
            createInstance: createInstance,
            destroyInstance: destroyInstance,
            executeScript: executeScript,
            executeCommand: executeCommand,
            getJobStatus: getJobStatus,
            cleanJob: cleanJob,
            getQuotas: getQuotas
         }, 30000);

         // Listen
         logger.info("["+MINION_NAME+"] Listening on "+config.listen);
         zserver.bind(config.listen);

         // RPC error handling
         zserver.on("error", function(error){
            logger.error("["+MINION_NAME+"] RPC server error: "+ error);
         });

         // Next
         wfcb(null);
      },
      // Load images
      function(wfcb){
         logger.info("["+MINION_NAME+"] Loading images...");
         // Get images from DB
         database.collection('images').find({minion: MINION_NAME}).toArray().then(function(images){
            for(var i = 0; i < config.images.length; i++){
               var image = config.images[i];

               // Search image
               var found = false;
               for(var j = 0; j < images.length; j++){
                  if(image.internal_id == images[j].internal_id){
                     // Found
                     logger.info('['+MINION_NAME+'] Image "'+image.internal_id+'" exists already.');
                     found = true;
                     break;
                  }
               }

               // Add to DB
               if(!found){
                  image._id = utils.generateUUID();
                  image.id = image._id;
                  image.minion = MINION_NAME;
                  database.collection('images').insert(image);
                  logger.info('['+MINION_NAME+'] Added image "'+image.id+'" to database.')
               }
            }

            // Next
            wfcb(null);
         });
      },
      // Load sizes
      function(wfcb){
         logger.info("["+MINION_NAME+"] Loading sizes...");
         // Get sizes from DB
         database.collection('sizes').find({minion: MINION_NAME}).toArray().then(function(sizes){
            for(var i = 0; i < config.sizes.length; i++){
               var size = config.sizes[i];

               // Search size
               var found = false;
               for(var j = 0; j < sizes.length; j++){
                  if(size.internal_id == sizes[j].internal_id){
                     // Found
                     logger.info('['+MINION_NAME+'] Size "'+size.internal_id+'" exists already.');
                     found = true;
                     break;
                  }
               }

               // Add to DB
               if(!found){
                  size._id = utils.generateUUID();
                  size.id = size._id;
                  size.minion = MINION_NAME;
                  database.collection('sizes').insert(size);
                  logger.info('['+MINION_NAME+'] Added size "'+size.id+'" to database.')
               }
            }

            // Next
            wfcb(null);
         });
      },
      // Setup compatibility
      function(wfcb){
         logger.info("["+MINION_NAME+"] Setting compatibility between images and sizes...");
         // Get images from DB
         database.collection('images').find({minion: MINION_NAME}).toArray().then(function(images){
            // Get sizes from DB
            database.collection('sizes').find({minion: MINION_NAME}).toArray().then(function(sizes){
               // Iterate images
               for(var i = 0; i < images.length; i++){
                  // Sizes for this image
                  var sizes_compatible = [];
                  var image = images[i];
                  // Iterate sizes
                  for(var j = 0; j < sizes.length; j++){
                     var size = sizes[j];

                     // Compatibles?
                     for(var z = 0; z < image.internal_sizes_compatible.length; z++){
                        var comp_id = image.internal_sizes_compatible[z];
                        if(size.internal_id == comp_id){
                           // Compatible!
                           sizes_compatible.push(size.id);
                        }
                     }
                  }
                  // Save changes
                  logger.info('['+MINION_NAME+'] Sizes "'+sizes_compatible+'" are compatible with image "'+image.id+'".');
                  database.collection('images').updateOne({id: image.id},{$set:{sizes_compatible:sizes_compatible}});
               }

               // Next
               return wfcb(null);
            });
         });
      },
   ],
   function(error){
      if(error){
         loadCallback(error);
      } else {
         logger.info("["+MINION_NAME+"] Config loaded.");
         loadCallback(null);
      }
   });
}

/**
 * Update repository and restart process.
 */
var autoupdate = function(cb){
   logger.info('['+MODULE_NAME+'] Autoupdate: Begin.');
   async.waterfall([
      // Check configuration
      function(wfcb){
         if(!constants.AUTOUPDATE){
            return wfcb(new Error('Autoupdate is not enabled.'));
         }
         return wfcb(null);
      },
      // Update repository
      function(wfcb){
         exec('git pull origin '+constants.AUTOUPDATE, function(error, stdout, stderr){
            return wfcb(error);
         });
      }
   ],
   function(error){
      if(error){
         // Error trying to checkpoint execution
         logger.debug('['+MODULE_NAME+'] Autoupdate: Error.');
         cb(error);
      } else {
         logger.debug('['+MODULE_NAME+'] Autoupdate: Done, resetting...');
         // Callback before exit
         cb(null);
         // Exit from Node, forever will restart the service.
         process.exit(1);
      }
   });
}

var getMinionName = function(getCallback){
   getCallback(null, MINION_NAME);
}

/***********************************************************
 * --------------------------------------------------------
 * INITIALIZATION
 * --------------------------------------------------------
 ***********************************************************/

// Get config file
if(!cfg) throw new Error("No CFG file has been provided.");

// Steps
async.waterfall([
   // Read config file
   function(wfcb){
      logger.info("["+MINION_NAME+"] Reading config file: "+cfg);
      fs.readFile(cfg, function(error, fcontent){
         if(error) return wfcb(error);
         return wfcb(null, fcontent);
      });
   },
   // Load cfg
   function(fcontent, wfcb){
      logger.info("["+MINION_NAME+"] Loading config file...");

      // Parse cfg
      constants = JSON.parse(fcontent);

      // Loading
      _loadConfig(constants, function(error){
         if(error) return wfcb(error);
         return wfcb(null);
      });
   }
],
function(error){
   if(error) throw error;
   logger.info('['+MINION_NAME+'] Initialization completed.');
   //__test();
});

