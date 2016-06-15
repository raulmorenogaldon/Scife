var zerorpc = require('zerorpc');
var async = require('async');
var request = require('request');
var fs = require('fs');
var mongo = require('mongodb').MongoClient;
var utils = require('../../overlord/private-server/utils');

/***********************************************************
 * --------------------------------------------------------
 * MINION NAME
 * --------------------------------------------------------
 ***********************************************************/
var MINION_NAME = "OpenStackVesuvius";

/***********************************************************
 * --------------------------------------------------------
 * GLOBAL VARS
 * --------------------------------------------------------
 ***********************************************************/
var zserver = null;
var cfg = process.argv[2];
var token = null;
var database = null;

var auth_url = null;
var compute_url = null;
var network_label = null;

/***********************************************************
 * --------------------------------------------------------
 * METHODS
 * --------------------------------------------------------
 ***********************************************************/
var login = function(loginCallback){
   var body = {
      auth: {
         identity: {
            methods: [
               "password"
            ],
            password: {
               user: {
                  name: process.env.OS_USERNAME,
                  domain: {
                     id: process.env.OS_USER_DOMAIN_NAME,
                  },
                  password: process.env.OS_PASSWORD
               }
            }
         }
      }
   };

   var req = {
      url: process.env.OS_AUTH_URL + '/auth/tokens',
      method: 'POST',
      json: body
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return loginCallback(error);
      if(!res.headers['x-subject-token']) return loginCallback(new Error(
         'No X-Subject-Token when login'
      ));

      // Save token
      token = res.headers['x-subject-token'];

      // Log services
      for(var c = 0; c < body.token.catalog.length; c++){
         var service = body.token.catalog[c];

         // Iterate endpoints
         for(var i = 0; i < service.endpoints.length; i++){
            if(service.endpoints[i].interface == 'public'){
               console.log("--------------------------\nService: " + service.name + "\nURL: " + service.endpoints[i].url);
               // Save compute endpoint
               if(service.type == 'compute') compute_url = service.endpoints[i].url;
            }
         }
      }

      // Checks
      if(!compute_url) console.error("Compute service not found in catalog.");

      // Callback
      loginCallback(null);
   });
}

var getMinionName = function(getCallback){
   getCallback(null, MINION_NAME);
}

var getImages = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));
   if(!filter) filter = {};

   // Retrieve images
   database.collection('images').find({minion: MINION_NAME}, filter).toArray().then(
      function(images){
         // Callback
         if(filter && images.length == 1) getCallback(null, images[0]);
         else getCallback(null, images);
      }
   );
}

var getSizes = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));
   if(!filter) filter = {};

   // Retrieve sizes
   database.collection('sizes').find({minion: MINION_NAME}, filter).toArray().then(
      function(sizes){
         // Callback
         if(filter && sizes.length == 1) getCallback(null, sizes[0]);
         else getCallback(null, sizes);
      }
   );
}

var getInstances = function(filter, getCallback){
   if(!database) return getCallback(new Error("No connection to DB."));
   if(!filter) filter = {};

   // Retrieve instances
   database.collection('instances').find({minion: MINION_NAME}, filter).toArray().then(
      function(instances){
         // Callback
         if(filter && instances.length == 1) getCallback(null, instances[0]);
         else getCallback(null, instances);
      }
   );
}

var createInstance = function(inst_cfg, createCallback){
   // Check params
   if(!inst_cfg) return createCallback(new Error("Null inst_cfg."));
   if(!inst_cfg.name) return createCallback(new Error("No name in inst_cfg."));
   if(!inst_cfg.image_id) return createCallback(new Error("No image_id in inst_cfg."));
   if(!inst_cfg.size_id) return createCallback(new Error("No size_id in inst_cfg."));

   // Create instance
   _createOpenStackInstance(inst_cfg, function(error, inst_id){
      if(error) return createCallback(error);
      createCallback(null, inst_id);
   });
}

var destroyInstance = function(inst_id, destroyCallback){
   // Destroy instance
   _destroyOpenStackInstance(inst_id, function(error){
      if(error) return destroyCallback(error);
      destroyCallback(null);
   });
}

var _getOpenStackImages = function(getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/images',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _getOpenStackSizes = function(getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/flavors',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _getOpenStackInstances = function(getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _createOpenStackInstance = function(inst_cfg, createCallback){
   if(!token) createCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) createCallback(new Error('Compute service URL is not defined.'));

   // Get image and size
   async.waterfall([
      // Get image
      function(wfcb){
         getImages(inst_cfg.image_id, function(error, image){
            if(error) return wfcb(error);
            inst_cfg.image = image;
            wfcb(null);
         });
      },
      // Get size
      function(wfcb){
         getSizes(inst_cfg.size_id, function(error, size){
            if(error) return wfcb(error);
            inst_cfg.size = size;
            wfcb(null);
         });
      },
      // Request
      function(wfcb){
         // Request type
         var req = {
            url: compute_url + '/servers',
            method: 'POST',
            json: true,
            headers: {
               'Content-Type': "application/json",
               'X-Auth-Token': token,
            }
         };

         // Request body
         req.body = {
            server: {
               name: inst_cfg.name,
               imageRef: inst_cfg.image_id,
               flavorRef: inst_cfg.size_id,
               availability_zone: "nova",
               security_groups: [{name: "default"}],
            }
         };

         // Send request
         request(req, function(error, res, body){
            if(error) return wfcb(error);
            inst_cfg.server = body.server;
            wfcb(null)
         });
      },
      // Wait ready
      function(wfcb){
         _waitOpenStackInstanceActive(inst_cfg.server.id, function(error){
            if(error) return wfcb(error);
            wfcb(null);
         });
      },
      // Need a public IP?
      function(wfcb){
         if(inst_cfg.publicIP){
            _assignOpenStackFloatIPInstance(inst_cfg.server.id, function(error, ip){
               if(error) console.log('['+MINION_NAME+'] Failed to assign public IP to instance "' + inst_cfg.server.id + '".');
               if(error) return wfcb(error);

               // Public IP assignation success
               console.log('['+MINION_NAME+'] Assigned IP: '+ip+' to instance "' + inst_cfg.server.id + '".');
               wfcb(null);
            });
         } else {
            // Skip step
            wfcb(null);
         }
      },
      // Create instance metadata
      function(wfcb){
         var inst = {
            _id: inst_cfg.server.id,
            id: inst_cfg.server.id,
            name: inst_cfg.name,
            image_id: inst_cfg.image_id,
            size_id: inst_cfg.size_id,
            adminPass: inst_cfg.server.adminPass,
            exps: [],
            workpath: inst_cfg.image['workpath'],
            inputpath: inst_cfg.image['inputpath'],
            minion: MINION_NAME,
            ready: true
         };

         wfcb(null, inst);
      }
   ],
   function(error, inst){
      if(error){
         console.log('['+MINION_NAME+'] Failed to create instance, error: '+error.message);

         // Destroy instance
         if(inst_cfg.server) {
            destroyInstance(inst_cfg.server.id, function(error){});
         }

         // Fail
         return createCallback(error);
      }

      // Add to DB
      database.collection('instances').insert(inst);
      console.log('['+MINION_NAME+'] Instance "' + inst.id + '" added to DB.');
      createCallback(null, inst.id);
   });
}

var _waitOpenStackInstanceActive = function(inst_id, waitCallback){
   _getOpenStackInstance(inst_id, function(error, inst){
      if(error) return waitCallback(error);

      if(inst.status != 'ACTIVE') setTimeout(_waitOpenStackInstanceActive, 1000, inst_id, waitCallback);
      else waitCallback(null);
   });
}

var _destroyOpenStackInstance = function(inst_id, destroyCallback){
   if(!token) destroyCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) destroyCallback(new Error('Compute service URL is not defined.'));

   // Request type
   var req = {
      url: compute_url + '/servers/' + inst_id,
      method: 'DELETE',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return destroyCallback(error);
      console.log('['+MINION_NAME+'] Deleted OpenStack instance "' + inst_id + '"');
      destroyCallback(null);
   });
}

var _getOpenStackInstance = function(inst_id, getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers/' + inst_id,
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body).server);
   });
}

var _getOpenStackInstanceIPs = function(inst_id, getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers/' + inst_id + '/ips/' + network_label,
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body)[network_label]);
   });
}

var _getOpenStackFreeFloatIP = function(getCallback){
   if(!token) getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/os-floating-ips',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   request(req, function(error, res, body){
      if(error) return getCallback(error);

      // Get floating IPs
      var ips = JSON.parse(body).floating_ips;
      if(!ips) getCallback(new Error("No floating ips in response from "+ req.url));

      // Search for an empty IP
      for(var i = 0; i < ips.length; i++){
         if(!ips[i].instance_id) return getCallback(null, ips[i]);
      }

      // No IP found, allocate
      _allocateOpenStackFloatIP(function(error, ip){
         if(error) return getCallback(error);
         getCallback(null, ip);
      });
   });
}

var _allocateOpenStackFloatIP = function(allocateCallback){
   if(!token) assignCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) assignCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/os-floating-ips',
      method: 'POST',
      json: true,
      headers: {
         'Content-Type': "application/json",
         'X-Auth-Token': token,
      }
   };

   req.body = {
      pool: "external"
   }

   // Send request
   request(req, function(error, res, body){
      if(error) return allocateCallback(error);
      allocateCallback(null, body.floating_ip);
   });
}

var _assignOpenStackFloatIPInstance = function(inst_id, assignCallback){
   if(!token) assignCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) assignCallback(new Error('Compute service URL is not defined.'));

   // Allocate an IP
   _getOpenStackFreeFloatIP(function(error, ip){
      if(error) return assignCallback(error);

      var req = {
         url: compute_url + '/servers/' + inst_id + '/action',
         method: 'POST',
         json: true,
         headers: {
            'Content-Type': "application/json",
            'X-Auth-Token': token,
         }
      };

      req.body = {
         addFloatingIp: {
            address: ip.ip
         }
      };

      // Send request
      request(req, function(error, res, body){
         if(error) return assignCallback(error);
         assignCallback(null, ip.ip);
      });
   });
}

var _loadConfig = function(config, loadCallback){
   if(!config.db) return loadCallback(new Error("No db field in CFG."));
   if(!config.listen) return loadCallback(new Error("No listen field in CFG."));
   if(!config.images) return loadCallback(new Error("No images field in CFG."));
   if(!config.sizes) return loadCallback(new Error("No sizes field in CFG."));

   async.waterfall([
      // Connect to MongoDB
      function(wfcb){
         // Connect to database
         console.log("["+MINION_NAME+"] Connecting to MongoDB: " + config.db);
         mongo.connect(config.db, function(error, db){
            if(error) throw error;
            console.log("["+MINION_NAME+"] Successfull connection to DB");

            // Set global var
            database = db;

            // Next
            wfcb(null);
         });
      },
      // Connect to OpenStack
      function(wfcb){
         console.log("["+MINION_NAME+"] Connecting to OpenStack...");
         login(function(error){
            if(error) wfcb(error);
            console.log("["+MINION_NAME+"] Successfull connection to OpenStack, token: " + token);

            // Setup network label
            network_label = config.network;
            if(!network_label) console.error("["+MINION_NAME+"] No network label provided.");

            // Next
            wfcb(null);
         });
      },
      // Setup RPC
      function(wfcb){
         // Setup server
         zserver = new zerorpc.Server({
            login: login,
            getImages: getImages,
            getSizes: getSizes,
            getInstances: getInstances
         }, 30);

         // Listen
         zserver.bind(config.listen);
         console.log("["+MINION_NAME+"] Minion is listening on "+config.listen)

         // RPC error handling
         zserver.on("error", function(error){
            console.error("["+MINION_NAME+"] RPC server error: "+ error)
         });

         // Next
         wfcb(null);
      },
      // Load images
      function(wfcb){
         console.log("["+MINION_NAME+"] Loading images...");
         // Get images from DB
         database.collection('images').find({minion: MINION_NAME}).toArray().then(function(images){
            for(var i = 0; i < config.images.length; i++){
               var image = config.images[i];

               // Search image
               var found = false;
               for(var j = 0; j < images.length; j++){
                  if(image.id == images[j].id){
                     // Found
                     console.log('['+MINION_NAME+'] Image "'+image.id+'" exists already.');
                     found = true;
                     break;
                  }
               }

               // Add to DB
               if(!found){
                  console.log('['+MINION_NAME+'] Added image "'+image.id+'" to database.')
                  image._id = image.id;
                  image.minion = MINION_NAME;
                  database.collection('images').insert(image);
               }
            }

            // Next
            wfcb(null);
         });
      },
      // Load sizes
      function(wfcb){
         console.log("["+MINION_NAME+"] Loading sizes...");
         // Get sizes from DB
         database.collection('sizes').find({minion: MINION_NAME}).toArray().then(function(sizes){
            for(var i = 0; i < config.sizes.length; i++){
               var size = config.sizes[i];

               // Search size
               var found = false;
               for(var j = 0; j < sizes.length; j++){
                  if(size.id == sizes[j].id){
                     // Found
                     console.log('['+MINION_NAME+'] Size "'+size.id+'" exists already.');
                     found = true;
                     break;
                  }
               }

               // Add to DB
               if(!found){
                  console.log('['+MINION_NAME+'] Added size "'+size.id+'" to database.')
                  size._id = utils.generateUUID();
                  size.minion = MINION_NAME;
                  database.collection('sizes').insert(size);
               }
            }

            // Next
            wfcb(null);
         });
      }
   ],
   function(error){
      if(error){
         loadCallback(error);
      } else {
         console.log("["+MINION_NAME+"] Config loaded.");
         loadCallback(null);
      }
   });
}

var _cleanOpenStackMissingInstances = function(cleanCallback){
   getInstances(null, function(error, instances){
      // Iterate instances
      var tasks = [];
      for(var i = 0; i < instances.length; i++){
         // var i must be independent between tasks
         (function(i){
            tasks.push(function(taskcb){
               var inst = instances[i];
               // Get OpenStack instance
               _getOpenStackInstance(inst.id, function(error, osinst){
                  if(error) return taskcb(error);

                  // Check if instances is in OpenStack
                  if(!osinst){
                     // Remove from DB
                     database.collection('instances').remove({_id: inst._id});
                     console.log('['+MINION_NAME+'] Removed instance "'+ inst.id +'" as no longer exists in OpenStack.');
                  }
                  taskcb(null);
               });
            });
         })(i);
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         if(error) return cleanCallback(error);
         cleanCallback(null);
      });
   });
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
      console.log("["+MINION_NAME+"] Reading config file: "+cfg);
      fs.readFile(cfg, function(error, fcontent){
         if(error) return wfcb(error);
         wfcb(null, fcontent);
      });
   },
   // Load cfg
   function(fcontent, wfcb){
      console.log("["+MINION_NAME+"] Loading config file...");

      // Parse cfg
      cfg = JSON.parse(fcontent);

      // Loading
      _loadConfig(cfg, function(error){
         if(error) return wfcb(error);
         wfcb(null);
      });
   },
   // Clean missing instances
   function(wfcb){
      console.log("["+MINION_NAME+"] Cleaning missing instances...");
      _cleanOpenStackMissingInstances(function(error){
         if(error) return wfcb(errro);
         wfcb(null);
      });
   },
],
function(error){
   if(error) throw error;
   console.log("["+MINION_NAME+"] Initialization completed.");

   // TODO: Remove, some tests
   var inst_cfg = {
      name: "Test inst from minion",
      image_id: "81743971-6f56-4c9f-a557-2edf4516d185",
      size_id: "3",
      publicIP: true
   };

   console.log("Creating: ");
   console.log(JSON.stringify(inst_cfg, null, 2));
   createInstance(inst_cfg, function(error, inst_id){
      if(error) return console.error(error);
      console.log("Created "+inst_id);

      //_assignOpenStackFloatIPInstance(inst_id, function(error, res){
      //   console.log("Assigned IP:");
      //   console.log(res);

      //   _getOpenStackInstanceIPs(inst_id, function(error, ips){
      //      // Print info
      //      console.log("IPs:");
      //      console.log(JSON.stringify(ips,null,2));

      //      //console.log("Destroying:");
      //      //destroyInstance(inst_id, function(error){
      //      //   if(error) throw error;
      //      //   console.log("Destroyed!");
      //      //   destroyInstance(inst_id, function(error){
      //      //      if(error) throw error;
      //      //      console.log("again");
      //      //   });
      //      //});
      //   });
      //});
   });

   console.log("Getting instances...");
   // List instances
   getInstances(null, function(error, instances){
      console.log("Instances:");
      console.log(instances);
   });
});
