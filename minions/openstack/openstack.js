var zerorpc = require('zerorpc');
var async = require('async');
var request = require('request');
var fs = require('fs');
var sleep = require('sleep');
var ssh2 = require('ssh2').Client;
var mongo = require('mongodb').MongoClient;
var utils = require('../../overlord/utils');

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

var keypair_name = null;
var private_key_path = null;

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
      console.info('['+MINION_NAME+'] New token: '+token);

      // Log services
      for(var c = 0; c < body.token.catalog.length; c++){
         var service = body.token.catalog[c];

         // Iterate endpoints
         for(var i = 0; i < service.endpoints.length; i++){
            if(service.endpoints[i].interface == 'public'){
               //console.log("--------------------------\nService: " + service.name + "\nURL: " + service.endpoints[i].url);
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

      // Cluster option
      var headnode = {};
      var insts = [];
      var tasks = [];
      var failed = false;
      for(var i = 0; i < inst_cfg.nodes; i++){
         // var i must be independent between tasks
         (function(i){
            tasks.push(function(taskcb){
               // Instance details
               var aux_cfg = {
                  name: inst_cfg.name + '-' + i,
                  image_id: inst_cfg.image_id,
                  size_id: inst_cfg.size_id
               };
               if(i == 0) aux_cfg.publicIP = true;

               // Create OpenStack instance
               _createOpenStackInstance(aux_cfg, function(error, os_inst_id){
                  if(error) {
                     failed = true;
                     // We want to execute the parallel part when all tasks ended.
                     return taskcb(null);
                  }

                  // Put public node in front
                  if(i == 0){
                     headnode = aux_cfg;
                     insts.unshift(os_inst_id);
                  }
                  else insts.push(os_inst_id);

                  // Created
                  taskcb(null);
               });
            });
         })(i);
      }

      // Execute tasks
      async.parallel(tasks, function(error){
         // Clean created instances
         if(error || failed){
            for (var i = 0; i < insts.length; i++){
               console.error('['+MINION_NAME+'] Destroying OpenStack instance "'+insts[i]+'".');
               _destroyOpenStackInstance(insts[i], function(error){
                  if(error) console.error('['+MINION_NAME+'] Error destroying OpenStack instance "'+insts[i]+'" - ' + error);
               });
            }
            if(failed) return createCallback(new Error('Failed to create instance members.'));
            else return createCallback(error);
         }

         // Create instance metadata
         var id = utils.generateUUID();
         var inst = {
            _id: id,
            id: id,
            name: inst_cfg.name,
            image_id: inst_cfg.image_id,
            size_id: inst_cfg.size_id,
            execs: [],
            nodes: insts.length,
            size: {
               cpus: size['workpath'],
               ram: size['inputpath']
            },
            image: {
               workpath: image['workpath'],
               inputpath: image['inputpath'],
               outputpath: image['outputpath'],
               libpath: image['libpath'],
               tmppath: image['tmppath']
            },
            minion: MINION_NAME,
            hostname: headnode.ip,
            ip: headnode.ip,
            ip_id: headnode.ip_id,
            members: insts,
            in_use: false,
            idle_time: Date.now(),
            ready: false
         };

         // Add to DB
         database.collection('instances').insert(inst, function(error){
            if(error) return createCallback(error);
            console.log('['+MINION_NAME+']['+inst.id+'] Added instance to DB.');

            // Configure cluster
            _configureInstance(inst.id, function(error){
               if(error){
                  for (var i = 0; i < insts.length; i++){
                     _destroyOpenStackInstance(insts[i], function(error){
                        if(error) console.error(error);
                     });
                  }
                  return createCallback(error);
               }

               // Set instance as ready
               database.collection('instances').updateOne({id:inst.id},{"$set":{ready: true, idle_time: Date.now()}});
               console.log('['+MINION_NAME+']['+inst.id+'] Ready.');
               createCallback(null, inst.id);
            });
         });
      });
   });
}

var destroyInstance = function(inst_id, destroyCallback){
   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error){
         // Remove instance from DB
         database.collection('instances').remove({_id: inst_id});
         return destroyCallback(error);
      }

      // Deallocate IP, no need to wait
      if(inst.ip){
         var ip_id = inst.ip_id;
         var ip = inst.ip;
         _deallocateOpenStackFloatingIP(ip_id, function(error){
            if(error) console.error(error);
            console.log('['+MINION_NAME+'] Deallocated '+ip+' "'+ip_id+'"');
         });
      }

      // Destroy members
      _destroyInstanceMembers(inst.members, function(error){
         // Remove instance from DB
         database.collection('instances').remove({_id: inst_id});
         if(error) return destroyCallback(error);
         destroyCallback(null);
      });
   });
}

var _destroyInstanceMembers = function(members, destroyCallback){
   // Iterate instances
   var tasks = [];
   for(var i = 0; i < members.length; i++){
      // var i must be independent between tasks
      (function(i){
         tasks.push(function(taskcb){
            var os_inst_id = members[i];
            // Get OpenStack instance
            console.log('['+MINION_NAME+'] Destroying - ' + os_inst_id);
            _destroyOpenStackInstance(os_inst_id, function(error){
               return taskcb(error);
            });
         });
      })(i);
   }

   // Execute tasks
   async.parallel(tasks, function(error){
      if(error) return destroyCallback(error);
      destroyCallback(null);
   });
}

var executeScript = function(script, work_dir, inst_id, nodes, executeCallback){
   // Wrapper
   _executeOpenStackInstanceScript(script, work_dir, inst_id, nodes, false, function(error, output){
      if(error) return executeCallback(error);
      // Return job ID
      executeCallback(null, output.stdout);
   });
}

var executeCommand = function(script, inst_id, executeCallback){
   // Wrapper
   _executeOpenStackInstanceScript(script, null, inst_id, 1, true, executeCallback);
}

var getJobStatus = function(job_id, inst_id, getCallback){
   // Wrapper
   _getOpenStackInstanceJobStatus(job_id, inst_id, getCallback);
}

var getQuotas = function(getCallback){
   // Get OpenStack quotas
   _getOpenStackQuotas(function(error, os_quotas){
      if(error) return getCallback(error);

      // Convert data
      var aux = os_quotas.limits.absolute;
      var quotas = {
         cores: {
            in_use: aux.totalCoresUsed,
            limit: aux.maxTotalCores
         },
         floating_ips: {
            in_use: aux.totalFloatingIpsUsed,
            limit: aux.maxTotalFloatingIps
         },
         instances: {
            in_use: aux.totalInstancesUsed,
            limit: aux.maxTotalInstances
         },
         ram: {
            in_use: aux.totalRAMUsed,
            limit: aux.maxTotalRAMSize
         }
      }

      // Return quotas
      getCallback(null, quotas);
   });
}

var cleanJob = function(job_id, inst_id, cleanCallback){
   // Kill PID
   var cmd = 'kill -9 '+job_id;
   _executeOpenStackInstanceScript(cmd, null, inst_id, 1, true, function(error, output){
      if(error) return cleanCallback(error);
      cleanCallback(null);
   })
}

var _configureInstance = function(inst_id, configureCallback){
   var _inst = null;
   var _size = null;

   async.waterfall([
      // Get instance data
      function(wfcb){
         getInstances(inst_id, function(error, inst){
            if(error) return wfcb(error);
            _inst = inst;
            wfcb(null);
         });
      },
      // Get size
      function(wfcb){
         getSizes(_inst.size_id, function(error, size){
            if(error) return wfcb(error);
            _size = size;
            wfcb(null);
         });
      },
      // Get hosts
      function(wfcb){
         _getInstanceHosts(inst_id, wfcb);
      },
      // Check connectivity with all instance members
      function(hosts, wfcb){
         _checkHostsConnectivity(inst_id, hosts, function(error, output){
            if(error) wfcb(error);
            wfcb(null, hosts);
         });
      },
      // Setup NFS
      function(hosts, wfcb){
         _setupNFS(inst_id, hosts, function(error){
            if(error) return wfcb(error);
            wfcb(error, hosts);
         });
      },
      // Build hosts file and copy to main host
      function(hosts, wfcb){
         var hostfile = '';
         for(var i = 0; i < hosts.length; i++){
            hostfile = hostfile + hosts[i] + ':' + _size.cpus + '\n';
         }

         // Copy to main host
         var cmd = 'echo -n "'+hostfile+'" > ~/hosts';
         _executeOpenStackInstanceScript(cmd, null, inst_id, 1, true, wfcb);
      }
   ],
   function(error){
      if(error) return configureCallback(error);
      configureCallback(null);
   });
};

var _setupNFS = function(inst_id, hosts, setupCallback){
   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error) return setupCallback(error);
      // Get image data
      getImages(inst.image_id, function(error, image){
         if(error) return setupCallback(error);

         // Create paths and add to exports
         var cmd = ''+
            '#!/bin/sh\n'+
            'mkdir -p '+image.workpath+'\n'+
            'mkdir -p '+image.inputpath+'\n'+
            'echo "'+image.workpath+' *(rw,async,no_subtree_check,no_root_squash)" | sudo tee /etc/exports\n'+
            'echo "'+image.inputpath+' *(rw,async,no_subtree_check,no_root_squash)" | sudo tee --append /etc/exports\n'+
            'sudo exportfs -a\n'+
            'sudo systemctl restart rpcbind\n'+
            'sudo systemctl restart nfs';
         _executeOpenStackInstanceScript(cmd, null, inst_id, 1, true, function(error, output){
            console.log(output);
            if(error) return setupCallback(error);

            // Iterate hosts
            var tasks = [];
            for(var i = 1; i < hosts.length; i++){
               // var i must be independent between tasks
               (function(i){
                  tasks.push(function(taskcb){
                     // Script to mount paths
                     var member_script = ''+
                        '#!/bin/sh\n'+
                        'sudo umount '+image.workpath+'\n'+
                        'sudo umount '+image.inputpath+'\n'+
                        'mkdir -p '+image.workpath+'\n'+
                        'mkdir -p '+image.inputpath+'\n'+
                        'sudo mount '+hosts[0]+':'+image.workpath+' '+image.workpath+'\n'+
                        'sudo mount '+hosts[0]+':'+image.inputpath+' '+image.inputpath+'\n'+
                        'sudo systemctl restart rpcbind';

                     // Mount NFS in members
                     var cmd = 'ssh -q -o StrictHostKeyChecking=no '+hosts[i]+' "'+member_script+'"';
                     _executeOpenStackInstanceScript(cmd, null, inst_id, 1, true, function(error, output){
                        console.log(output);
                        if(error) return taskcb(error);

                        console.log('['+MINION_NAME+'] Mounted NFS in ' + hosts[i]);
                        taskcb(null);
                     });
                  });
               })(i);
            }

            // Execute tasks
            async.series(tasks, function(error){
               if(error) return setupCallback(error);
               setupCallback(null);
            });
         });
      });
   });
}

var _getInstanceHosts = function(inst_id, getCallback){
   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error) return getCallback(error);

      // Iterate instances
      var hosts = [];
      var tasks = [];
      for(var i = 0; i < inst.members.length; i++){
         // var i must be independent between tasks
         (function(i){
            tasks.push(function(taskcb){
               _getOpenStackInstanceIPs(inst.members[i], function(error, ips){
                  if(error) taskcb(error);
                  // Get IP
                  hosts.push(ips[0].addr)
                  taskcb(null);
               });
            });
         })(i);
      }

      // Execute tasks
      async.series(tasks, function(error){
         if(error) return getCallback(error);
         getCallback(null, hosts);
      });
   });
}

var _checkHostsConnectivity = function(inst_id, hosts, checkCallback){
   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error) return checkCallback(error);
      getImages(inst.image_id, function(error, image){
         if(error) return checkCallback(error);

         // Load private key
         var private_key = fs.readFileSync(private_key_path);

         // Iterate hosts
         var tasks = [];
         for(var i = 0; i < hosts.length; i++){
            // var i must be independent between tasks
            (function(i){
               tasks.push(function(taskcb){
                  // First, connect to instance
                  utils.connectSSH(image.username, inst.ip, private_key, 300000, function(error, conn){
                     if(error) taskcb(error);
                     // Check ssh command
                     //var cmd = 'ssh -o StrictHostKeyChecking=no ' + hosts[i] + ' exit; echo -n $?';
                     var cmd = ''+
                        '#!/bin/sh \n'+
                        '((count = 20))\n'+
                        'while [[ \$count -ne 0 ]] ; do\n'+
                        'ssh -q -o StrictHostKeyChecking=no '+hosts[i]+' exit\n'+
                        'rc=\$?\n'+
                        'if [[ \$rc -eq 0 ]] ; then\n'+
                        '((count = 1))\n'+
                        'else\n'+
                        'sleep 3\n'+
                        'fi\n'+
                        '((count = count - 1))\n'+
                        'done\n'+
                        'echo -n \$rc';
                     utils.execSSH(conn, cmd, null, true, function(error, output){
                        // Close connection
                        utils.closeSSH(conn);

                        if(error || output.stdout != '0'){
                           console.error('['+MINION_NAME+'] Connection with ' + hosts[i] + ': ERROR - ' + output.stdout);
                           return taskcb(new Error('Failed to connect to ' + hosts[i]));
                        }

                        // OK
                        console.log('['+MINION_NAME+'] Connection with ' + hosts[i] + ': OK.');
                        taskcb(null);
                     });
                  });
               });
            })(i);
         }

         // Execute tasks
         async.series(tasks, function(error){
            if(error) return checkCallback(error);
            checkCallback(null, hosts);
         });
      });
   });
}

var _getOpenStackImages = function(getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/images',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _getOpenStackSizes = function(getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/flavors',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _getOpenStackInstances = function(getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body));
   });
}

var _createOpenStackInstance = function(inst_cfg, createCallback){
   if(!token) return createCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return createCallback(new Error('Compute service URL is not defined.'));

   // Get image and size
   console.log('['+MINION_NAME+'] Creating OpenStack instance "'+inst_cfg.name+'"...');
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
               imageRef: inst_cfg.image.os_id,
               flavorRef: inst_cfg.size.os_id,
               availability_zone: "nova",
               security_groups: [{name: "default"}],
               key_name: keypair_name
            }
         };

         // Send request
         _requestOpenStack(req, function(error, res, body){
            if(error) return wfcb(error);
            if(!body.server) return wfcb(new Error(JSON.stringify(body, null, 2)));
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
      // Get private IPs
      function(wfcb){
         // Get private ip
         _getOpenStackInstanceIPs(inst_cfg.server.id, function(error, ips){
            if(error) wfcb(error);
            inst_cfg.ip_private = ips[0].addr;
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
               inst_cfg.ip = ip.ip;
               inst_cfg.ip_id = ip.id;
               console.log('['+MINION_NAME+'] Assigned IP: '+inst_cfg.ip+' to instance "' + inst_cfg.server.id + '".');
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      },
      // Wait SSH connectivity (5 minutes)
      function(wfcb){
         if(inst_cfg.ip){
            var private_key = fs.readFileSync(private_key_path);
            utils.connectSSH(inst_cfg.image.username, inst_cfg.ip, private_key, 600000, function(error, conn){
               if(error) wfcb(error);
               // Connected, close connection
               utils.closeSSH(conn);
               console.log('['+MINION_NAME+'] Established connectivity with instance "' + inst_cfg.server.id + '".');
               wfcb(null);
            });
         } else {
            wfcb(null);
         }
      },
   ],
   function(error, inst){
      if(error){
         console.error('['+MINION_NAME+'] Failed to create OpenStack instance, error: '+error.message);
         // Destroy instance
         if(inst_cfg.server) {
            _destroyOpenStackInstance(inst_cfg.server.id, function(error){});
         }
         // Fail
         return createCallback(error);
      }
      // Return OpenStack ID
      createCallback(null, inst_cfg.server.id);
   });
}

var _waitOpenStackInstanceActive = function(inst_id, waitCallback){
   _getOpenStackInstance(inst_id, function(error, inst){
      if(error) return waitCallback(error);

      if(inst.status == 'ERROR') return waitCallback(new Error('Failed to create instance: \n'+JSON.stringify(inst, null, 2)));
      if(inst.status != 'ACTIVE') return setTimeout(_waitOpenStackInstanceActive, 1000, inst_id, waitCallback);

      waitCallback(null);
   });
}

var _destroyOpenStackInstance = function(inst_id, destroyCallback){
   if(!token) return destroyCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return destroyCallback(new Error('Compute service URL is not defined.'));

   // Request type
   var req = {
      url: compute_url + '/servers/' + inst_id,
      method: 'DELETE',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return destroyCallback(error);
      console.log('['+MINION_NAME+'] Deleted OpenStack instance "' + inst_id + '"');
      destroyCallback(null);
   });
}

var _executeOpenStackInstanceScript = function(script, work_dir, inst_id, nodes, blocking, executeCallback){
   if(!token) return executeCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return executeCallback(new Error('Compute service URL is not defined.'));

   console.log('['+MINION_NAME+']['+inst_id+'] Executing script (blck: ' + blocking + '): ' + script);

   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error) return executeCallback(error);

      // Check IP
      if(!inst.ip) return executeCallback(new Error('Instance has not floating IP.'));

      // Get image data
      var private_key = fs.readFileSync(private_key_path);
      getImages(inst.image_id, function(error, image){
         if(error) return executeCallback(error);

         // Get connection
         utils.connectSSH(image.username, inst.ip, private_key, 30000, function(error, conn){
            if(error) return executeCallback(error);

            // Execute command
            //console.log('['+MINION_NAME+']['+inst_id+'] Connected, executing command.');
            utils.execSSH(conn, script, work_dir, blocking, function(error, output){
               if(error){
                  // Close connection
                  utils.closeSSH(conn);
                  return executeCallback(error);
               }

               // Close connection
               utils.closeSSH(conn);

               //console.log('['+MINION_NAME+']['+inst_id+'] Executed command.');
               executeCallback(null, output);
            }); // execSSH
         }); // connectSSH
      }); // getImages
   });// getInstances
}

var _getOpenStackInstanceJobStatus = function(job_id, inst_id, getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   // Get instance data
   getInstances(inst_id, function(error, inst){
      if(error) return getCallback(error);

      // Check IP
      if(!inst.ip) return getCallback(new Error('Instance has not floating IP.'));

      // Get image data
      var private_key = fs.readFileSync(private_key_path);
      getImages(inst.image_id, function(error, image){
         if(error) return getCallback(error);

         // Get connection
         utils.connectSSH(image.username, inst.ip, private_key, 30000, function(error, conn){
            if(error) return getCallback(error);
            //console.log('['+MINION_NAME+']['+inst_id+'] Connected, retrieving job status.');

            // Execute command
            var status = "finished";
            var cmd = 'ps -ef | tr -s [:blank:] | cut -d " " -f 2 | grep -x '+job_id
            utils.execSSH(conn, cmd, null, true, function(error, output){
               if(error){
                  // Close connection
                  utils.closeSSH(conn);
                  return getCallback(error);
               }

               // Status depends on the output
               if(output.stdout != ""){
                  // The job is running
                  status = "running";
               }

               // Close connection
               utils.closeSSH(conn);

               //console.log('['+MINION_NAME+']['+inst_id+'] Retrieved job status.');
               getCallback(null, status);
            }); // execSSH
         }); // connectSSH
      }); // getImages
   }); // getInstances
}

var _getOpenStackInstance = function(inst_id, getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers/' + inst_id,
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);

      // Success??
      var server = JSON.parse(body).server;
      if(!server) return getCallback(new Error("Failed to get instance '"+inst_id+"': \n"), JSON.stringify(JSON.parse(body), null, 2));
      getCallback(null, server);
   });
}

var _getOpenStackQuotas = function(getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/limits',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);

      // Success??
      var quotas = JSON.parse(body);
      if(!quotas) return getCallback(new Error("Failed to get quotas.\n"), JSON.stringify(JSON.parse(body), null, 2));
      getCallback(null, quotas);
   });
}

var _getOpenStackInstanceIPs = function(inst_id, getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   var req = {
      url: compute_url + '/servers/' + inst_id + '/ips/' + network_label,
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return getCallback(error);
      getCallback(null, JSON.parse(body)[network_label]);
   });
}

var _getOpenStackInstanceFloatingIP = function(inst_id, getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   // Get OpenStack instance data
   _getOpenStackInstance(inst_id, function(error, os_inst){
      if(error) return getCallback(error);

      // Get IP (prefer floating)
      var ip_array = os_inst.addresses[network_label];
      var ip = ip_array[0].addr;
      for(var i = 0; i < ip_array.length; i++){
         if(ip_array[i]['OS-EXT-IPS:type'] == 'floating'){
            ip = ip_array[i].addr;
            break;
         }
      }
   });
}

var _getOpenStackFreeFloatIP = function(getCallback){
   if(!token) return getCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return getCallback(new Error('Compute service URL is not defined.'));

   // TODO: No way to know by now if an IP has been selected for other instance
   // No IP found, allocate
   return _allocateOpenStackFloatIP(function(error, ip){
      if(error) return getCallback(error);
      getCallback(null, ip);
   });

   var req = {
      url: compute_url + '/os-floating-ips',
      method: 'GET',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
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
   if(!token) return assignCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return assignCallback(new Error('Compute service URL is not defined.'));

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
   _requestOpenStack(req, function(error, res, body){
      if(error) return allocateCallback(error);
      if(!body.floating_ip) return allocateCallback(new Error("Error getting field in float IP response: "+JSON.stringify(body)));
      allocateCallback(null, body.floating_ip);
   });
}

var _deallocateOpenStackFloatingIP = function(ip_id, deallocateCallback){
   if(!token) return deallocateCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return deallocateCallback(new Error('Compute service URL is not defined.'));

   // Request type
   var req = {
      url: compute_url + '/os-floating-ips/' + ip_id,
      method: 'DELETE',
      headers: {
         'X-Auth-Token': token,
      }
   };

   // Send request
   _requestOpenStack(req, function(error, res, body){
      if(error) return deallocateCallback(error);
      deallocateCallback(null);
   });
}

var _assignOpenStackFloatIPInstance = function(inst_id, assignCallback){
   if(!token) return assignCallback(new Error('Minion is not connected to OpenStack cloud.'));
   if(!compute_url) return assignCallback(new Error('Compute service URL is not defined.'));

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
      _requestOpenStack(req, function(error, res, body){
         if(error) return assignCallback(error);
         assignCallback(null, ip);
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

            // Setup keypair
            keypair_name = config.keypairname;
            if(!keypair_name) console.error("["+MINION_NAME+"] No keypair name provided.");

            // Setup private key path
            private_key_path = config.privatekeypath;
            if(!private_key_path) console.error("["+MINION_NAME+"] No private key path provided.");

            // Next
            wfcb(null);
         });
      },
      // Setup RPC
      function(wfcb){
         // Setup server
         zserver = new zerorpc.Server({
            login: login,
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
                  if(image.os_id == images[j].os_id){
                     // Found
                     console.log('['+MINION_NAME+'] Image "'+image.os_id+'" exists already.');
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
                  console.log('['+MINION_NAME+'] Added image "'+image.id+'" to database.')
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
                  if(size.os_id == sizes[j].os_id){
                     // Found
                     console.log('['+MINION_NAME+'] Size "'+size.os_id+'" exists already.');
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
                  console.log('['+MINION_NAME+'] Added size "'+size.id+'" to database.')
               }
            }

            // Next
            wfcb(null);
         });
      },
      // Setup compatibility
      function(wfcb){
         console.log("["+MINION_NAME+"] Setting compatibility between images and sizes...");
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
                     for(var z = 0; z < image.os_sizes_compatible.length; z++){
                        var comp_id = image.os_sizes_compatible[z];
                        if(size.os_id == comp_id){
                           // Compatible!
                           sizes_compatible.push(size.id);
                        }
                     }
                  }
                  // Save changes
                  console.log('['+MINION_NAME+'] Sizes "'+sizes_compatible+'" are compatible with image "'+image.id+'".');
                  database.collection('images').updateOne({id: image.id},{$set:{sizes_compatible:sizes_compatible}});
               }
            });
         });
      },
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
                  // Not in OpenStack?
                  if(error){
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

var _requestOpenStack = function(req, requestCallback){

   // Set correct token
   req.headers['X-Auth-Token'] = token;

   // Send request
   request(req, function(error, res, body){
      if(error) return requestCallback(error);

      // Check authentication
      if(typeof body == 'string' && body.includes('Authentication required')){
         // Relogin
         console.log('['+MINION_NAME+'] Trying relogin...');
         return login(function(error){
            setTimeout(_requestOpenStack, 3000, req, requestCallback);
         });
      }

      return requestCallback(null, res, body);
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
         // Set task
         setInterval(_cleanOpenStackMissingInstances, 10000, function(error){
            if(error) console.error(error);
         });
         if(error) return wfcb(error);
         wfcb(null);
      });
   },
],
function(error){
   if(error) throw error;
   console.info('['+MINION_NAME+'] Initialization completed.');

   // Login
   login(function(error){
      if(error) {
         console.error('['+MINION_NAME+'] Failed to login.');
         throw new Error('Failed to login.')
      }
      console.info('['+MINION_NAME+'] New token: '+token);
   });

   //__test();
});

// TODO: Remove, some tests
var __test = function(){
   var instances = null;
   var inst_id = null;
   // Steps
   async.waterfall([
      // List instances
      function(wfcb){
         console.log("TEST: Getting instances...");
         getInstances(null, function(error, list){
            if(error) wfcb(error);
            console.log("TEST: Instances:");
            console.log(list);
            instances = list;
            wfcb(null);
         });
      },
      // Create instance
      function(wfcb){
         if(instances.length == 0){
            var inst_cfg = {
               name: "Test inst from minion",
               image_id: "81743971-6f56-4c9f-a557-2edf4516d185",
               size_id: "3",
               publicIP: true
            };
            console.log("TEST: Creating: ");
            console.log(JSON.stringify(inst_cfg, null, 2));
            createInstance(inst_cfg, function(error, inst_id2){
               if(error) return wfcb(error);
               console.log("Created "+inst_id2);
               inst_id = inst_id2;
               wfcb(null);
            });
         } else {
            inst_id = instances[0].id;
            console.log("TEST: Selected existing instance - "+inst_id);
            wfcb(null);
         }
      },
      // Execute script
      function(wfcb){
         var script = "pwd";
         var work_dir = "/home/fedora";
         console.log("TEST: Executing ("+work_dir+") script: "+script);
         //executeScript(script, work_dir, inst_id, 1, function(error, pid){
         //   if(error) return wfcb(error);
         //   console.log("TEST: Waiting for: " + pid);
         //   __waitJob(pid, inst_id, function(error){
         //      if(error) return wfcb(error);
         //      wfcb(null);
         //   });
         //});
         executeCommand(script, inst_id, function(error, output){
            if(error) return wfcb(error);
            console.log("TEST: Output: " + output);
            wfcb(null);
         });
      },
      // Destroy
      function(wfcb){
         //console.log("Destroying:");
         //destroyInstance(inst_id, function(error){
         //   if(error) wfcb(error);
         //   console.log("Destroyed!");
         //   destroyInstance(inst_id, function(error){
         //      if(error) wfcb(error);
         //      wfcb(null);
         //   });
         //});
         wfcb(null);
      }
   ],
   function(error){
      if(error) return console.error(error);
      console.log("TEST: Test done");
   });
}

var __waitJob = function(job_id, inst_id, waitCallback){
   getJobStatus(job_id, inst_id, function(error, status){
      if(error) return waitCallback(error);
      if(status == "running"){
         setTimeout(__waitJob, 1000, job_id, inst_id, waitCallback);
      } else {
         waitCallback(null);
      }
   });
}
