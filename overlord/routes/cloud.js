var express = require('express'),
   multer = require('multer'),
   router = express.Router(),
   zerorpc = require('zerorpc'),
   fs = require('fs'),
   mpath = require('path');

var jwt = require('jsonwebtoken');

var codes = require('../error_codes.js');
var utils = require('../utils.js');
var scheduler = require('../scheduler.js');
var instmanager = require('../instance.js');
var usermanager = require('../users.js');
var appmanager = require('../application.js');
var execmanager = require('../execution.js');

/**
 * Multer tmp uploads
 */
var upload = multer({dest: '/tmp/'});

/**
 * Module vars
 */
var MODULE_NAME = "RT";


/***********************************************************
 * --------------------------------------------------------
 * AUTHENTICATION
 * --------------------------------------------------------
 ***********************************************************/
router.use('/', function(req, res, next){
   // Check token in header
   if(req.headers && req.headers['x-access-token']){
      // Get token
      var token = req.headers['x-access-token'];

      // Verify token
      jwt.verify(token, app.get('constants').SECRET, function(error, token_decoded) {
         if(error){
            // Token is not valid
            utils.logger.debug('['+MODULE_NAME+'] Invalid token');
            return next({
               'http': codes.HTTPCODE.UNAUTHORIZED,
               'errors': [codes.ERRCODE.AUTH_FAILED]
            });
         } else {
            // Save decoded token
            req.auth = token_decoded;
            //utils.logger.debug('['+MODULE_NAME+'] Authenticated user '+token_decoded.username+ ' - '+token_decoded.id);
            return next();
         }
      });
   } else {
      // Token is needed
      utils.logger.debug('['+MODULE_NAME+'] No token provided');
      return next({
         'http': codes.HTTPCODE.UNAUTHORIZED,
         'errors': [codes.ERRCODE.AUTH_REQUIRED]
      });
   }
});

/***********************************************************
 * --------------------------------------------------------
 * SIZE METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Return a list with the sizes in the server.
 * @return {[Object]} - A json object list with the follow strucutre:
 * 	[{ "id":"size id", "name":"size name", "desc":"Description", "cpus":"Nº CPUs", "ram":"ram in mb"}]
 */
router.get('/sizes', function (req, res, next) {
   instmanager.getSizesList(function (error, result){
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * Check size id parameter
 * @param {String} - Size ID.
 */
router.param('size_id', function(req, res, next, size_id){
   // Get size
   instmanager.getSize(size_id, function (error, size) {
      // Error retrieving this size
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.ID_NOT_FOUND]
         });
      } else {
         // Set parameter
         req.size = size;
         return next();
      }
   });
});

/**
 * Get the size info from de server.
 * @param {String} - The size id.
 * @return {Object} - A json Object with the follow structure: { "id":"size id", "name":"size name", "desc":"Description", "cpus":"Nº CPUs", "ram":"ram in mb"}
 */
router.get('/sizes/:size_id', function (req, res, next) {
   return res.json(req.size);
});

/***********************************************************
 * --------------------------------------------------------
 * INSTANCE METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get a instance list from the server.
 * @return {[Obect]} - A json object list with the follow strucutre:
 * 	[{ "id":"instance id", "name":"name", "desc":"Description", "image_id":"image id", "size_id":"size id"}]
 */
router.get('/instances', function (req, res, next) {
   instmanager.getInstancesList(function (error, result){
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * Check instance id parameter
 * @param {String} - Instance ID.
 */
router.param('instance_id', function(req, res, next, instance_id){
   // Get instance
   instmanager.getInstance(instance_id, function (error, instance) {
      // Error retrieving this instance
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.ID_NOT_FOUND]
         });
      } else {
         // Set parameter
         req.instance = instance;
         return next();
      }
   });
});

/**
 * Get the instance info from de server.
 * @param {String} - The instance id.
 * @return {Object} - A json Object with the follow structure: {"id":"instance id", "name":"name", "desc":"description", "image_id":"image id", "size_id":"size id"}
 */
router.get('/instances/:instance_id', function (req, res, next) {
   return res.json(req.instance);
});


/***********************************************************
 * --------------------------------------------------------
 * IMAGE METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get image list from the server
 * @return {[Object]} - A  list of json objects with she follow strucutre: [{"id":"image id", "name":"name", "desc":"description"}]
 */
router.get('/images', function (req, res, next) {
   instmanager.getImagesList(function (error, result){
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * Check image id parameter
 * @param {String} - Image ID.
 */
router.param('image_id', function(req, res, next, image_id){
   // Get image
   instmanager.getImage(image_id, function (error, image) {
      // Error retrieving this image
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.ID_NOT_FOUND]
         });
      } else {
         // Set parameter
         req.image = image;
         return next();
      }
   });
});

/**
 * Get the image infor fron de server.
 * @param {String} - The image id.
 * @return {Object} - A json Object with the follow structure: {"id":"image id", "name":"name", "desc":"description"}
 */
router.get('/images/:image_id', function (req, res, next) {
   return res.json(req.image);
});

/***********************************************************
 * --------------------------------------------------------
 * APPLICATION METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get applications from the storage
 * @return {[Object]} - A json Object with application metadata
 */
router.get('/applications', function (req, res, next) {
   scheduler.searchApplications(null, function (error, result) {
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * This method allow to create a new application.
 * @param {Object} - A json Object with application metadata
 * @return {[Object]} - A json Object with application ID
 */
router.post('/applications', function (req, res, next) {
   if (!req.body.name || !req.body.creation_script || !req.body.execution_script || !req.body.path) {
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.APP_INCORRECT_PARAMS]
      });
   } else {
      // Create experiment
      scheduler.createApplication(req.body, function (error, result) {
         if (error) {
            return next({
               'http': codes.HTTPCODE.INTERNAL_ERROR,
               'errors': [codes.ERRCODE.EXP_INCORRECT_PARAMS]
            });
         } else {
            return res.json(result);
         }
      });
   }
});

/**
 * Check application id parameter
 * @param {String} - The application ID.
 */
router.param('app_id', function(req, res, next, app_id){
   // Get application
   scheduler.getApplication(app_id, function (error, app) {
      // Error retrieving this application
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.APP_NOT_FOUND]
         });
      } else {
         // Set parameter
         req.app = app;
         return next();
      }
   });
});

/**
 * Get application metadata from the storage using its ID
 * @param {String} - The application id.
 * @return {[Object]} - A json Object with application metadata
 */
router.get('/applications/:app_id', function (req, res, next) {
   scheduler.getApplication(req.params.app_id, function (error, result, more) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [
               codes.ERRCODE.ID_NOT_FOUND,
               codes.ERRCODE.APP_NOT_FOUND
            ]
         });
      }
      return res.json(result);
   });
});

/**
 * Update application metadata
 * @param {String} - The application id.
 */
router.put('/applications/:app_id', function (req, res, next) {
   return next({
      'http': codes.HTTPCODE.NOT_IMPLEMENTED,
      'errors': [codes.ERRCODE.NOT_IMPLEMENTED]
   });
});

/**
 * Perform an operation over an application
 * @param {String} - The application id.
 */
router.post('/applications/:app_id', function (req, res, next) {
   // Check if operation has been requested
   if (!req.body.op){
      // No operation requested
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.APP_NO_OPERATION]
      });
   }

   // Execute operation
   appmanager.maintainApplication(req.params.app_id, req.body.op, function (error) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'errors': [error.message]
         });
      }
      return res.json(null);
   });
});

/**
 * Delete application
 * @param {String} - The application id.
 */
router.delete('/applications/:app_id', function (req, res, next) {
   return next({
      'http': codes.HTTPCODE.NOT_IMPLEMENTED,
      'errors': [codes.ERRCODE.NOT_IMPLEMENTED]
   });
});

/***********************************************************
 * --------------------------------------------------------
 * EXPERIMENT METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get experiments from the storage
 * @return {[Object]} - A json Object with experiments metadata
 */
router.get('/experiments', function (req, res, next) {
   // Get selected user
   var selected_user = req.query.user ? req.query.user : req.auth.id;

   // User exists?
   usermanager.getUser(selected_user, function(error, user){
      if(error) return next({
         'http': codes.HTTPCODE.NOT_FOUND,
         'errors': [codes.ERRCODE.USER_NOT_FOUND]
      });

      // Get ID
      var user_id = user.id;
      if(user_id != req.auth.id){
         // Check admin
         if(!req.auth.admin){
            return next({
               'http': codes.HTTPCODE.FORBIDDEN,
               'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
            });
         }
      }

      // Search fields
      var fields = {
         owner: user_id
      }

      // Search
      scheduler.searchExperiments(fields, function (error, result) {
         if(error) return next(error);
         return res.json(result);
      });
   });
});

/**
 * Create experiment metadata
 * @param {String} - The experiment id.
 */
router.post('/experiments', function (req, res, next) {
   if (!req.body.name || !req.body.app_id) {
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_INCORRECT_PARAMS]
      });
   } else {
      // Set owner
      req.body.owner = req.auth.id;

      // Create experiment
      scheduler.createExperiment(req.body, function (error, result) {
         if (error) return next(error);
         return res.json(result);
      });
   }
});

/**
 * Check experiment id parameter
 * @param {String} - The experiment ID.
 */
router.param('exp_id', function(req, res, next, exp_id){
   // Get experiment
   scheduler.getExperiment(exp_id, null, function (error, exp) {
      // Error retrieving this experiment
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_NOT_FOUND]
         });
      } else {
         // Check permissions
         if(exp.owner != req.auth.id && !req.auth.admin){
            return next({
               'http': codes.HTTPCODE.FORBIDDEN,
               'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
            });
         }
         // Set parameter
         req.exp = exp;
         req.exp_minimal = {
            id: exp.id,
            name: exp.name,
            app_id: exp.app_id
         };

         return next();
      }
   });
});

/**
 * Get experiment metadata from the storage using its ID
 * @param {String} - The experiment ID.
 * @return {[Object]} - A json Object with experiment metadata
 */
router.get('/experiments/:exp_id', function (req, res, next) {
   var exp = {
      'id': req.exp.id,
      'name': req.exp.name,
      'desc': req.exp.desc,
      'app_id': req.exp.app_id,
      'last_execution': req.exp.last_execution,
      'last_execution_status': req.exp.last_execution_status,
      'labels': req.exp.labels
   }

   // Response
   return res.json(exp);
});


/**
 * Get experiment sources tree
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment sources tree
 */
router.get('/experiments/:exp_id/srctree', function (req, res, next) {
   // Retrieve sources tree
   scheduler.getExperiment(req.params.exp_id, {id: 1, name: 1, src_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_CODE_FILE_NOT_FOUND]
         });
      }
      result.src_tree = utils.cutTree(result.src_tree, fpath, depth);

      return res.json(result);
   });
});

/**
 * Get experiment input files tree
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment input files tree
 */
router.get('/experiments/:exp_id/inputtree', function (req, res, next) {
   // Retrieve input tree
   scheduler.getExperiment(req.params.exp_id, {id: 1, name: 1, input_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_INPUT_FILE_NOT_FOUND]
         });
      }
      result.input_tree = utils.cutTree(result.input_tree, fpath, depth);

      return res.json(result);
   });
});

/**
 * Get experiment source file content
 * @param {String} - The experiment id.
 * @return {[Object]} - File contents
 */
router.get('/experiments/:exp_id/code', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;
   if(!fpath){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_CODE_FILE_PATH_MISSING]
      });
   }

   // Get file contents
   scheduler.getExperimentCode(req.params.exp_id, fpath, function (error, fcontent) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'errors': [error.message]
         });
      } else {
         res.set('Content-Type', 'text/plain');
         res.send(fcontent);
      }
   });
});

/**
 * Delete source file
 * @param {String} - The experiment id.
 */
router.delete('/experiments/:exp_id/code', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;
   if(!fpath){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_CODE_FILE_PATH_MISSING]
      });
   }

   // Save to experiment data
   scheduler.deleteExperimentCode(req.params.exp_id, fpath, function(error){
      // Error deleting file?
      if(error) return next(error);

      // Reload trees
      scheduler.reloadExperimentTree(req.params.exp_id, true, true, function(error){
         if (error) return next(error);
         return res.json(null);
      });
   });
});

/**
 * Save file changes
 * @param {String} - The experiment id.
 * @return {[Object]} - File contents
 */
router.post('/experiments/:exp_id/code', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;
   if(!fpath){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_CODE_FILE_PATH_MISSING]
      });
   }

   // Check if passed folder or filename
   var fcontent = null;
   if(fpath.slice(-1) != '/'){
      // Check data type
      if(!req.text){
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'errors': [codes.ERRCODE.REQ_CONTENT_TYPE_TEXT_PLAIN]
         });
      }

      // File content in body
      fcontent = req.text;
   }

   // Save file
   scheduler.putExperimentCode(req.params.exp_id, fpath, fcontent, function (error) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'errors': [codes.ERRCODE.EXP_CODE_FILE_NOT_FOUND]
         });
      }

      // Reload labels
      scheduler.maintainExperiment(req.params.exp_id, "discoverLabels", function(error){
         if (error) return next(error);

         // Reload trees
         scheduler.reloadExperimentTree(req.params.exp_id, false, true, function(error){
            if (error) return next(error);
            return res.json(null);
         });
      });
   });
});

/**
 * Delete input file
 * @param {String} - The experiment id.
 */
router.delete('/experiments/:exp_id/input', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;
   if(!fpath){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_INPUT_FILE_PATH_MISSING]
      });
   }

   // Save to experiment data
   scheduler.deleteExperimentInput(req.params.exp_id, fpath, function(error){
      // Error deleting file?
      if(error) return next(error);

      // Reload trees
      scheduler.reloadExperimentTree(req.params.exp_id, true, false, function(error){
         if (error) return next(error);
         return res.json(null);
      });
   });
});

/**
 * Upload input file
 * @param {String} - The experiment id.
 * @return {[Object]} - File contents
 */
router.post('/experiments/:exp_id/input', upload.array('inputFile'), function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;
   if(!fpath){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_INPUT_FILE_PATH_MISSING]
      });
   }

   // Check if passed folder or filename
   var finfo = null;
   var tmpfile = null;
   if(fpath.slice(-1) != '/'){
      // Check files in packet
      if(!req.files || !req.files.length || req.files.length < 1){
         return next(new Error("Malformed uploaded data."));
      }
      // File, get uploaded file path
      finfo = req.files[0];
      tmpfile = finfo.path;
   }

   // Save to experiment data
   scheduler.putExperimentInput(req.params.exp_id, fpath, tmpfile, function(error){
      // Remove file
      if(tmpfile) fs.unlink(tmpfile, function(error){
         if(error) console.error(error);
      });

      // Error saving file?
      if(error) return next(error);

      // Reload trees
      scheduler.reloadExperimentTree(req.params.exp_id, true, false, function(error){
         if (error) return next(error);
         return res.json(null);
      });
   });
});

/**
 * Update experiment metadata
 * @param {String} - The experiment id.
 */
router.put('/experiments/:exp_id', function (req, res, next) {
   scheduler.updateExperiment(req.params.exp_id, req.body, function (error, result, more) {
      if (error) return next(error);
      return res.json(result);
   });
});

/**
 * Delete experiment
 * @param {String} - The experiment id.
 */
router.delete('/experiments/:exp_id', function (req, res, next) {
   // Remove experiment
   scheduler.destroyExperiment(req.params.exp_id, function(error){
      if(error) return next(error);
      return res.json(null);
   });
});

/**
 * Perform an operation in an experiment
 * @param {String} - The experiment id.
 */
router.post('/experiments/:exp_id', function (req, res, next) {
   if (!req.body.op){
      // No operation requested
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_NO_OPERATION]
      });
   } else if (req.body.op == "launch"){
      if (!req.body.nodes || !req.body.image_id || !req.body.size_id) {
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'errors': [codes.ERRCODE.LAUNCH_INCORRECT_PARAMS]
         });
      } else {
         // Set default debug
         if(req.body.debug == null) req.body.debug = false;

         // Launch experiment
         scheduler.launchExperiment(req.params.exp_id, req.body.nodes, req.body.image_id, req.body.size_id, {
            debug:req.body.debug,
            checkpoint_load:req.body.load_checkpoint,
            checkpoint_interval:req.body.checkpoint_interval
         }, function(error){
            if(error){
               // Quota reached?
               if(error.message.includes("quota")){
                  return next({
                     'http': codes.HTTPCODE.BAD_REQUEST,
                     'errors': [codes.ERRCODE.LAUNCH_QUOTA_REACHED]
                  });
               } else {
                  // Unknown
                  return next(error);
               }
            }
            return res.json(null);
         });
      }
   } else if (req.body.op == "reset") {
      // Delete last execution
      if(req.exec){
         scheduler.abortExecution(req.exec.id, function(error){
            if(error) return next(error);
            return res.json(null);
         });
      } else {
         return res.json(null);
      }
   } else if (req.body.op == "reloadTrees") {
      // Reload experiment trees
      scheduler.reloadExperimentTree(req.params.exp_id, true, true, function(error){
         if(error) logger.error('['+MODULE_NAME+']['+req.params.exp_id+'] Failed to reload experiment trees.');
         if(req.exec){
            // Reload execution trees
            scheduler.reloadExecutionOutputTree(req.exec.id, function(error){
               if(error) logger.error('['+MODULE_NAME+']['+req.exec.id+'] Failed to reload execution output tree.');
            });
         }
      });
      return res.json(null);
   } else {
      // Unknown operation
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_UNKNOWN_OPERATION]
      });
   }
});

/***********************************************************
 * --------------------------------------------------------
 * EXECUTION METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get executions metadata from the storage using its ID
 * @param {String} - The execution ID.
 * @return {[Object]} - A json Object with execution metadata
 */
router.get('/executions', function (req, res, next) {
   var fields = {};

   // Set queries
   if(req.query.exp) fields.exp_id = req.query.exp;
   if(req.query.deleted != 1) fields.status = {'$nin': ["deleted", "deleting"]};

   // Search
   execmanager.searchExecutions(fields, function (error, result) {
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * Check execution id parameter
 * @param {String} - The execution ID.
 */
router.param('exec_id', function(req, res, next, exec_id){
   // Get execution
   execmanager.getExecution(exec_id, null, function (error, exec) {
      if(error) return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXEC_NOT_FOUND]
      });

      // Set execution parameter
      req.exec = exec;

      // Get execution experiment
      scheduler.getExperiment(exec.exp_id, null, function(error, exp){
         if(!exp) return next();

         // Check permissions
         if(exp.owner != req.auth.id && !req.auth.admin) return next({
            'http': codes.HTTPCODE.FORBIDDEN,
            'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
         });

         // Set parameters
         req.exec.exp_name = exp.name;
         return next();
      });
   });
});

/**
 * Get execution metadata from the storage using its ID
 * @param {String} - The experiment ID.
 * @param {String} - The execution ID.
 * @return {[Object]} - A json Object with execution metadata
 */
router.get('/executions/:exec_id', function (req, res, next) {
   // Project execution
   var exec = {
      'id': req.exec.id,
      'parent_id': req.exec.parent_id,
      'exp_id': req.exec.exp_id,
      'name': req.exec.name,
      'status': req.exec.status,
      'create_date': req.exec.create_date,
      'launch_date': req.exec.launch_date,
      'finish_date': req.exec.finish_date,
      'launch_opts': req.exec.launch_opts,
      'labels': req.exec.labels,
      'usage': req.exec.usage
   }
   res.json(exec);
});

/**
 * Delete execution
 * @param {String} - The experiment id.
 */
router.delete('/executions/:exec_id', function (req, res, next) {
   // Remove execution
   scheduler.destroyExecution(req.exec.id, function(error){
      if(error) return next(error);
      return res.json(null);
   });
});

/**
 * Get execution logs
 * @param {String} - The execution id.
 * @return {[Object]} - A json Object with execution logs
 */
router.get('/executions/:exec_id/logs', function (req, res, next) {
   // Get logs
   execmanager.getExecution(req.exec.id, {id: 1, exp_id: 1, logs: 1}, function (error, result) {
      if (error) return next(error);

      // Provided specific log?
      var log = req.query.log;
      if(log){
         // Search this log in result
         var fcontent = null;
         for(var i = 0; i < result.logs.length; i++){
            if(result.logs[i].name == log){
               fcontent = result.logs[i].content;
               break;
            }
         }

         // Not found
         if(!fcontent){
            return next({
               'http': codes.HTTPCODE.NOT_FOUND,
               'errors': [codes.ERRCODE.EXEC_LOG_NOT_FOUND]
            });
         }

         // Response
         res.set('Content-Type', 'text/plain');
         res.send(fcontent);
      } else {
         // Response all logs
         result.exp_name = req.exec.exp_name;
         return res.json(result);
      }
   });
});

/**
 * Get execution output files tree
 * @param {String} - The execution id.
 * @return {[Object]} - A json Object with execution output files tree
 */
router.get('/executions/:exec_id/outputtree', function (req, res, next) {
   // Get output tree
   execmanager.getExecution(req.exec.id, {id: 1, exp_id: 1, output_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_OUTPUT_FILE_NOT_FOUND]
         });
      }
      // Response requested tree
      result.output_tree = utils.cutTree(result.output_tree, fpath, depth);
      result.exp_name = req.exec.exp_name;
      return res.json(result);
   });
});


/**
 * Get download link for execution output data
 * @param {String} - The execution id.
 * @return {[Object]} - A json Object with output data
 */
router.get('/executions/:exec_id/download', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;

   // Get file
   scheduler.getExecutionOutputFile(req.exec.id, fpath, function (error, file) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXEC_NO_OUTPUT_DATA]
         });
      } else {
         // Create header with file info
         var stat = fs.statSync(file);
         var filename = req.exec.exp_name+'.tar.gz';
         if(fpath) filename = mpath.basename(file);
         res.writeHead(200, {
            'Content-Type': 'application/octet-stream',
            'Content-Length': stat.size,
            'Content-Disposition': 'inline; filename="'+filename+'"'
         });

         // Send file
         var readStream = fs.createReadStream(file);
         readStream.pipe(res);
      }
   });
});

/***********************************************************
 * --------------------------------------------------------
 * USERS METHODS
 * --------------------------------------------------------
 ***********************************************************/

/**
 * Get users
 * @return {[Object]} - A json Object with users metadata
 */
router.get('/users', function (req, res, next) {
   // Check permissions
   if(!req.auth.admin){
      return next({
         'http': codes.HTTPCODE.FORBIDDEN,
         'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
      });
   }

   // List all users
   usermanager.searchUsers(null, function (error, result) {
      if(error) return next(error);
      return res.json(result);
   });
});

/**
 * Create user metadata
 * @return {[Object]} - A json Object with the new user metadata
 */
router.post('/users', function (req, res, next) {
   // Check permissions
   if(!req.auth.admin){
      return next({
         'http': codes.HTTPCODE.FORBIDDEN,
         'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
      });
   }

   // Check params
   if (!req.body.username || !req.body.password) {
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.USER_CREATE_INCORRECT_PARAMS]
      });
   }

   // Create user
   usermanager.createUser(req.body.username, req.body.password, req.body.admin, function (error, result) {
      if (error){
         return next({
            'http': codes.HTTPCODE.CONFLICT,
            'errors': [codes.ERRCODE.USER_CREATE_USERNAME_UNAVAILABLE]
         });
      }
      return res.json(result);
   });
});

/**
 * Check user id parameter
 * @param {String} - User ID.
 */
router.param('user_id', function(req, res, next, user_id){
   // Get size
   usermanager.getUser(user_id, function (error, user) {
      // Error retrieving this user
      if(error){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.ID_NOT_FOUND]
         });
      } else {
         // Set parameter
         req.user = user;
         return next();
      }
   });
});

/**
 * Get user metadata
 * @param {String} - The user ID.
 * @return {[Object]} - A json Object with user metadata
 */
router.get('/users/:user_id', function (req, res, next) {
   // Check permissions
   if(!req.auth.admin && req.auth.username != req.user.username){
      return next({
         'http': codes.HTTPCODE.FORBIDDEN,
         'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
      });
   }

   // Return data
   return res.json({
      'id': req.user.id,
      'username': req.user.username,
      'admin': req.user.admin
   });
});

/**
 * Update user permissions
 * @param {String} - The user ID.
 */
router.put('/users/:user_id/permissions', function (req, res, next) {
   // Check permissions
   if(!req.auth.admin){
      return next({
         'http': codes.HTTPCODE.FORBIDDEN,
         'errors': [codes.ERRCODE.AUTH_PERMISSION_DENIED]
      });
   }

   if(!typeof req.body.permission === 'string' || !typeof req.body.value === 'string'){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.USER_PERMISSIONS_INCORRECT_PARAMS]
      });
   }

   // Set permissions
   usermanager.setUserPermissions(req.params.user_id, req.body.permission, req.body.value, req.body.allow, function (error, result) {
      if (error) return next(error);
      return res.json(null);
   });
});

module.exports = router;
