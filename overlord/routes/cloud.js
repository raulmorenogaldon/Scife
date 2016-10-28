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
            utils.logger.debug('['+MODULE_NAME+'] Authenticated user '+token_decoded.user_id);
            next();
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
      res.json(result);
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
            'json': codes.ERRCODE.ID_NOT_FOUND
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
   res.json(req.size);
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
      res.json(result);
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
            'json': codes.ERRCODE.ID_NOT_FOUND
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
   res.json(req.instance);
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
      res.json(result);
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
            'json': codes.ERRCODE.ID_NOT_FOUND
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
   res.json(req.image);
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
      res.json(result);
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
            res.json(result);
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
      res.json(result);
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
   scheduler.searchExperiments(null, function (error, result) {
      if(error) return next(error);
      res.json(result);
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
      // Create experiment
      scheduler.createExperiment(req.body, function (error, result) {
         if (error) {
            return next(error);
         } else {
            res.json(result);
         }
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
         // Set parameter
         req.exp = exp;
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
   res.json({
      'id': req.exp.id,
      'name': req.exp.name,
      'desc': req.exp.desc,
      'app_id': req.exp.app_id,
      'status': req.exp.status,
      'labels': req.exp.labels,
      'input_tree': req.exp.input_tree,
      'output_tree': req.exp.output_tree,
      'src_tree': req.exp.src_tree
   });
});

/**
 * Get experiment logs
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment logs
 */
router.get('/experiments/:exp_id/logs', function (req, res, next) {
   scheduler.getExperiment(req.params.exp_id, {id: 1, logs: 1}, function (error, result) {
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
               'json': codes.ERRCODE.EXP_LOG_NOT_FOUND
            });
         }

         // Response
         res.set('Content-Type', 'text/plain');
         res.send(fcontent);
      } else {
         // Response all logs
         res.json(result);
      }
   });
});

/**
 * Get experiment sources tree
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment sources tree
 */
router.get('/experiments/:exp_id/srctree', function (req, res, next) {
   scheduler.getExperiment(req.params.exp_id, {id: 1, src_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      result.src_tree = utils.cutTree(result.src_tree, fpath, depth);
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_CODE_FILE_NOT_FOUND]
         });
      }

      res.json(result);
   });
});

/**
 * Get experiment input files tree
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment input files tree
 */
router.get('/experiments/:exp_id/inputtree', function (req, res, next) {
   scheduler.getExperiment(req.params.exp_id, {id: 1, input_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      result.input_tree = utils.cutTree(result.input_tree, fpath, depth);
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_INPUT_FILE_NOT_FOUND]
         });
      }

      res.json(result);
   });
});

/**
 * Get experiment output files tree
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment output files tree
 */
router.get('/experiments/:exp_id/outputtree', function (req, res, next) {
   scheduler.getExperiment(req.params.exp_id, {id: 1, output_tree: 1}, function (error, result) {
      if (error) return next(error);

      // Get folder path and depth if provided
      var fpath = req.query.folder;
      var depth = req.query.depth;
      result.output_tree = utils.cutTree(result.output_tree, fpath, depth);
      if(!result){
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_OUTPUT_FILE_NOT_FOUND]
         });
      }

      res.json(result);
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
            'errors': [codes.ERRCODE.EXP_CODE_FILE_NOT_FOUND]
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
      scheduler.reloadExperimentTree(req.params.exp_id, true, true, true, function(error){
         if (error) return next(error);
         res.json(null);
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

      // TODO: Reload labels
      // ...

      // Reload trees
      scheduler.reloadExperimentTree(req.params.exp_id, false, false, true, function(error){
         if (error) return next(error);
         res.json(null);
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
      scheduler.reloadExperimentTree(req.params.exp_id, true, false, false, function(error){
         if (error) return next(error);
         res.json(null);
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
      scheduler.reloadExperimentTree(req.params.exp_id, true, false, false, function(error){
         if (error) return next(error);
         res.json(null);
      });
   });
});

/**
 * Get download link for experiment output data
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with output data
 */
router.get('/experiments/:exp_id/download', function (req, res, next) {
   // Get file path if provided
   var fpath = req.query.file;

   // Get file
   scheduler.getExperimentOutputFile(req.params.exp_id, fpath, function (error, file) {
      if (error) {
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'errors': [codes.ERRCODE.EXP_NO_OUTPUT_DATA]
         });
      } else {
         // Create header with file info
         var stat = fs.statSync(file);
         var filename = req.exp.name+'.tar.gz';
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

/**
 * Update experiment metadata
 * @param {String} - The experiment id.
 */
router.put('/experiments/:exp_id', function (req, res, next) {
   scheduler.updateExperiment(req.params.exp_id, req.body, function (error, result, more) {
      if (error) return next(error);
      res.json(result);
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
      res.json(null);
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
         if(req.body.debug == null) req.body.debug = true;

         // Launch experiment
         scheduler.launchExperiment(req.params.exp_id, req.body.nodes, req.body.image_id, req.body.size_id, req.body.debug, function(error){
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
            res.json(null);
         });
      }
   } else if (req.body.op == "reset") {
      // Reset experiment
      scheduler.resetExperiment(req.params.exp_id, function(error){
         if(error) return next(error);
         res.json(null);
      });
   } else {
      // Unknown operation
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.EXP_UNKNOWN_OPERATION]
      });
   }
});

module.exports = router;
