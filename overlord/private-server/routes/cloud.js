var express = require('express'),
   router = express.Router(),
   zerorpc = require('zerorpc'),
   fs = require('fs'),
   constants = require('../constants.json');

var codes = require('../error_codes.js');
var scheduler = require('../scheduler.js');
var instmanager = require('../instance.js');


/***********************************************************
 * --------------------------------------------------------
 * LOGIN METHODS
 * --------------------------------------------------------
 ***********************************************************/

// The config object depends on the provider
//router.get('/login', function(req, res, next){
//   var config = {
//      'url': "galgo.i3a.info",
//      'username': "rmoreno2",
//   }
//   minionClient.invoke("login", config, function(error, result, more){
//      if(error){
//         console.log("Error in the request /login");
//         res.status(500); //Internal server error
//         res.json(error);
//      }else{
//         res.json(result);
//      }
//   });
//});

/*
// The config object depends on the provider
router.post('login', function (req, res, next) {
   minionClient.invoke('login', {config: 'hola'}, function (error, result, more) {
      if (error) {
         console.log('Error in the request /createstorage\n' + error);
         res.json({error: error});
      } else {
         res.json({result: result});
      }
   });
});
*/

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
      if (error) {
         console.log('Error in the request /sizes');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get the size info from de server.
 * @param {String} - The size id.
 * @return {Object} - A json Object with the follow structure: { "id":"size id", "name":"size name", "desc":"Description", "cpus":"Nº CPUs", "ram":"ram in mb"}
 */
router.get('/sizes/:size_id', function (req, res, next) {
   instmanager.getSize(req.params.size_id, function (error, result) {
      if (error) {
         console.log('Error in the request /sizes/:size_id');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
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
      if (error) {
         console.log('Error in the request /instances');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get the instance info from de server.
 * @param {String} - The instance id.
 * @return {Object} - A json Object with the follow structure: {"id":"instance id", "name":"name", "desc":"description", "image_id":"image id", "size_id":"size id"}
 */
router.get('/instances/:instance_id', function (req, res, next) {
   instmanager.getInstance(req.params.instance_id, function (error, result){
      if (error) {
         console.log('Error in the request /instances/:instance_id');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
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
      if (error) {
         console.log('Error in the request /images');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get the image infor fron de server.
 * @param {String} - The image id.
 * @return {Object} - A json Object with the follow structure: {"id":"image id", "name":"name", "desc":"description"}
 */
router.get('/images/:image_id', function (req, res, next) {
   instmanager.getImage(req.params.image_id, function (error, result) {
      if (error) {
         console.log('Error in the request /images/:image_id');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
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
      if (error) {
         console.log('Error in the GET request /applications, err: ', error);
         res.status(codes.HTTPCODE.INTERNAL_ERROR); //Internal server error
         res.json({
            'errors': [codes.ERRCODE.UNKNOWN],
            'details': error.message
         });
      }
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
      res.status(codes.HTTPCODE.BAD_REQUEST); //Bad request
      res.json({
         'errors': [codes.ERRCODE.APP_INCORRECT_PARAMS]
      });
   } else {
      // Create experiment
      scheduler.createApplication(req.body, function (error, result) {
         if (error) {
            console.log('Error in the POST creation request /applications, err: ', error);
            res.status(codes.HTTPCODE.INTERNAL_ERROR); //Internal server error
            res.json({
               'errors': [codes.ERRCODE.EXP_INCORRECT_PARAMS],
               'details': error.message
            });
         } else {
            res.json(result);
         }
      });
   }
});

/**
 * Get application metadata from the storage using its ID
 * @param {String} - The application id.
 * @return {[Object]} - A json Object with application metadata
 */
router.get('/applications/:app_id', function (req, res, next) {
   scheduler.getApplication(req.params.app_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /applications/:app_id, err: ', error);
         res.status(codes.HTTPCODE.NOT_FOUND); //Not Found
         res.json({
            'errors': [
               codes.ERRCODE.ID_NOT_FOUND,
               codes.ERRCODE.APP_NOT_FOUND
            ],
            'details': error.message
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
   res.status(codes.HTTPCODE.NOT_IMPLEMENTED); //Not Implemented
   res.json({
      'errors': [codes.ERRCODE.NOT_IMPLEMENTED]
   });
});

/**
 * Delete application
 * @param {String} - The application id.
 */
router.delete('/applications/:app_id', function (req, res, next) {
   res.status(codes.HTTPCODE.NOT_IMPLEMENTED); //Not Implemented
   res.json({
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
      if (error) {
         console.log('Error in the GET request /experiments, err: ', error);
         res.status(codes.HTTPCODE.INTERNAL_ERROR); //Internal server error
         res.json({
            'errors': [codes.ERRCODE.UNKNOWN],
            'details': error.message
         });
      }
      res.json(result);
   });
});

/**
 * Create experiment metadata
 * @param {String} - The experiment id.
 */
router.post('/experiments', function (req, res, next) {
   if (!req.body.name || !req.body.app_id) {
      res.status(codes.HTTPCODE.BAD_REQUEST); //Bad request
      res.json({
         'errors': [codes.ERRCODE.EXP_INCORRECT_PARAMS]
      });
   } else {
      // Create experiment
      scheduler.createExperiment(req.body, function (error, result) {
         if (error) {
            console.log('Error in the POST creation request /experiments, err: ', error);
            res.status(codes.HTTPCODE.INTERNAL_ERROR); //Internal server error
            res.json({
               'errors': [codes.ERRCODE.EXP_INCORRECT_PARAMS],
               'details': error.message
            });
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
            'json': codes.ERRCODE.EXP_NOT_FOUND
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
      'src_tree': req.exp.src_tree
   });
});

/**
 * Get experiment logs
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment logs
 */
router.get('/experiments/:exp_id/logs', function (req, res, next) {
   scheduler.getExperiment(req.params.exp_id, {logs: 1}, function (error, result) {
      if (error) {
         console.log('Error in the request /experiments/:exp_id/logs, err: ', error);
         return next(error);
      } else {
         res.json(result);
      }
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
         'json': codes.ERRCODE.EXP_CODE_FILE_PATH_MISSING
      });
   }

   // Get file contents
   scheduler.getExperimentCode(req.params.exp_id, fpath, function (error, fcontent) {
      if (error) {
         console.log('Error in the request /experiments/:exp_id/code, err: ', error);
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'json': codes.ERRCODE.EXP_CODE_FILE_NOT_FOUND
         });
      } else {
         res.json(fcontent);
      }
   });
});

/**
 * Get download link for experiment output data
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with output data
 */
router.get('/experiments/:exp_id/download', function (req, res, next) {
   scheduler.getExperimentOutputFile(req.params.exp_id, function (error, file) {
      if (error) {
         console.log('Error in the request /experiments/:exp_id/download, err: ', error);
         return next({
            'http': codes.HTTPCODE.NOT_FOUND,
            'json': codes.ERRCODE.EXP_NO_OUTPUT_DATA
         });
      } else {
         // Create header with file info
         var stat = fs.statSync(file);
         res.writeHead(200, {
            'Content-Type': 'application/octet-stream',
            'Content-Length': stat.size,
            'Content-Disposition': 'inline; filename="output.tar.gz"'
         });

         // Send file
         var readStream = fs.createReadStream(file);
         console.log("Sending: ", file);
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
      if (error) {
         console.log('Error in the PUT update request /experiments/:exp_id, err: ', error);
         return next(error);
      } else {
         res.json(result);
      }
   });
});

/**
 * Delete experiment
 * @param {String} - The experiment id.
 */
router.delete('/experiments/:exp_id', function (req, res, next) {
   // Remove experiment
   scheduler.destroyExperiment(req.params.exp_id, function(error){
      if(error){
         return next(error);
      } else {
         res.json(null);
      }
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
         'json': codes.ERRCODE.EXP_NO_OPERATION
      });
   } else if (req.body.op == "launch"){
      if (!req.body.nodes || !req.body.image_id || !req.body.size_id) {
         return next({
            'http': codes.HTTPCODE.BAD_REQUEST,
            'json': codes.ERRCODE.LAUNCH_INCORRECT_PARAMS
         });
      } else {
         // Launch experiment
         scheduler.launchExperiment(req.params.exp_id, req.body.nodes, req.body.image_id, req.body.size_id, function(error){
            if(error){
               return next(error);
            } else {
               res.json(null);
            }
         });
      }
   } else if (req.body.op == "reset") {
      // Reset experiment
      scheduler.resetExperiment(req.params.exp_id, function(error){
         if(error){
            return next(error);
         } else {
            res.json(null);
         }
      });
   } else {
      // Unknown operation
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'json': codes.ERRCODE.EXP_UNKNOWN_OPERATION
      });
   }
});

/**
 * Generic error handler
 */
function errorGeneric(error, req, res, next){
   if(error.json){
      res.status(error.http);
      res.json(error.json);
   } else {
      res.status(codes.HTTPCODE.INTERNAL_ERROR); //What happened?
      res.json({
         'errors': [
            codes.ERRCODE.UNKNOWN
         ],
         'details': error.message
      });
   }
}
router.use(errorGeneric);

module.exports = router;
