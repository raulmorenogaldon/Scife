var express = require('express'),
   router = express.Router(),
   zerorpc = require('zerorpc'),
   constants = require('../constants.json');

var minionClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});
var storageClient = new zerorpc.Client({
	heartbeatInterval: 30000,
	timeout: 3600
});

console.log('Conecting to minion-url: ' + constants.MINION_URL +
            '\nConecting to storage-url: ' + constants.STORAGE_URL);

minionClient.connect(constants.MINION_URL);
storageClient.connect(constants.STORAGE_URL);

/***********************************************************
 * --------------------------------------------------------
 * LOGIN METHODS
 * --------------------------------------------------------
 ***********************************************************/

// The config object depends on the provider
router.get('/login', function(req, res, next){
   minionClient.invoke("login", {}, function(error, result, more){
      if(error){
         console.log("Error in the request /login");
         res.status(500); //Internal server error
         res.json(error);
      }else{
         res.json(result);
      }
   });
});

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
   minionClient.invoke('getSizes', function (error, result, more) {
      if (error) {
         console.log('Error in the request /sizes');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get the size info fron de server.
 * @param {String} - The size id.
 * @return {Object} - A json Object with the follow structure: { "id":"size id", "name":"size name", "desc":"Description", "cpus":"Nº CPUs", "ram":"ram in mb"}
 */
router.get('/sizes/:size_id', function (req, res, next) {
   minionClient.invoke('findSize',req.params.size_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /sizes');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * This method allow to create a new size. When the size is created this function returns and result object with the Id of the size.
 * @param {Object} {{"name":"name", "desc":"Description", "dpus":"Nº CPUs", "ram":"ram in mb"}}
 * @return {Object} - A json object with the follow structure: {"result":"id of the size"}
 */
router.post('/createsize', function (req, res, next) {
   if (!req.body.name || !req.body.desc || !req.body.cpus || !req.body.ram) {
      console.log(req.body);
      res.status(400); //Bad request
      res.json('Error, you must pass the name, description, cpus and ram params');
   } else {
      minionClient.invoke('createSize', {
         name: req.body.name,
         desc: req.body.desc,
         cpus: req.body.cpus,
         ram: req.body.ram
      },
      function (error, result, more) {
         if (error) {
            console.log('Error in the request /createsize\n' + error);
            res.status(500); //Internal server error
            res.json(error);
         } else {
            res.json(result);
         }
      });
   }
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
   minionClient.invoke('getInstances', function (error, result, more) {
      if (error) {
         console.log('Error in the request /instances');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get the instance info fron de server.
 * @param {String} - The instance id.
 * @return {Object} - A json Object with the follow structure: {"id":"instance id", "name":"name", "desc":"description", "image_id":"image id", "size_id":"size id"}
 */
router.get('/instances/:instance_id', function (req, res, next) {
   minionClient.invoke('findInstance',req.params.image_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /instances');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});


/**
 * This method allow to create a new instance. When the size is created this function returns and result object with the Id of the instance.
 * @param {Object} {{"name":"name", "desc":"Description", "imageId":"image id", "sizeId":"size id"}}
 * @return {Object} - A json object with the follow structure: {"result":"id of the instance created"}
 */
router.post('/createinstance', function (req, res, next) {
   if (!req.body.name || !req.body.desc || !req.body.imageId || !req.body.sizeId) {
      res.status(400); //Bad request
      res.json({error: 'Error, you must pass the name, description, image id and size id params'});
   } else {
      minionClient.invoke('createInstance', {
         name: req.body.name,
         desc: req.body.desc,
         image_id: req.body.imageId,
         size_id: req.body.sizeId
      },
      function (error, result, more) {
         if (error) {
            console.log('Error in the request /createinstance\n' + error);
            res.json(error);
         } else {
            res.json(result);
         }
      });
   }
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
   minionClient.invoke('getImages', function (error, result, more) {
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
   minionClient.invoke('findImage', req.params.image_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /images');
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
   storageClient.invoke('getApplications', function (error, result, more) {
      if (error) {
         console.log('Error in the request /applications');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get application metadata from the storage using its ID
 * @param {String} - The application id.
 * @return {[Object]} - A json Object with application metadata
 */
router.get('/applications/:app_id', function (req, res, next) {
   storageClient.invoke('findApplication', req.params.app_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /applications');
         res.status(404); //Not Found
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * This method allow to create a new application.
 * @param {Object} - A json Object with application metadata
 * @return {[Object]} - A json Object with application ID
 */
router.post('/createapplication', function (req, res, next) {
   if (!req.body.name || !req.body.desc || !req.body.path || !req.body.creation_script || !req.body.execution_script) {
      res.status(400); //Bad request
      res.json({error: 'Error, you must pass the name, description, input app folder, creation and execution scripts.'});
   } else {
      console.log("Creating application: ", req.body);
      storageClient.invoke('createApplication', req.body, function (error, result, more) {
         if (error) {
            console.log('Error in the request /createapplication\n' + error);
            res.status(500); //Internal server error
            res.json(error);
         } else {
            res.json(result);
         }
      });
   }
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
   storageClient.invoke('getExperiments', function (error, result, more) {
      if (error) {
         console.log('Error in the request /experiments');
         res.status(500); //Internal server error
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * Get experiment metadata from the storage using its ID
 * @param {String} - The experiment id.
 * @return {[Object]} - A json Object with experiment metadata
 */
router.get('/experiments/:exp_id', function (req, res, next) {
   storageClient.invoke('findExperiment', req.params.exp_id, function (error, result, more) {
      if (error) {
         console.log('Error in the request /experiments');
         res.status(404); //Not Found
         res.json(error);
      }
      res.json(result);
   });
});

/**
 * This method allow to create a new experiment.
 * @param {Object} - A json Object with experiment metadata
 * @return {[Object]} - A json Object with experiment ID
 */
router.post('/createexperiment', function (req, res, next) {
   if (!req.body.name || !req.body.desc || !req.body.app_id || !req.body.labels || !req.body.exec_env) {
      res.status(400); //Bad request
      res.json({error: 'Error, you must pass the name, description, application id, a JSON of labels and a JSON of execution variables.'});
   } else {
      storageClient.invoke('createExperiment', req.body, function (error, result, more) {
         if (error) {
            console.log('Error in the request /createexperiment\n' + error);
            res.status(500); //Internal server error
            res.json(error);
         } else {
            res.json(result);
         }
      });
   }
});

module.exports = router;
