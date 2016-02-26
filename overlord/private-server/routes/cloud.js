var express = require('express'),
  router = express.Router(),
  zerorpc = require('zerorpc'),
  constants = require('../constants.json');

var minionClient = new zerorpc.Client(),
  storageClient = new zerorpc.Client();

console.log('Conecting to minion-url: ' + constants.MINION_URL +
  '\nConecting to storage-url: ' + constants.STORAGE_URL);

minionClient.connect(constants.MINION_URL);
storageClient.connect(constants.STORAGE_URL);



router.get('/login', function(req, res, next){
	minionClient.invoke("login", {name:"hola"}, function(error, result, more){
		if(error){
			console.log("Error in the request /createstorage\n"+error);
			res.json({error:error});
		}else{
			res.json({result:result});
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


/**
 * Return a list with the sizes in the server.
 * @return {[Object]} - A json object list with the follow strucutre:
 * 	[{ "id":"size id", "name":"size name", "desc":"Description", "cpus":"Nº CPUs", "ram":"ram in mb"}]
 */
router.get('/sizes', function (req, res, next) {
  minionClient.invoke('getSizes', function (error, result, more) {
    if (error) {
      console.log('Error in the request /sizes');
      res.json({error: error});
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
      res.json({error: error});
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
    res.json({error: 'Error, you must pass the name, description, cpus and ram params'});
  } else {
    minionClient.invoke('createSize',
      {name: req.body.name,
        desc: req.body.desc,
        cpus: req.body.cpus,
      ram: req.body.ram},
      function (error, result, more) {
        if (error) {
          console.log('Error in the request /createsize\n' + error);
          res.json({error: error});
        } else {
          res.json({result: result});
        }
      });
  }
});


/**
 * Get a instance list from the server.
 * @return {[Obect]} - A json object list with the follow strucutre:
 * 	[{ "id":"instance id", "name":"name", "desc":"Description", "image_id":"image id", "size_id":"size id"}]
 */
router.get('/instances', function (req, res, next) {
  minionClient.invoke('getInstances', function (error, result, more) {
    if (error) {
      console.log('Error in the request /instances');
      res.json({error: error});
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
      console.log('Error in the request /images');
      res.json({error: error});
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
    res.json({error: 'Error, you must pass the name, description, image id and size id params'});
  } else {
    minionClient.invoke('createInstance',
      {name: req.body.name,
        desc: req.body.desc,
        image_id: req.body.imageId,
      size_id: req.body.sizeId},
      function (error, result, more) {
        if (error) {
          console.log('Error in the request /createinstance\n' + error);
          res.json({error: error});
        } else {
          res.json({result: result});
        }
      });
  }
});

/**
 * Get image list from the server
 * @return {[Object]} - A  list of json objects with she follow strucutre: [{"id":"image id", "name":"name", "desc":"description"}]
 */
router.get('/images', function (req, res, next) {
  minionClient.invoke('getImages', function (error, result, more) {
    if (error) {
      console.log('Error in the request /images');
      res.json({error: error});
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
      res.json({error: error});
    }
    res.json(result);
  });
});


module.exports = router;
