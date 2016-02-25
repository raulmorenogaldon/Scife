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

router.get('/', function (req, res, next) {
  res.json({return: 'Ha solicitado el path: ' + req.baseUrl});
});

/**
 * Get image list from the server
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
 * Get a available sizes list from the server
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
 * Get a instance list from the server
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
 * Return the imagen whose id is passed in the url get method
 */
router.get('/image/:idImage', function (req, res, next) {
  minionClient.invoke('getInstances', function (error, result, more) {
    if (error) {
      console.log('Error in the request /image/:idImage');
      res.json({error: error});
    }
    res.json(result);
  });
});

router.post('/createstorage', function (req, res, next) {
  console.log(req.body);
  if (!req.body.path || !req.body.publicUrl || !req.body.userName) {
    console.log('Error in /createstorage');
    res.json({error: 'path, public url and username are required'});
  } else {
    storageClient.invoke('Storage', req.body.path, req.body.publicUrl, req.body.userName, function (error, result, more) {
      if (error) {
        console.log('Error in the request /createstorage\n' + error);
        res.json({error: error});
      } else {
        res.json({result: result});
      }
    });
  }
});

/*
router.get('/login', function(req, res, next){
	minionClient.invoke("login", {name:"hola"}, function(error, result, more){
		if(error){
			console.log("Error in the request /createstorage\n"+error)
			res.json({error:error})
		}else{
			res.json({result:result})
		}
	})
})
*/

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

/**
 * Return a Json object named result with a list of sizes 
 */
router.get('/sizes', function (req, res, next) {
  minionClient.invoke('getSizes', function (error, result, more) {
    if (error) {
      console.log('Error in the request /sizes\n' + error);
      res.json({error: error});
    } else {
      res.json({result: result});
    }
  });
});

/**
 * This method allow to create a new size and assign an UUID. When the size is created this function returns and result object with the Id of the size.
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

module.exports = router;
