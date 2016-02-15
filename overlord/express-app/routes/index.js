var express = require('express');
var router= express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
	if(req.get('accept').indexOf('json') > -1){
		res.setHeader('Content-Type', 'application/json');
		res.send(JSON.stringify({return: 'Ha solicitado un objeto json'}));
	}else{
		res.render('index/index');
	}
});

module.exports = router;
