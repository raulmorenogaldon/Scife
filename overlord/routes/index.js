var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function (req, res, next) {
  // res.setHeader('Content-Type', 'application/json')
  // res.send(JSON.stringify({ return: 'Esto lo devuelve el servidor interno' }))
  // res.json({ return: 'Esto lo devuelve el servidor interno' });
   next();
});

module.exports = router;
