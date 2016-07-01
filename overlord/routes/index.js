var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function (req, res, next) {
  // res.setHeader('Content-Type', 'application/json')
  // res.send(JSON.stringify({ return: 'Esto lo devuelve el servidor interno' }))
  res.json({ return: 'Esto lo devuelve el servidor interno' });
});

router.get('/suma', function (req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  res.send(JSON.stringify({ resultado: req.body.num1 + req.body.num2 }));
});

module.exports = router;
