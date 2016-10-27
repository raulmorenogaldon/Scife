var express = require('express');
var router = express.Router();
var jwt = require('jsonwebtoken');
var codes = require('../error_codes.js');

/**
 * Login
 */
router.post('/', function (req, res, next) {
   // Get authentication info
   var login_info = req.body;

   // Check errors
   if(!login_info || !login_info.username || !login_info.password){
      return next({
         'http': codes.HTTPCODE.BAD_REQUEST,
         'errors': [codes.ERRCODE.AUTH_REQ_MALFORMED]
      });
   }

   //TODO: Check credentials in DB
   //...

   // Authorized, create token (expires in 24 hours)
   var token = jwt.sign({user_id: login_info.username, admin: true}, app.get('constants').SECRET, {
      expiresIn: 86400
   });

   // Return authorization token
   return res.json({token: token});
});

module.exports = router;
