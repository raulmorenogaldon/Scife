var express = require('express'),
  morgan = require('morgan'),
  path = require('path'),
  cookieParser = require('cookie-parser'),
  bodyParser = require('body-parser');

var http = require('http');
var fs = require('fs');

var codes = require('./error_codes.js');
var logger = require('./utils.js').logger;

var routerLogin = require('./routes/login'),
  routerCloud = require('./routes/cloud');

// Global express var
app = express();

/**
 * Config file
 */
var cfg = process.argv[2];
if(!cfg) throw new Error('No CFG file has been provided.');
var constants = JSON.parse(fs.readFileSync(cfg));

// Set constants
app.set('constants', constants);

// uncomment after placing your favicon in /public
// app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')))
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));


/**
 * Add field text when text/plain
 */
app.use(function(req, res, next){
   if (req.is('text/*')) {
      req.text = '';
      req.setEncoding('utf8');
      req.on('data', function(chunk){ req.text += chunk  });
      req.on('end', next);
   } else {
      next();
   }
});

// Handle login
app.use('/login', routerLogin);

// Protected routes
app.use('/', routerCloud);

/**
 * No route error handling
 */
app.use(function (req, res, next) {
   res.status(codes.HTTPCODE.NOT_FOUND); //The route do not exists
   res.json({
      'errors': [{
         code: codes.ERRCODE.INEXISTENT_METHOD.code,
         message: codes.ERRCODE.INEXISTENT_METHOD.message + " Requested: " + req.method + " " + req.url
      }]
   });
});

/**
 * Generic error handler
 */
function errorGeneric(error, req, res, next){
   if(error.errors){
      res.status(error.http);
      res.json({'errors': error.errors});
   } else {
      logger.error('INTERNAL ERROR: '+error.message);
      res.status(codes.HTTPCODE.INTERNAL_ERROR); //What happened?
      res.json({
         'errors': [{
            code: codes.ERRCODE.UNKNOWN.code,
            message: error.message
         }]
      });
   }
}
app.use(errorGeneric);

/**
 * REST SERVER
 */
var server = http.createServer(app);
var port = constants.OVERLORD_LISTEN_PORT;

// Listen
server.listen(port);

// Event handling
server.on('error', onError);
server.on('listening', onListening);

/**
 * Event listener for HTTP server "error" event.
 */
function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  // handle specific listen errors with friendly messages
  var bind = typeof port === 'string' ? 'Pipe ' + port : 'Port ' + port;
  switch (error.code) {
    case 'EACCES':
      logger.error(bind + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      logger.error(bind + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

/**
 * Event listener for HTTP server "listening" event.
 */

function onListening() {
  var addr = server.address();
  var bind = typeof addr === 'string' ? 'pipe ' + addr : 'port ' + addr.port;
  logger.info("Listening on port "+port);
}

module.exports = app;
