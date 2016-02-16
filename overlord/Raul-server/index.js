// Main file for the web server
// This source defines post operations and its handlers
// A handler process a request

var server = require("./js/http_server");
var router = require("./js/http_router");
var requestHandlers = require("./js/http_handlers");

var PORT = 8888;

// Handlers for petitions
var handle = {}
handle["/"] = requestHandlers.root;
handle["/create"] = requestHandlers.create;

// Initiate server
server.init(router.route, handle, PORT);
