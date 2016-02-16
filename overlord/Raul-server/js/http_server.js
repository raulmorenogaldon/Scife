// This file initialices the base web server.
// It creates a listener in the specified port and waits for requests.
// When a request is made, the data is passed to the router.

var http = require("http");
var url = require("url");

function init(route, handle, port) {
    // Request callback
    function onRequest(request, response) {
        var postData = "";
        var pathname = url.parse(request.url).pathname;
        console.log("Received request: " + pathname);

        // UTF8 encoding is used
        request.setEncoding("utf8");

        // Save complete POST data
        request.addListener("data", function(chunk) {
            postData += chunk;
        });

        // Route request
        request.addListener("end", function(){
            route(handle, pathname, response, postData);
        });
    }

    // Create server
    http.createServer(onRequest).listen(port);
    console.log("Server initialized, listening in port",port);
}

// Export public functions
exports.init = init;
