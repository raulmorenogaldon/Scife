// This file contains the logic for the request routing.
// When a request is passed here, the router calls the corresponding handler for this request.
// If no handler is defined for this request, 404 is sent.

function route(handle, pathname, response, postData) {
    console.log("Routing: "+pathname);
    if (typeof handle[pathname] === 'function') {
        handle[pathname](response, postData);
    } else {
        console.log("No handle for " + pathname);
        response.writeHead(404, {"Content-Type": "text/html"});
        response.write("404 Not Found");
        response.end();
    }
}

// Export public functions
exports.route = route;
