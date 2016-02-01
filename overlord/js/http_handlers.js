// This file contains the logic for the requested operations.
// For example, localhost:8888/create will execute the "create" function in the end.
// Querystring is a package for REST parsing.

var querystring = require("querystring");
var cloud = require("./cloud");

function create(response, postData) {
    console.log("Creating cluster...");

    // Set cluster details
    var cluster_details = {
        prefix: "Test",
        nodes: 1,
        image: "Centos 7",
        flavor: "m1.small", 
    };

    // Create cluster
    cloud.createCluster(cluster_details); 

    // Response
    response.writeHead(200, {"Content-Type": "text/html"});
    response.write(body);
    response.end();
}

function root(response, postData) {
    console.log("Root page, POST: " + postData);

    // Response
    response.writeHead(200, {"Content-Type": "text/html"});
    response.write("Root, POST: " + querystring.parse(postData)["text"]);
    response.end();
}

// Export public functions
exports.root = root;
exports.create = create;
