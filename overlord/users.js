/**
 * Copyright 2017 University of Castilla - La Mancha
 */
var zerorpc = require('zerorpc');
var async = require('async');

/**
 * Load submodules
 */
var utils = require('./utils.js');
var logger = utils.logger;
var apps = require('./application.js');
var database = require('./database.js');
var storage = require('./storage.js');

/**
 * Module name
 */
var MODULE_NAME = "US";

/**
 * Module vars
 */
var cfg = process.argv[2];

/**
 * Create user and add to DB
 */
var createUser = function(username, password, admin, createCallback){
   // Check parameters
   if(!username) return createCallback(new Error("'username' not provided"));
   if(!password) return createCallback(new Error("'password' not provided"));

   // Check if user already exists
   database.db.collection('users').findOne({username: username}, function(error, user){
      if(user) return createCallback(new Error('User "'+username+'" already exists'));

      // Create user metadata
      var id = utils.generateUUID();
      var user = {
         '_id': id,
         'id': id,
         'username': username,
         'password': password,
         'admin': (admin ? true : false),
         'permissions': {
            'applications': [],
            'images': []
         }
      }

      // Insert into DB
      database.db.collection('users').insert(user, function(error){
         if(error) return createCallback(error);
         // Success adding user
         logger.info('['+MODULE_NAME+']['+user.id+'] User created.');
         createCallback(null, {
            'id': user.id,
            'username': user.username,
            'admin': user.admin
         });
      });
   });
}

/**
 * Get user metadata
 */
var getUser = function(user_id, getCallback){
   // Connected to DB?
   if(database.db == null){
      searchCallback(new Error("Not connected to DB"));
      return;
   }

   // Check parameters
   if(!user_id) return setCallback(new Error("'user_id' not provided"));

   // Search user in DB
   database.db.collection('users').findOne({$or: [{_id: user_id}, {username: user_id}]}, function(error, user){
      if(error || !user) return getCallback(new Error("User '" + user_id + "' does not exist"));
      getCallback(null, user);
   });
}

/**
 * Search users
 */
var searchUsers = function(name, searchCallback){
   // Connected to DB?
   if(database.db == null){
      searchCallback(new Error("Not connected to DB"));
      return;
   }

   // Set query
   var query;
   if(!name){
      query = ".*";
   } else {
      query = ".*"+name+".*";
   }

   // Projection
   var fields = {
      _id: 0,
      id: 1,
      username: 1,
      admin: 1
   };

   // Retrieve users metadata
   database.db.collection('users').find({username: {$regex: query}}, fields).toArray(function(error, users){
      if(error) return searchCallback(new Error("Query for users with name: " + name + " failed"));
      searchCallback(null, users);
   });
}

/**
 * Set or unset user permissions
 * @param {String} - The user id.
 * @param {String} - Permission identifier (i.e. 'applications')
 * @param {String} - Value to add/remove (i.e. application ID), null means all when deny
 * @param {Boolean} - Add or remove
 */
var setUserPermissions = function(user_id, permission, value, allow, setCallback){
   // Check parameters
   if(!user_id) return setCallback(new Error('"user_id" not provided'));
   if(!permission) return setCallback(new Error('"permission" not provided'));
   if(allow && !value) return setCallback(new Error('Setting permissions but no "value" has been provided'));

   // Get user
   getUser(user_id, function(error, user){
      if(error) return setCallback(error);

      // Aux
      var query = {};
      var key = 'permissions.'+permission;

      // Cases
      if(allow){
         // Allow value
         query['$addToSet'] = {};
         query['$addToSet'][key] = value;
      } else if (!value){
         // Deny all
         query['$set'] = {};
         query['$set'][key] = [];
      } else {
         // Deny value
         query['$pull'] = {};
         query['$pull'][key] = value;
      }

      // Update permissions
      database.db.collection('users').updateOne({_id: user_id}, query, function(error){
         if(error) return setCallback(error);

         // Logging
         if(allow) logger.info('['+MODULE_NAME+']['+user_id+'] Value "'+value+'" has been added to permission "'+key+'".');
         else if(!value) logger.info('['+MODULE_NAME+']['+user_id+'] Permission "'+key+'" cleaned.');
         else logger.info('['+MODULE_NAME+']['+user_id+'] Value "'+value+'" has been removed from permission "'+key+'".');

         return setCallback(null);
      });
   });
}

exports.createUser = createUser;
exports.getUser = getUser;
exports.searchUsers = searchUsers;
exports.setUserPermissions = setUserPermissions;
