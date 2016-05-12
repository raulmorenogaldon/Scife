var async = require('async');
var constants = require('./constants.json');
var utils = require('./utils.js');
var database = require('./database.js');
var EventEmitter = require('events').EventEmitter;

/**
 * Module name
 */
var MODULE_NAME = "TK";


/**
 * Module vars
 */
var ee = new EventEmitter();
var tasks = [];

/**
 * TASK
 * {
 *    type:
 *    otherdata:
 *    ...:
 * }
 */

var setTaskHandler = function(type, handler){
   // Add listener
   ee.on(type, handler);
}

var pushTask = function(task){
   // TODO: Check task validity

   // Create UUID
   task.id = utils.generateUUID();

   // Set status
   task._status = "waiting";

   // Add to database
   task._id = task.id;
   database.db.collection('tasks').insert(task, function(error){
      if(error) throw new Error('Failed to add task to DB: '+task);
   });

   // Add to tasks list
   tasks.push(task);

   return task.id;
}

var setTaskFailed = function(task_id, error){
   // Iterate tasks
   for(var i = 0; i < tasks.length; i++){
      var task = tasks[i];
      if(task.id == task_id){
         // Change status
         task._status = "failed";
         task.details = error;
         // Update DB
         database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status, details:task.details}});
      }
   }
}

var setTaskDone = function(task_id){
   // Iterate tasks
   for(var i = 0; i < tasks.length; i++){
      var task = tasks[i];
      if(task.id == task_id){
         // Change status
         task._status = "done";
         // Update DB
         database.db.collection('tasks').remove({id: task.id});
      }
   }
}

/**
 * Tasks launching
 */
var _launchTasks = function(){
   // Iterate tasks
   for(var i = 0; i < tasks.length; i++){
      var task = tasks[i];
      if(task._status == "waiting"){
         // Launch task
         task._status = "running";
         // Update DB
         database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status}});
         // Emit event
         ee.emit(task.type, task);
      }
   }
}

/**
 * Load tasks from DB
 */
var _loadTasks = function(){
   // TODO: Event?
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_loadTasks, 1000);
   }

   // Get tasks from DB
   database.db.collection('tasks').find().toArray(function(error, list){
      console.log('['+MODULE_NAME+'] Loaded tasks');
      // Relaunch running tasks
      for(var i = 0; i < list.length; i++){
         var task = list[i];
         if(task._status == 'running'){
            // Change status
            task._status = "waiting";
            // Update DB
            database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status}});
         }
      }
      // Set tasks as array
      tasks = list;
   });
}

/**
 * Initialize
 */
_loadTasks();
setInterval(_launchTasks, 2000);

module.exports.pushTask = pushTask;
module.exports.setTaskHandler = setTaskHandler;
module.exports.setTaskDone = setTaskDone;
module.exports.setTaskFailed = setTaskFailed;
