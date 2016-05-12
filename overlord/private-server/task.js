var async = require('async');
var constants = require('./constants.json');
var utils = require('./utils.js');
var database = require('./database.js');
var EventEmitter = require('events').EventEmitter;

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

   // TODO: Add to database

   // Add to tasks list
   tasks.push(task);

   return task.id;
}

var setTaskFailed = function(task_id){
   // Iterate tasks
   for(var i = 0; i < tasks.length; i++){
      var task = tasks[i];
      if(task.id == task_id){
         // Change status
         task._status = "failed";
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
         console.log("Emitting");
         ee.emit(task.type, task);
      }
   }
}

setInterval(_launchTasks, 2000);

module.exports.pushTask = pushTask;
module.exports.setTaskHandler = setTaskHandler;
module.exports.setTaskDone = setTaskDone;
module.exports.setTaskFailed = setTaskFailed;
