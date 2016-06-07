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
var hashQueue = {};
var hashTasks = {};

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

var pushTask = function(task, queue){
   // TODO: Check task validity

   // Create UUID
   task.id = utils.generateUUID();

   // Set status and queue
   task._status = "waiting";
   task._queue = queue;

   // Add to database
   task._id = task.id;
   database.db.collection('tasks').insert(task, function(error){
      if(error) throw new Error('Failed to add task to DB: '+task);
   });

   // Add shortcut
   hashTasks[task.id] = task;

   // Specified queue?
   if(queue){
      if(!hashQueue[queue]){
         // Create queue
         hashQueue[queue] = [];
      }
      // Add to task queue
      hashQueue[queue].push(task);
   } else {
      // Add to general queue
      tasks.push(task);
   }

   return task.id;
}

var setTaskFailed = function(task_id, error){
   // Get tasks
   var task = hashTasks[task_id];

   if(task){
      // Check if not aborted
      if(task._status != "aborted"){
         // Change status
         task._status = "failed";
         task.details = error;
         // Update DB
         database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status, details:task.details}});
      } else {
         // Aborted
         if(task._abortcb) task._abortcb();
         // Update DB
         database.db.collection('tasks').remove({id: task.id});
      }
   }
}

var setTaskDone = function(task_id, next_task, queue){
   // Get task
   var task = hashTasks[task_id];

   if(task){
      // Check if not aborted
      if(task._status != "aborted"){
         // Change status
         task._status = "done";
         // Update DB
         database.db.collection('tasks').remove({id: task.id});

         // Add next task to queue
         if(next_task) pushTask(next_task, queue);
      } else {
         // Aborted
         if(task._abortcb) task._abortcb();
         // Update DB
         database.db.collection('tasks').remove({id: task.id});
      }
   }
}

var getTaskQueue = function(queue){
   return hashQueue[queue];
}

/**
 * Abort all tasks for this queue
 */
var abortQueue = function(queue, abortCallback){
   // Iterate tasks in this queue
   if(hashQueue[queue]){
      for(var i = 0; i < hashQueue[queue].length; i++){
         var task = hashQueue[queue][i];

         // Change status and abort callback
         task._status = "aborted";
         task._abortcb = abortCallback;

         // Update DB
         database.db.collection('tasks').remove({id: task.id});
      }
   }
}

/**
 * Check if task is aborted
 */
var isTaskAborted = function(task_id){
   // Get task
   var task = hashTasks[task_id];

   // Check if aborted
   return (task && task._status == "aborted");
}

/**
 * Tasks launching
 */
var _launchTasks = function(){
   // Iterate queues
   Object.keys(hashQueue).forEach(function(queue){
      var tasks = hashQueue[queue];

      // Iterate tasks in this queue
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
   });

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
         // Add to shortcuts
         hashTasks[task.id] = task;

         if(task._queue){
            // Add to queue
            if(!hashQueue[task._queue]) hashQueue[task._queue] = [];
            hashQueue[task._queue].push(task);
         } else {
            // Add to general tasks list
            tasks.push(task);
         }
      }
   });
}

/**
 * Initialize
 */
_loadTasks();
setInterval(_launchTasks, 2000);

module.exports.pushTask = pushTask;
module.exports.abortQueue = abortQueue;
module.exports.setTaskHandler = setTaskHandler;
module.exports.setTaskDone = setTaskDone;
module.exports.setTaskFailed = setTaskFailed;
module.exports.getTaskQueue = getTaskQueue;
module.exports.isTaskAborted = isTaskAborted;
