var async = require('async');
var constants = require('./constants.json');
var utils = require('./utils.js');
var logger = utils.logger;
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

/**
 * Set handler for a task.
 * i. e. event callback function
 */
var setTaskHandler = function(type, handler){
   // Add listener
   ee.on(type, handler);
}

/**
 * Add a task to a queue.
 * If queue is null, default queue will be used.
 */
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

/**
 * Signal task as failed.
 */
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
         logger.error('['+MODULE_NAME+'] Task "'+task.type+'" failed: '+error+'. Stack: '+error.stack);
      } else {
         // Aborted
         if(task._abortcb) task._abortcb(null, task);
      }
   }
}

/**
 * Signal task as successful.
 */
var setTaskDone = function(task_id, next_task, queue){
   // Get task
   var task = hashTasks[task_id];

   if(task){
      // Check if not aborted
      if(task._status != "aborted"){
         // Change status
         task._status = "done";
         // Update DB
         database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status}});
         // Add next task to queue
         if(next_task) pushTask(next_task, queue);
      } else {
         // Aborted
         if(task._abortcb) task._abortcb(null, task);
      }
   }
}

/**
 * Return a task array from a queue
 */
var getTaskQueue = function(queue){
   return hashQueue[queue];
}

/**
 * Abort all tasks for this queue
 * abortFunc will be called for every task in the queue.
 * abortCallback will be called when all tasks finish their execution.
 */
var abortQueue = function(queue, abortTaskFunc, abortCallback){
   var tasks = [];

   // Iterate tasks in this queue
   if(hashQueue[queue]){
      // Iterate queue
      var i = hashQueue[queue].length;
      while(i--){
         var task = hashQueue[queue][i];

         // Remove from queue
         hashQueue[queue].splice(i, 1);

         // Only stop running tasks
         if(task._status == "running"){
            (function(task){
               tasks.push(function(taskcb){
                  // Change status
                  task._status = "aborted";
                  // When task finished execution, callback
                  task._abortcb = taskcb;
                  // Update DB
                  database.db.collection('tasks').updateOne({id: task.id},{$set:{_status:task._status}});
                  // Call abort function if not null
                  if(abortTaskFunc) abortTaskFunc(task);
               });
            })(task);
         }
      }
   }

   // Execute tasks
   // All aborted and finished
   logger.info('['+MODULE_NAME+'] Aborting queue "'+queue+'", '+tasks.length+' running tasks');
   async.parallel(tasks, function(error, task){
      if(error){return abortCallback(error);}
      abortCallback(null);
   });
}

/**
 * Return true if task has been aborted.
 */
var isTaskAborted = function(task_id){
   // Get task
   var task = hashTasks[task_id];

   // Check if aborted
   return (task && task._status == "aborted");
}

/******************************************************
 *
 * Private functions
 *
 *****************************************************/
/**
 * Launch all waiting tasks.
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
            logger.info('['+MODULE_NAME+'] Task "'+task.type+'" emmited.');
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
 * Load tasks from DB.
 */
var _loadTasks = function(){
   // TODO: Event?
   // Wait for database to connect
   if(!database.db){
      return setTimeout(_loadTasks, 1000);
   }

   // Get tasks from DB
   database.db.collection('tasks').find().toArray(function(error, list){
      logger.log('['+MODULE_NAME+'] Loaded tasks');
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

/******************************************************
 *
 * Module initialization
 *
 *****************************************************/
// Load previous tasks from DB
_loadTasks();
// Call _launchTasks every 2 seconds
setInterval(_launchTasks, 2000);

/******************************************************
 *
 * Public interface
 *
 *****************************************************/
module.exports.pushTask = pushTask;
module.exports.abortQueue = abortQueue;
module.exports.setTaskHandler = setTaskHandler;
module.exports.setTaskDone = setTaskDone;
module.exports.setTaskFailed = setTaskFailed;
module.exports.getTaskQueue = getTaskQueue;
module.exports.isTaskAborted = isTaskAborted;
