var ssh2 = require('ssh2').Client;
var winston = require('winston');

/**
 * Logger configuration
 */
winston.level = 'debug';

// Create SSH connection
function connectSSH(username, host, private_key, timeout, connectCallback){
   // Create connection object
   var conn = new ssh2();

   // Timeout
   var time_elapsed = 0;

   // Define connection callback
   conn.on('ready', function(){
      // Connected
      connectCallback(null, conn);
   });

   // Retry on error in connection callback
   conn.on('error', function(error){
      if(time_elapsed < timeout){
         setTimeout(function(){
            // Increate elapsed time
            time_elapsed = time_elapsed + 5000;
            // Retry SSH connection
            conn.connect({
               host: host,
               port: 22,
               username: username,
               privateKey: private_key
            });
         }, 5000);
         return;
      }
      // No more retries
      connectCallback(error);
   });

   // SSH connect
   conn.connect({
      host: host,
      port: 22,
      username: username,
      privateKey: private_key
   });
};

// Execute a command
function execSSH(conn, cmd, work_dir, blocking, execCallback){
   // Check connection
   if(!conn || !conn.exec) return execCallback(new Error("Invalid connection object."));

   // Full command var
   var full_cmd = null;

   // If no work_dir is specified, use $HOME
   if(!work_dir) work_dir = "~";

   // If the command is blocking, then wait until command execution
   if(blocking){
      // Change to dir and execute
      full_cmd = ". ~/.bash_profile; cd "+work_dir+"; "+cmd+";"
   } else {
      // Create a background process
      full_cmd = "nohup sh -c '. ~/.bash_profile; cd "+work_dir+"; "+cmd+"' > /dev/null 2>&1 & echo -n $!;"
   }

   // Output object with normal and error outputs.
   var output = {
      stdout: "",
      stderr: ""
   }

   // Execute command
   conn.exec(full_cmd, function(error, stream){
      if(error) return execCallback(error);

      // Handle received data
      stream.on('close', function(code, signal){
         // Command executed, return output
         conn.end();
         execCallback(null, output);
      }).on('data', function(data) {
         output.stdout = output.stdout + data;
      }).stderr.on('data', function(data){
         output.stderr = output.stderr + data;
      });
   });
};

// Close a connection
function closeSSH(conn){
   if(conn && conn.end) conn.end();
};

// Generate uuid
function generateUUID(){
   var d = new Date().getTime();
   var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = (d + Math.random()*16)%16 | 0;
      d = Math.floor(d/16);
      return (c=='x' ? r : (r&0x3|0x8)).toString(16);
   });
   return uuid;
};

// Cut tree in depth
function cutTree(tree, fpath, depth){
   // Search path
   if(fpath){
      // Search this folder
      var stack = [];
      var cur_node = {
         id: "",
         children: tree
      }
      stack.push(cur_node);

      // Iterate tree
      var found = false;
      while(stack.length > 0){
         // Get next child
         if(cur_node.children.length > 0){
            stack.push(cur_node);
            cur_node = cur_node.children.pop()
            if(cur_node.id == fpath){
               // Found!
               tree = cur_node.children;
               found = true;
               break;
            }
         } else {
            cur_node = stack.pop();
         }
      }
      if(!found) return null;
   }

   // Get depth
   if(depth && depth >= 0){
      // Cut depth function
      var __treeDepth = function(node, depth){
         if(depth == 0){
            node.children = [];
         } else {
            for(var i = 0; i < node.children.length; i++){
               __treeDepth(node.children[i], depth - 1);
            }
         }
      }
      // Cut depth
      for(var i = 0; i < tree.length; i++) __treeDepth(tree[i], depth);
   }

   return tree;
}

module.exports.connectSSH = connectSSH;
module.exports.execSSH = execSSH;
module.exports.closeSSH = closeSSH;
module.exports.generateUUID = generateUUID;
module.exports.cutTree = cutTree;
module.exports.logger = winston;
