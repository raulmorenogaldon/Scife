
// Generate uuid
function generateUUID() {
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

module.exports.generateUUID = generateUUID;
module.exports.cutTree = cutTree;
