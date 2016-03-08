var HTTPCODE = {
   OK: 200,
   BAD_REQUEST: 400,
   NOT_FOUND: 404,
   INTERNAL_ERROR: 500,
   NOT_IMPLEMENTED: 501
}

var ERRCODE = {
   // General error
   UNKNOWN: {
      'message': "Internal error",
      'code': 1
   },

   // Development
   NOT_IMPLEMENTED: {
      'message': "Not Implemented",
      'code': 10
   },

   // ID errors
   ID_NOT_FOUND: {
      'message': "Requested ID has not been found!",
      'code': 20
   },

   // Experiment
   EXP_NOT_FOUND: {
      'message': "Requested experiments has not been found!",
      'code': 30
   },
   EXP_INCORRECT_PARAMS: {
      'message': "You must pass 'name' and 'app_id'.",
      'code': 31
   },
   LAUNCH_INCORRECT_PARAMS: {
      'message': "You must pass 'nodes', 'image_id' and 'size_id'.",
      'code': 32
   },
}

exports.HTTPCODE = HTTPCODE;
exports.ERRCODE = ERRCODE;
