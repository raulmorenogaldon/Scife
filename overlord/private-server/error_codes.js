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

   // Application
   APP_NOT_FOUND: {
      'message': "Requested application has not been found!",
      'code': 30
   },
   APP_INCORRECT_PARAMS: {
      'message': "You must pass 'name', 'creation_script', 'execution_script' and 'path'.",
      'code': 31
   },

   // Experiment
   EXP_NOT_FOUND: {
      'message': "Requested experiments has not been found!",
      'code': 40
   },
   EXP_INCORRECT_PARAMS: {
      'message': "You must pass 'name' and 'app_id'.",
      'code': 41
   },
   EXP_MALFORMED_LABELS: {
      'message': "Error parsing labels JSON",
      'code': 42
   },
   LAUNCH_INCORRECT_PARAMS: {
      'message': "You must pass 'nodes', 'image_id' and 'size_id'.",
      'code': 43
   },
   EXP_NO_OPERATION: {
      'message': "You must pass 'op' argument. Possible: launch, reset.",
      'code': 44
   },
   EXP_UNKNOWN_OPERATION: {
      'message': "Unknown operation, possible operations: [launch, reset].",
      'code': 45
   },
   EXP_NO_OUTPUT_DATA: {
      'message': "Output data does not exist for this experiment.",
      'code': 46
   },
}

exports.HTTPCODE = HTTPCODE;
exports.ERRCODE = ERRCODE;
