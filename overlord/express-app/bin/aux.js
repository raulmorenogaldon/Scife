/**
 *This module implements auxiliar functions
 */
module.exports = {
    /**
     * Return the string "html" or "json" that appearing firs in the string passed as parameter. If you pass a string with don't contain "html" neither "json" words, the function returns null.
     */
    getOrderedAccept: function(req) {

        var html = req.indexOf('html') > -1 ? req.indexOf('html') : null;
        var json = req.indexOf('json') > -1 ? req.indexOf('json') : null;

        if (html != null && json != null) {
            if (html < json) {
                return 'html';
            } else {
                return 'json';
            }
        } else if (html != null) {
            return 'html';
        } else if (json != null) {
            return 'json'
        }

        return null;
    }
}
