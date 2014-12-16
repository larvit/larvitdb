'use strict';

var path    = require('path'),
    appPath = path.dirname(require.main.filename),
    mysql   = require('mysql'),
    dbConf  = require(appPath + '/config/db.json');

exports = module.exports = mysql.createPool(dbConf);
/* Example of usage:
var db = require('larvitdb');

db.query('SELECT 1 + 1 AS solution', function(err, rows, fields) {
	console.log('dbmodel: The solution is: ', rows[0].solution);
});

Or, if a connection is needed:

db.getConnection(function(err, dbCon) {

	var sql = 'SELECT * FROM users WHERE username LIKE ' +
	    	dbCon.escape(postData);

	dbCon.query(sql, function(err, rows) {
		dbCon.release(); // Always release your connection when the query is done

		if (err)
			throw err;


You dont need to get a connection to escape though. You can do like this:

db.query('SELECT * FROM users WHERE id = ?', [userId], function(err, results) {
  // ...
});

*/