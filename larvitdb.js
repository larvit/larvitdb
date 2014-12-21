'use strict';

var path    = require('path'),
    appPath = path.dirname(require.main.filename),
    mysql   = require('mysql'),
    dbConf  = require(appPath + '/config/db.json'),
    log     = require('winston'),
    pool    = mysql.createPool(dbConf);

// Expose getConnection()
exports.getConnection = pool.getConnection;

// Wrap the query function to log database errors
exports.query = function query(sql, dbFields, callback) {
	if (typeof(dbFields) == 'function') {
		callback = dbFields;

		pool.query(sql, function(err, rows, rowFields) {
			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql    = sql;
				err.fields = dbFields;
				log.error(err);
				callback(err);
				return;
			}

			callback(null, rows, rowFields);
		});
	} else {
		pool.query(sql, dbFields, function(err, rows, rowFields) {
			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql    = sql;
				err.fields = dbFields;
				log.error(err);
				callback(err);
				return;
			}

			callback(null, rows, rowFields);
		});
	}
}