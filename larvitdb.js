'use strict';

var mysql = require('mysql'),
    log   = require('winston'),
    conf,
    pool;

exports.setup = function(thisConf) {
	conf = thisConf;
	pool = mysql.createPool(conf);

	// Expose getConnection()
	exports.getConnection = pool.getConnection;
};

// Wrap the query function to log database errors
exports.query = function query(sql, dbFields, callback) {
	var err;

	if (pool === undefined) {
		err = new Error('larvitdb: No pool configured. setup() must be ran with config parameters to configure a pool.');
		log.error(err.message);
		callback(err);
		return;
	}

	if (typeof dbFields === 'function') {
		callback = dbFields;

		pool.query(sql, function(err, rows, rowFields) {
			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql    = sql;
				log.error(err.message, err);
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
				log.error(err.message, err);
				callback(err);
				return;
			}

			callback(null, rows, rowFields);
		});
	}
};