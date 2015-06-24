'use strict';

var mysql = require('mysql'),
    log   = require('winston'),
    conf;

exports.setup = function(thisConf, callback) {
	conf         = thisConf;
	exports.pool = mysql.createPool(conf);

	// Expose getConnection()
	exports.getConnection = exports.pool.getConnection;

	// Make connection test to database
	exports.query('SELECT 1', function(err, rows) {
		if (err || rows.length === 0) {
			log.error('larvitdb: setup() - Database connection test failed!');
		} else {
			log.info('larvitdb: setup() - Database connection test succeeded.');
		}

		if (typeof callback === 'function') {
			callback(err);
		}
	});
};

// Wrap the query function to log database errors
exports.query = function query(sql, dbFields, callback) {
	var err;

	if (typeof callback !== 'function') {
		callback = function(){};
	}

	if (exports.pool === undefined) {
		err = new Error('larvitdb: No pool configured. setup() must be ran with config parameters to configure a pool.');
		log.error(err.message);
		callback(err);
		return;
	}

	if (typeof dbFields === 'function') {
		callback = dbFields;

		log.debug('larvitdb: Running SQL: "' + sql + '"');

		exports.pool.query(sql, function(err, rows, rowFields) {
			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql = sql;
				log.error(err.message, err);
				callback(err);
				return;
			}

			callback(null, rows, rowFields);
		});
	} else {
		log.debug('larvitdb: Running SQL: "' + sql + '" with dbFields = ' + JSON.stringify(dbFields));

		exports.pool.query(sql, dbFields, function(err, rows, rowFields) {
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