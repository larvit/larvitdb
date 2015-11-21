'use strict';

var mysql = require('mysql'),
    log   = require('winston'),
    conf;

exports.setup = function(thisConf, callback) {
	conf         = thisConf;
	exports.pool = mysql.createPool(conf);

	// Default to 3 retries on recoverable errors
	if (conf.retries === undefined) {
		conf.retries = 3;
	}

	// Default to not setting any recoverable errors
	if (conf.recoverableErrors === undefined) {
		conf.recoverableErrors = [];
	}

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
exports.query = function query(sql, dbFields, retryNr, callback) {
	var err;

	if (typeof retryNr === 'function') {
		callback = retryNr;
		retryNr  = 0;
	}

	if (typeof dbFields === 'function') {
		callback = dbFields;
		dbFields = [];
		retryNr  = 0;
	}

	if (typeof callback !== 'function') {
		callback = function(){};
	}

	if (exports.pool === undefined) {
		err = new Error('larvitdb: No pool configured. setup() must be ran with config parameters to configure a pool. sql: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
		log.error(err.message);
		callback(err);
		return;
	}

	// Log SELECTs as debug, but all others as verbose (since that mostly is INSERT, REPLACE etc that will aler the database)
	if (sql.substring(0, 6).toLowerCase() === 'select') {
		log.debug('larvitdb: Running SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields));
	} else {
		log.verbose('larvitdb: Running SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields));
	}

	exports.pool.query(sql, dbFields, function(err, rows, rowFields) {
		// We log and handle plain database errors in a unified matter
		if (err) {
			err.sql    = sql;
			err.fields = dbFields;

			// If this is a coverable error, simply try again.
			if (conf.recoverableErrors.indexOf(err.code) !== - 1) {
				retryNr ++;
				if (retryNr <= conf.retries) {
					log.warn('larvitdb: Retrying database recoverable error: ' + err.message + ' retryNr: ' + retryNr + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
					setTimeout(function() {
						exports.query(sql, dbFields, retryNr, callback);
					}, 50);
					return;
				}

				log.error('larvitdb: Exhausted retries (' + retrnyNr + ') for database recoverable error: ' + err.message + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
				callback(err);
				return;
			}

			log.error('larvitdb: Database error: ' + err.message + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
			callback(err);
			return;
		}

		callback(null, rows, rowFields);
	});
};