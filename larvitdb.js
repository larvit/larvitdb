'use strict';

var events       = require('events'),
    eventEmitter = new events.EventEmitter(),
    dbSetup      = false,
    mysql        = require('mysql'),
    log          = require('winston'),
    conf;

function ready(cb) {
	if (dbSetup) {
		cb();
		return;
	}

	eventEmitter.on('checked', cb);
};

function setup(thisConf, cb) {
	exports.conf = conf = thisConf;
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
	exports.pool.query('SELECT 1', function(err, rows) {
		if (err || rows.length === 0) {
			log.error('larvitdb: setup() - Database connection test failed!');
		} else {
			log.info('larvitdb: setup() - Database connection test succeeded.');
		}

		dbSetup = true;
		eventEmitter.emit('checked');

		if (typeof cb === 'function') {
			cb(err);
		}
	});
};

// Wrap the query function to log database errors
function query(sql, dbFields, retryNr, cb) {
	ready(function() {
		var err;

		if (typeof retryNr === 'function') {
			cb = retryNr;
			retryNr  = 0;
		}

		if (typeof dbFields === 'function') {
			cb = dbFields;
			dbFields = [];
			retryNr  = 0;
		}

		if (typeof cb !== 'function') {
			cb = function(){};
		}

		if (exports.pool === undefined) {
			err = new Error('larvitdb: No pool configured. sql: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
			log.error(err.message);
			cb(err);
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
							query(sql, dbFields, retryNr, cb);
						}, 50);
						return;
					}

					log.error('larvitdb: Exhausted retries (' + retrnyNr + ') for database recoverable error: ' + err.message + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
					cb(err);
					return;
				}

				log.error('larvitdb: Database error: ' + err.message + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
				cb(err);
				return;
			}

			cb(null, rows, rowFields);
		});
	});
};

exports.ready = ready;
exports.setup = setup;
exports.query = query;