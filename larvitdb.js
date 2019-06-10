'use strict';

const	topLogPrefix	= 'larvitdb: larvitdb.js: ',
	events	= require('events'),
	eventEmitter	= new events.EventEmitter(),
	LUtils	= require('larvitutils'),
	lUtils	= new LUtils(),
	async	= require('async'),
	mysql	= require('mysql2');

let	dbSetup	= false,
	conf;

eventEmitter.setMaxListeners(50); // There is no problem with a lot of listeneres on this one

// Wrap getConnection to log errors
function getConnection(cb) {
	const	logPrefix	= topLogPrefix + 'getConnection() - ';

	exports.pool.getConnection(function (err, dbCon) {
		if (err) {
			exports.log.error(logPrefix + 'Could not get connection, err: ' + err.message);
			return cb(err);
		}

		dbCon.org_beginTransaction	= dbCon.beginTransaction;
		dbCon.org_commit	= dbCon.commit;
		dbCon.org_query	= dbCon.query;
		dbCon.org_rollback	= dbCon.rollback;

		dbCon.beginTransaction = function beginTransaction(cb) {
			const	subLogPrefix	= logPrefix + 'beginTransaction() - ';

			if (typeof cb !== 'function') cb = function () {};

			dbCon.org_beginTransaction(function (err) {
				if (err) {
					exports.log.error(subLogPrefix + err.message);
					return cb(err);
				}

				cb(err);
			});
		};

		dbCon.commit = function commit(cb) {
			const	subLogPrefix	= logPrefix + 'commit() - ';

			if (typeof cb !== 'function') cb = function () {};

			dbCon.org_commit(function (err) {
				if (err) {
					exports.log.error(subLogPrefix + err.message);
					return cb(err);
				}

				cb(err);
			});
		};

		dbCon.query = function query(sql, dbFields, cb) {
			const	subLogPrefix	= logPrefix + 'query() - ';

			let	startTime;

			if (typeof dbFields === 'function') {
				cb	= dbFields;
				dbFields	= [];
			}

			if ( ! dbFields) {
				dbFields	= [];
			}

			if (typeof cb !== 'function') cb = function () {};

			startTime	= process.hrtime();

			dbCon.org_query(sql, dbFields, function (err, rows) {
				const	queryTime	= lUtils.hrtimeToMs(startTime, 4);

				exports.log.debug(subLogPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');

				if (err) {
					exports.log.error(subLogPrefix + err.message, ', SQL: "' + err.sql + '"');
				}

				cb(err, rows);
			});
		};

		dbCon.rollback = function rollback(cb) {
			const	subLogPrefix	= logPrefix + 'rollback() - ';

			if (typeof cb !== 'function') cb = function () {};

			dbCon.org_rollback(function (err) {
				if (err) {
					exports.log.error(subLogPrefix + err.message);
					return cb(err);
				}

				cb(err);
			});
		};

		cb(err, dbCon);
	});
}

// Wrap the query function to log database errors or slow running queries
function query(sql, dbFields, options, cb) {
	const	logPrefix	= topLogPrefix + 'query() - ';

	ready(function () {
		let startTime;

		if (typeof options === 'function') {
			cb	= options;
			options	= {};
		}

		if (typeof dbFields === 'function') {
			cb	= dbFields;
			dbFields	= [];
			options	= {};
		}

		if (typeof cb !== 'function') {
			cb	= function () {};
			options	= {};
		}

		if (options.retryNr	=== undefined) { options.retryNr	= 0;	}
		if (options.ignoreLongQueryWarning	=== undefined) { options.ignoreLongQueryWarning	= true;	}

		if (exports.pool === undefined) {
			const	err	= new Error('No pool configured. sql: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
			exports.log.error(logPrefix + err.message);
			return cb(err);
		}

		// Convert datetimes to UTC string
		if (Array.isArray(dbFields)) {
			for (let i = 0; i < dbFields.length; i ++) {
				if (dbFields[i] instanceof Date) {
					let	dbField	= dbFields[i].toISOString();
					dbFields[i]	= (dbField.substring(0, 10) + ' ' + dbField.slice(11)).substring(0, 19); // Replace T with ' ' and cut the last Z off
				}
			}
		}

		startTime	= process.hrtime();

		exports.pool.query(sql, dbFields, function (err, rows, rowFields) {
			const	queryTime	= lUtils.hrtimeToMs(startTime, 4);

			if (conf.longQueryTime !== false && conf.longQueryTime < queryTime && options.ignoreLongQueryWarning !== true) {
				exports.log.warn(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
			} else {
				exports.log.debug(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
			}

			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql	= sql;
				err.fields	= dbFields;

				// If this is a coverable error, simply try again.
				if (conf.recoverableErrors.indexOf(err.code) !== - 1) {
					options.retryNr = options.retryNr + 1;
					if (options.retryNr <= conf.retries) {
						exports.log.warn(logPrefix + 'Retrying database recoverable error: ' + err.message + ' retryNr: ' + options.retryNr + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
						setTimeout(function () {
							query(sql, dbFields, {'retryNr': options.retryNr}, cb);
						}, 50);
						return;
					}

					exports.log.error(logPrefix + 'Exhausted retries (' + options.retryNr + ') for database recoverable error: ' + err.message + ' SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));
					return cb(err);
				}

				exports.log.error(logPrefix + 'Database error msg: ' + err.message + ', code: "' + err.code + '" SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));
				return cb(err);
			}

			cb(null, rows, rowFields);
		});
	});
};

function ready(cb) {
	if (dbSetup) return cb();

	eventEmitter.once('checked', cb);
};

function removeAllTables(cb) {
	const	logPrefix	= topLogPrefix + 'removeAllTables() - ';

	ready(function () {
		exports.pool.getConnection(function (err, con) {
			const	tables	= [],
				tasks	= [];

			if (err) {
				exports.log.error(logPrefix + 'Could not get a connection from the pool: ' + err.message);
				return cb(err);
			}

			// Disalbe foreign key checks to be able to remove tables in any order
			tasks.push(function (cb) {
				con.query('SET FOREIGN_KEY_CHECKS=0;', cb);
			});

			// Gather table names
			tasks.push(function (cb) {
				con.query('SHOW TABLES', function (err, rows) {
					if (err) {
						exports.log.error(logPrefix + 'Error when running "SHOW TABLES": ' + err.message);
						return cb(err);
					}

					for (let i = 0; rows[i] !== undefined; i ++) {
						tables.push(rows[i]['Tables_in_' + exports.conf.database]);
					}

					cb();
				});
			});

			// Actually remove tables
			tasks.push(function (cb) {
				const sqlTasks = [];

				for (let i = 0; tables[i] !== undefined; i ++) {
					let tableName = tables[i];

					sqlTasks.push(function (cb) {
						con.query('DROP TABLE `' + tableName + '`;', cb);
					});
				}

				async.parallel(sqlTasks, cb);
			});

			// Set foreign key checks back to normal
			tasks.push(function (cb) {
				con.query('SET FOREIGN_KEY_CHECKS=1;', cb);
			});

			tasks.push(function (cb) {
				con.release();
				cb();
			});

			async.series(tasks, cb);
		});
	});
}

function setup(thisConf, cb) {
	const	validDbOptions	= [],
		logPrefix	= topLogPrefix + 'setup() - ',
		dbConf	= {},
		tasks	= [];

	validDbOptions.push('host');
	validDbOptions.push('port');
	validDbOptions.push('localAddress');
	validDbOptions.push('socketPath');
	validDbOptions.push('user');
	validDbOptions.push('password');
	validDbOptions.push('database');
	validDbOptions.push('charset');
	validDbOptions.push('timezone');
	validDbOptions.push('connectTimeout');
	validDbOptions.push('stringifyObjects');
	validDbOptions.push('insecureAuth');
	validDbOptions.push('typeCast');
	validDbOptions.push('queryFormat');
	validDbOptions.push('supportBigNumbers');
	validDbOptions.push('bigNumberStrings');
	validDbOptions.push('dateStrings');
	validDbOptions.push('debug');
	validDbOptions.push('trace');
	validDbOptions.push('multipleStatements');
	validDbOptions.push('flags');
	validDbOptions.push('ssl');

	// Valid for pools
	validDbOptions.push('waitForConnections');
	validDbOptions.push('connectionLimit');
	validDbOptions.push('queueLimit');

	exports.conf	= conf	= thisConf;

	if ( ! exports.conf.log) {
		exports.conf.log	= new lUtils.Log();
	}

	exports.log	= exports.conf.log;

	for (const key of Object.keys(conf)) {
		if (validDbOptions.indexOf(key) !== - 1) {
			dbConf[key]	= conf[key];
		}
	}

	function tryToConnect(cb) {
		const	dbCon	= mysql.createConnection(dbConf);

		dbCon.connect(function (err) {
			if (err) {
				const	retryIntervalSeconds	= 1;

				exports.log.warn(logPrefix + 'Could not connect to database, retrying in ' + retryIntervalSeconds + ' seconds');
				return setTimeout(function () {
					tryToConnect(cb);
				}, retryIntervalSeconds * 1000);
			}

			dbCon.destroy();
			cb();
		});
	}

	// Wait for database to become available
	tasks.push(tryToConnect);

	// Start pool and check connection
	tasks.push(function (cb) {
		try {
			exports.pool	= mysql.createPool(dbConf); // Expose pool
		} catch (err) {
			exports.log.error(logPrefix + 'Throwed error from database driver: ' + err.message);
			return cb(err);
		}

		// Default to 3 retries on recoverable errors
		if (conf.retries === undefined) {
			conf.retries	= 3;
		}

		// Default to setting recoverable errors to lost connection
		if (conf.recoverableErrors === undefined) {
			conf.recoverableErrors	= ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'];
		}

		// Default slow running queries to 10 seconds
		if (conf.longQueryTime === undefined) {
			conf.longQueryTime	= 10000;
		}

		// Set timezone
		exports.pool.on('connection', function (connection) {
			connection.query('SET time_zone = \'+00:00\';');
		});

		// Make connection test to database
		exports.pool.query('SELECT 1', function (err, rows) {
			if (err || rows.length === 0) {
				exports.log.error(logPrefix + 'Database connection test failed!');
			} else {
				exports.log.info(logPrefix + 'Database connection test succeeded.');
			}

			dbSetup	= true;
			eventEmitter.emit('checked');

			if (typeof cb === 'function') {
				cb(err);
			}
		});
	});

	async.series(tasks, cb);
};

exports.getConnection	= getConnection;
exports.query	= query;
exports.ready	= ready;
exports.removeAllTables	= removeAllTables;
exports.setup	= setup;
exports.log	= new lUtils.Log(); // Default to simple console log before anything else happends
