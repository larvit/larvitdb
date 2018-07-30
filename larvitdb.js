'use strict';

const	topLogPrefix	= 'larvitdb: larvitdb.js: ',
	events	= require('events'),
	eventEmitter	= new events.EventEmitter(),
	async	= require('async'),
	utils	= require('larvitutils'),
	mysql	= require('mysql2'),
	log	= require('winston');

let	dbSetup	= false,
	conf;

eventEmitter.setMaxListeners(50); // There is no problem with a lot of listeneres on this one

// Wrap getConnection to log errors
function getConnection(cb) {
	const	logPrefix	= topLogPrefix + 'getConnection() - ';

	exports.pool.getConnection(function (err, dbCon) {
		if (err) {
			log.error(logPrefix + 'Could not get connection, err: ' + err.message);
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
					log.error(subLogPrefix + err.message);
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
					log.error(subLogPrefix + err.message);
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
				const	queryTime	= utils.hrtimeToMs(startTime, 4);

				log.debug(subLogPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');

				if (err) {
					log.error(subLogPrefix + err.message, ', SQL: "' + err.sql + '"');
				}

				cb(err, rows);
			});
		};

		dbCon.rollback = function rollback(cb) {
			const	subLogPrefix	= logPrefix + 'rollback() - ';

			if (typeof cb !== 'function') cb = function () {};

			dbCon.org_rollback(function (err) {
				if (err) {
					log.error(subLogPrefix + err.message);
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
			log.error(logPrefix + err.message);
			return cb(err);
		}

		// Convert datetimes to UTC
		if (Array.isArray(dbFields)) {
			for (let i = 0; dbFields[i] !== undefined; i ++) {
				if (typeof dbFields[i] === Date) {
					const	dbField	= dbFields[i];
					dbField	= dbField.toISOString();
					dbField[10]	= ' '; // Replace T with a space
					dbField	= dbField.substring(0, dbField.length - 1);	// Cut the last Z off
				}
			}
		}

		startTime	= process.hrtime();

		exports.pool.query(sql, dbFields, function (err, rows, rowFields) {
			const	queryTime	= utils.hrtimeToMs(startTime, 4);

			if (conf.longQueryTime !== false && conf.longQueryTime < queryTime && options.ignoreLongQueryWarning !== true) {
				log.warn(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
			} else {
				log.debug(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
			}

			// We log and handle plain database errors in a unified matter
			if (err) {
				err.sql	= sql;
				err.fields	= dbFields;

				// If this is a coverable error, simply try again.
				if (conf.recoverableErrors.indexOf(err.code) !== - 1) {
					options.retryNr = options.retryNr + 1;
					if (options.retryNr <= conf.retries) {
						log.warn(logPrefix + 'Retrying database recoverable error: ' + err.message + ' retryNr: ' + options.retryNr + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
						setTimeout(function () {
							query(sql, dbFields, {'retryNr': options.retryNr}, cb);
						}, 50);
						return;
					}

					log.error(logPrefix + 'Exhausted retries (' + options.retryNr + ') for database recoverable error: ' + err.message + ' SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));
					return cb(err);
				}

				log.error(logPrefix + 'Database error msg: ' + err.message + ', code: "' + err.code + '" SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));
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
				log.error(logPrefix + 'Could not get a connection from the pool: ' + err.message);
				cb(err);
				return;
			}

			// Disalbe foreign key checks to be able to remove tables in any order
			tasks.push(function (cb) {
				con.query('SET FOREIGN_KEY_CHECKS=0;', cb);
			});

			// Gather table names
			tasks.push(function (cb) {
				con.query('SHOW TABLES', function (err, rows) {
					if (err) {
						log.error(logPrefix + 'Error when running "SHOW TABLES": ' + err.message);
						cb(err);
						return;
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
	const	logPrefix	= topLogPrefix + 'setup() - ',
		tasks	= [];

	exports.conf	= conf	= thisConf;

	function tryToConnect(cb) {
		const	dbCon	= mysql.createConnection(conf);

		dbCon.connect(function (err) {
			if (err) {
				const	retryIntervalSeconds	= 1;

				log.warn(logPrefix + 'Could not connect to database, retrying in ' + retryIntervalSeconds + ' seconds');
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
			exports.pool	= mysql.createPool(conf); // Expose pool
		} catch (err) {
			log.error(logPrefix + 'Throwed error from database driver: ' + err.message);
			cb(err);
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
				log.error(logPrefix + 'Database connection test failed!');
			} else {
				log.info(logPrefix + 'Database connection test succeeded.');
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
