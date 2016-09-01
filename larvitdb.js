'use strict';

const	events	= require('events'),
	eventEmitter	= new events.EventEmitter(),
	async	= require('async'),
	utils	= require('larvitutils'),
	mysql	= require('mysql2'),
	log	= require('winston');

let	dbSetup	= false,
	conf;

// Wrap the query function to log database errors or slow running queries
function query(sql, dbFields, retryNr, cb) {
	try {
		ready(function() {
			let startTime;

			if (typeof retryNr === 'function') {
				cb	= retryNr;
				retryNr	= 0;
			}

			if (typeof dbFields === 'function') {
				cb	= dbFields;
				dbFields	= [];
				retryNr	= 0;
			}

			if (typeof cb !== 'function') {
				cb = function(){};
			}

			if (exports.pool === undefined) {
				let err = new Error('larvitdb: No pool configured. sql: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
				log.error(err.message);
				cb(err);
				return;
			}

			startTime = process.hrtime();

			exports.pool.query(sql, dbFields, function(err, rows, rowFields) {
				const queryTime = utils.hrtimeToMs(startTime, 4);

				if (conf.longQueryTime !== false && conf.longQueryTime < queryTime) {
					log.warn('larvitdb: Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
				} else {
					log.debug('larvitdb: Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
				}

				// We log and handle plain database errors in a unified matter
				if (err) {
					err.sql	= sql;
					err.fields	= dbFields;

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

						log.error('larvitdb: Exhausted retries (' + retryNr + ') for database recoverable error: ' + err.message + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
						cb(err);
						return;
					}

					log.error('larvitdb: Database error msg: ' + err.message + ', code: "' + err.code + '" SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
					cb(err);
					return;
				}

				cb(null, rows, rowFields);
			});
		});
	} catch (err) {
		log.error('larvitdb: query() - Throwed error from database driver: ' + err.message);
		cb(err);
	}
};

function ready(cb) {
	if (dbSetup) { cb(); return; }

	eventEmitter.on('checked', cb);
};

function removeAllTables(cb) {
	try {
		ready(function() {
			exports.pool.getConnection(function(err, con) {
				const	tables	= [],
					tasks	= [];

				if (err) {
					log.error('larvitdb: removeAllTables() - Could not get a connection from the pool: ' + err.message);
					cb(err);
					return;
				}

				// Disalbe foreign key checks to be able to remove tables in any order
				tasks.push(function(cb) {
					con.query('SET FOREIGN_KEY_CHECKS=0;', cb);
				});

				// Gather table names
				tasks.push(function(cb) {
					con.query('SHOW TABLES', function(err, rows) {
						if (err) {
							log.error('larvitdb: removeAllTables() - Error when running "SHOW TABLES": ' + err.message);
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
				tasks.push(function(cb) {
					const sqlTasks = [];

					for (let i = 0; tables[i] !== undefined; i ++) {
						let tableName = tables[i];

						sqlTasks.push(function(cb) {
							con.query('DROP TABLE `' + tableName + '`;', cb);
						});
					}

					async.parallel(sqlTasks, cb);
				});

				// Set foreign key checks back to normal
				tasks.push(function(cb) {
					con.query('SET FOREIGN_KEY_CHECKS=1;', cb);
				});

				tasks.push(function(cb) {
					con.release();
					cb();
				});

				async.series(tasks, cb);
			});
		});
	} catch (err) {
		log.error('larvitdb: removeAllTables() - Throwed error from database driver: ' + err.message);
		cb(err);
	}
}

function setup(thisConf, cb) {
	try {
		exports.conf = conf = thisConf;
		exports.pool = mysql.createPool(conf); // Expose pool

		// Default to 3 retries on recoverable errors
		if (conf.retries === undefined) {
			conf.retries = 3;
		}

		// Default to setting recoverable errors to lost connection
		if (conf.recoverableErrors === undefined) {
			conf.recoverableErrors = ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'];
		}

		// Default slow running queries to 10 seconds
		if (conf.longQueryTime === undefined) {
			conf.longQueryTime = 10000;
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
	} catch (err) {
		log.error('larvitdb: setup() - Throwed error from database driver: ' + err.message);
		cb(err);
	}
};

exports.query	= query;
exports.ready	= ready;
exports.removeAllTables	= removeAllTables;
exports.setup	= setup;
