'use strict';

const	assert	= require('assert'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('../larvitdb.js'),
	fs	= require('fs');

// Set up winston
log.remove(log.transports.Console);
log.add(log.transports.Console, {
	'level':	'warn',
	'colorize':	true,
	'timestamp':	true,
	'json':	false
});

before(function(done) {
	let confFile;

	function checkEmptyDb() {
		db.query('SHOW TABLES', function(err, rows) {
			if (err)	throw err;
			if (rows.length)	throw new Error('Database is not empty. To make a test, you must supply an empty database!');

			done();
		});
	}

	function runDbSetup(confFile) {
		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		db.setup(require(confFile), function(err) {
			if (err) throw err;

			checkEmptyDb();
		});
	}

	if (process.argv[3] === undefined) {
		confFile = __dirname + '/../config/db_test.json';
	} else {
		confFile = process.argv[3].split('=')[1];
	}

	log.verbose('DB config file: "' + confFile + '"');

	fs.stat(confFile, function(err) {
		const altConfFile = __dirname + '/../config/' + confFile;

		if (err) {
			log.info('Failed to find config file "' + confFile + '", retrying with "' + altConfFile + '"');

			fs.stat(altConfFile, function(err) {
				if (err) throw err;

				runDbSetup(altConfFile);
			});
		} else {
			runDbSetup(confFile);
		}
	});
});

describe('Db tests', function() {
	function dbTests(dbCon, cb) {
		const	tasks	= [];

		tasks.push(function(cb) {
			dbCon.query('CREATE TABLE `fjant` (`test` int NOT NULL) ENGINE=\'InnoDB\';', cb);
		});

		tasks.push(function(cb) {
			dbCon.query('INSERT INTO `fjant` VALUES(13);', cb);
		});

		tasks.push(function(cb) {
			dbCon.query('SELECT test FROM fjant', function(err, rows) {
				if (err) throw err;
				assert.strictEqual(rows.length,	1);
				assert.strictEqual(rows[0].test,	13);
				cb(err);
			});
		});

		tasks.push(function(cb) {
			dbCon.query('UPDATE fjant SET test = ?', [7], function(err) {
				if (err) throw err;
				dbCon.query('SELECT test FROM fjant', function(err, rows) {
					if (err) throw err;
					assert.strictEqual(rows.length,	1);
					assert.strictEqual(rows[0].test,	7);
					cb(err);
				});
			});
		});

		tasks.push(function(cb) {
			dbCon.query('DELETE FROM fjant', function(err) {
				if (err) throw err;
				dbCon.query('SELECT test FROM fjant', function(err, rows) {
					if (err) throw err;
					assert.strictEqual(rows.length,	0);
					cb(err);
				});
			});
		});

		tasks.push(function(cb) {
			dbCon.query('DROP TABLE fjant', cb);
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			cb();
		});
	}

	it('Simple queries', function(done) {
		dbTests(db, done);
	});

	it('Queries on a single connection from the pool', function(done) {
		db.getConnection(function(err, dbCon) {
			if (err) throw err;

			dbTests(dbCon, done);
		});
	});

	it('Transactions', function(done) {
		const	tasks	= [];

		tasks.push(function(cb) {
			db.query('CREATE TABLE `foobar` (`baz` int);', cb);
		});

		tasks.push(function(cb) {
			db.query('INSERT INTO foobar VALUES(5)', cb);
		});

		// Successfull transaction
		tasks.push(function(cb) {
			db.getConnection(function(err, dbCon) {
				if (err) throw err;

				dbCon.beginTransaction(function(err) {
					if (err) throw err;

					dbCon.query('INSERT INTO foobar VALUES(5)', function(err) {
						if (err) throw err;

						// If an error occurred in the queries, in a production environment
						// a rollback should be performed...

						dbCon.commit(function(err) {
							if (err) throw err;

							cb(err);
						});
					});
				});
			});
		});

		// Rolled back transaction
		tasks.push(function(cb) {
			db.getConnection(function(err, dbCon) {
				if (err) throw err;

				dbCon.beginTransaction(function(err) {
					if (err) throw err;

					dbCon.query('INSERT INTO foobar VALUES(5)', function(err) {
						if (err) throw err;

						dbCon.rollback(function(err) {
							if (err) throw err;

							cb(err);
						});
					});
				});
			});
		});

		// Since we have 1 normal insert with value of 5, then a transaction insert
		// also of 5, and then a rolled back transaction of 5, the sum should be
		// 5 + (5 - 5) + 5 = 10
		tasks.push(function(cb) {
			db.query('SELECT SUM(baz) AS bazSum FROM foobar', function(err, rows) {
				if (err) throw err;
				assert.strictEqual(Number(rows[0].bazSum),	10);
				cb(err);
			});
		});

		tasks.push(function(cb) {
			db.query('DROP TABLE foobar', cb);
		});

		async.series(tasks, done);
	});

	it('Remove all tables from database', function(done) {
		const tasks = [];

		// Create tables with internal relations
		tasks.push(function(cb) {
			db.query(`CREATE TABLE foo (
				id int(11) NOT NULL AUTO_INCREMENT,
				name int(11) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`, cb);
		});

		tasks.push(function(cb) {
			db.query(`CREATE TABLE bar (
					fooId int(11) NOT NULL,
					stuff varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
					KEY fooId (fooId),
					CONSTRAINT bar_ibfk_1 FOREIGN KEY (fooId) REFERENCES foo (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`, cb);
		});

		// Try to remove all tables from the database
		tasks.push(function(cb) {
			db.removeAllTables(cb);
		});

		// Check so no tables exists
		tasks.push(function(cb) {
			db.query('SHOW TABLES', function(err, rows) {
				if (err) throw err;
				assert.strictEqual(rows.length,	0);
				cb();
			});
		});

		async.series(tasks, done);
	});
});
