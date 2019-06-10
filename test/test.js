'use strict';

const	assert	= require('assert'),
	LUtils	= require('larvitutils'),
	lUtils	= new LUtils(),
	async	= require('async'),
	log	= new lUtils.Log('warn'),
	db	= require('../larvitdb.js'),
	fs	= require('fs');

before(function (done) {
	let	confFile;

	function checkEmptyDb() {
		db.query('SHOW TABLES', function (err, rows) {
			if (err)	throw err;
			if (rows.length) {
				// Remove tables
				const dropTableQuery = 'DROP TABLE ' + rows.map(x => x.Tables_in_test).join(',');

				db.query(dropTableQuery, done);
			} else {
				done();
			}
		});
	}

	function runDbSetup(confFile) {
		let	conf;

		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		conf	= require(confFile);
		conf.log	= log;

		db.setup(conf, function (err) {
			if (err) throw err;

			checkEmptyDb();
		});
	}

	if (process.env.CONFFILE === undefined) {
		confFile	= __dirname + '/../config/db_test.json';
	} else {
		confFile	= process.env.CONFFILE;
	}

	log.verbose('DB config file: "' + confFile + '"');

	fs.stat(confFile, function (err) {
		const	altConfFile	= __dirname + '/../config/' + confFile;

		if (err) {
			log.info('Failed to find config file "' + confFile + '", retrying with "' + altConfFile + '"');

			fs.stat(altConfFile, function (err) {
				if (err) throw err;

				runDbSetup(altConfFile);
			});
		} else {
			runDbSetup(confFile);
		}
	});
});

after(function (done) {
	db.pool.end(done);
});

describe('Db tests', function () {
	function dbTests(dbCon, cb) {
		const	tasks	= [];

		tasks.push(function (cb) {
			dbCon.query('CREATE TABLE `fjant` (`test` int NOT NULL) ENGINE=\'InnoDB\';', cb);
		});

		tasks.push(function (cb) {
			dbCon.query('INSERT INTO `fjant` VALUES(13);', cb);
		});

		tasks.push(function (cb) {
			dbCon.query('SELECT test FROM fjant', function (err, rows) {
				if (err) throw err;
				assert.strictEqual(rows.length,	1);
				assert.strictEqual(rows[0].test,	13);
				cb(err);
			});
		});

		tasks.push(function (cb) {
			dbCon.query('UPDATE fjant SET test = ?', [7], function (err) {
				if (err) throw err;
				dbCon.query('SELECT test FROM fjant', function (err, rows) {
					if (err) throw err;
					assert.strictEqual(rows.length,	1);
					assert.strictEqual(rows[0].test,	7);
					cb(err);
				});
			});
		});

		tasks.push(function (cb) {
			dbCon.query('DELETE FROM fjant', function (err) {
				if (err) throw err;
				dbCon.query('SELECT test FROM fjant', function (err, rows) {
					if (err) throw err;
					assert.strictEqual(rows.length,	0);
					cb(err);
				});
			});
		});

		tasks.push(function (cb) {
			dbCon.query('DROP TABLE fjant', cb);
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			cb();
		});
	}

	it('Simple queries', function (done) {
		dbTests(db, done);
	});

	it('Queries on a single connection from the pool', function (done) {
		db.getConnection(function (err, dbCon) {
			if (err) throw err;
			dbTests(dbCon, done);
		});
	});

	it('Transactions', function (done) {
		const	tasks	= [];

		tasks.push(function (cb) {
			db.query('CREATE TABLE `foobar` (`baz` int);', cb);
		});

		tasks.push(function (cb) {
			db.query('INSERT INTO foobar VALUES(5)', cb);
		});

		// Successfull transaction
		tasks.push(function (cb) {
			db.getConnection(function (err, dbCon) {
				if (err) throw err;

				dbCon.beginTransaction(function (err) {
					if (err) throw err;

					dbCon.query('INSERT INTO foobar VALUES(5)', function (err) {
						if (err) throw err;

						// If an error occurred in the queries, in a production environment
						// a rollback should be performed...

						dbCon.commit(function (err) {
							if (err) throw err;

							cb(err);
						});
					});
				});
			});
		});

		// Rolled back transaction
		tasks.push(function (cb) {
			db.getConnection(function (err, dbCon) {
				if (err) throw err;

				dbCon.beginTransaction(function (err) {
					if (err) throw err;

					dbCon.query('INSERT INTO foobar VALUES(5)', function (err) {
						if (err) throw err;

						dbCon.rollback(function (err) {
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
		tasks.push(function (cb) {
			db.query('SELECT SUM(baz) AS bazSum FROM foobar', function (err, rows) {
				if (err) throw err;
				assert.strictEqual(Number(rows[0].bazSum),	10);
				cb(err);
			});
		});

		tasks.push(function (cb) {
			db.query('DROP TABLE foobar', cb);
		});

		async.series(tasks, done);
	});

	it('Remove all tables from database', function (done) {
		const tasks = [];

		// Create tables with internal relations
		tasks.push(function (cb) {
			db.query(`CREATE TABLE foo (
				id int(11) NOT NULL AUTO_INCREMENT,
				name int(11) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`, cb);
		});

		tasks.push(function (cb) {
			db.query(`CREATE TABLE bar (
					fooId int(11) NOT NULL,
					stuff varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
					KEY fooId (fooId),
					CONSTRAINT bar_ibfk_1 FOREIGN KEY (fooId) REFERENCES foo (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`, cb);
		});

		// Try to remove all tables from the database
		tasks.push(function (cb) {
			db.removeAllTables(cb);
		});

		// Check so no tables exists
		tasks.push(function (cb) {
			db.query('SHOW TABLES', function (err, rows) {
				if (err) throw err;
				assert.strictEqual(rows.length,	0);
				cb();
			});
		});

		async.series(tasks, done);
	});

	it('Stream rows', function (done) {
		const	tasks	= [];

		let	rowNr	= 0,
			dbCon;

		// Create table with data
		tasks.push(function (cb) {
			db.query('CREATE TABLE `plutt` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` varchar(191) NOT NULL);', cb);
		});
		tasks.push(function (cb) {
			db.query('INSERT INTO plutt (name) VALUES(\'bosse\'),(\'hasse\'),(\'vråkbert\');', cb);
		});

		// Get dbCon
		tasks.push(function (cb) {
			db.pool.getConnection(function (err, result) {
				dbCon	= result;
				cb(err);
			});
		});

		// Check contents
		tasks.push(function (cb) {
			const	query	= dbCon.query('SELECT * FROM plutt');

			query.on('error', function (err) {
				throw err;
			});

			query.on('fields', function (fields) {
				assert.strictEqual(fields[0].name,	'id');
				assert.strictEqual(fields[1].name,	'name');
			});

			query.on('result', function (row) {
				dbCon.pause(); // Pause streaming while handling row

				rowNr ++;

				if (rowNr === 1) {
					assert.strictEqual(row.id,	1);
					assert.strictEqual(row.name,	'bosse');
				} else if (rowNr === 2) {
					assert.strictEqual(row.id,	2);
					assert.strictEqual(row.name,	'hasse');
				} else if (rowNr === 2) {
					assert.strictEqual(row.id,	3);
					assert.strictEqual(row.name,	'vråkbert');
				}

				dbCon.resume(); // Resume streaming when processing of row is done (this is normally done in async, doh)
			});

			query.on('end', function () {
				assert.strictEqual(rowNr,	3);
				dbCon.release();
				cb();
			});
		});

		// Clear database
		tasks.push(function (cb) {
			db.removeAllTables(cb);
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			done();
		});
	});

	it('Time zone dependent data', function (done) {
		const	tasks	= [],
			dates	= [];

		// Create table
		tasks.push(function (cb) {
			const	sql	= 'CREATE TABLE tzstuff (id int(11), text varchar(191), tzstamp timestamp, tzdatetime DATETIME);';

			db.query(sql, cb);
		});

		// Set datetime as javascript Date object UTC
		tasks.push(function (cb) {
			dates.push(new Date('2018-05-04T17:00:00Z'));

			db.query('INSERT INTO tzstuff VALUES(?,?,?,?);', [1, '', dates[dates.length - 1], dates[dates.length - 1]], cb);
		});

		// Set date as javascript Date object
		tasks.push(function (cb) {
			dates.push(new Date('2018-05-04 17:00:00'));

			db.query('INSERT INTO tzstuff VALUES(?,?,?,?);', [2, null, dates[dates.length - 1], dates[dates.length - 1]], cb);
		});

		// Set date as string
		tasks.push(function (cb) {
			dates.push('2018-05-04 17:00:00');

			db.query('INSERT INTO tzstuff VALUES(?,?,?,?);', [3, undefined, dates[dates.length - 1], dates[dates.length - 1]], cb);
		});

		// Check the values
		tasks.push(function (cb) {
			db.query('SELECT * FROM tzstuff ORDER BY id', function (err, rows) {

				if (err) throw err;

				for (let i = 0; rows[i] !== undefined; i ++) {
					const	row	= rows[i],
						date = dates[i] instanceof Date ? dates[i].toISOString().replace('T', ' ').replace('Z', '').substring(0, 19) : dates[i];

					if (row.tzstamp instanceof Date) {
						err = new Error('dateStrings is not set to true in db config');
						throw err;
					}

					assert.equal(row.tzstamp, date);
					assert.equal(row.tzdatetime, date);
				}

				assert.strictEqual(rows.length,	dates.length);

				cb();
			});
		});

		// Remove table
		tasks.push(function (cb) {
			db.query('DROP TABLE tzstuff;', cb);
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			done();
		});
	});
});
