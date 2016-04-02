'use strict';

const assert = require('assert'),
      async  = require('async'),
      log    = require('winston'),
      db     = require('../larvitdb.js'),
      fs     = require('fs');

// Set up winston
log.remove(log.transports.Console);
log.add(log.transports.Console, {
	'level':     'warn',
	'colorize':  true,
	'timestamp': true,
	'json':      false
});

before(function(done) {
	let confFile;

	function checkEmptyDb() {
		db.query('SHOW TABLES', function(err, rows) {
			if (err) {
				log.error(err);
				assert( ! err, 'err should be negative');
				process.exit(1);
			}

			if (rows.length) {
				log.error('Database is not empty. To make a test, you must supply an empty database!');
				assert.deepEqual(rows.length, 0);
				process.exit(1);
			}

			done();
		});
	}

	function runDbSetup(confFile) {
		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		db.setup(require(confFile), function(err) {
			assert( ! err, 'err should be negative');

			checkEmptyDb();
		});
	}

	if (process.argv[3] === undefined)
		confFile = __dirname + '/../config/db_test.json';
	else
		confFile = process.argv[3].split('=')[1];

	log.verbose('DB config file: "' + confFile + '"');

	fs.stat(confFile, function(err) {
		const altConfFile = __dirname + '../config/' + confFile;

		if (err) {
			log.info('Failed to find config file "' + confFile + '", retrying with "' + altConfFile + '"');

			fs.stat(altConfFile, function(err) {
				if (err)
					assert( ! err, 'fs.stat failed: ' + err.message);

				if ( ! err)
					runDbSetup(altConfFile);
			});
		} else {
			runDbSetup(confFile);
		}
	});
});

describe('Db tests', function() {
	it('Create a table', function(done) {
		db.query('CREATE TABLE `fjant` (`test` int NOT NULL) ENGINE=\'InnoDB\';', function(err) {
			assert( ! err, 'err should be negative');
			done();
		});
	});

	it('Insert into table', function(done) {
		db.query('INSERT INTO `fjant` VALUES(13);', function(err) {
			assert( ! err, 'err should be negative');
			done();
		});
	});

	it('Select from table', function(done) {
		db.query('SELECT test FROM fjant', function(err, rows) {
			assert( ! err, 'err should be negative');
			assert.deepEqual(rows.length, 1);
			assert.deepEqual(rows[0].test, 13);
			done();
		});
	});

	it('Update table', function(done) {
		db.query('UPDATE fjant SET test = 7', function(err) {
			assert( ! err, 'err should be negative');
			db.query('SELECT test FROM fjant', function(err, rows) {
				assert( ! err, 'err should be negative');
				assert.deepEqual(rows.length, 1);
				assert.deepEqual(rows[0].test, 7);
				done();
			});
		});
	});

	it('Delete from table', function(done) {
		db.query('DELETE FROM fjant', function(err) {
			assert( ! err, 'err should be negative');
			db.query('SELECT test FROM fjant', function(err, rows) {
				assert( ! err, 'err should be negative');
				assert.deepEqual(rows.length, 0);
				done();
			});
		});
	});

	it('Drop table', function(done) {
		db.query('DROP TABLE fjant', function(err) {
			assert( ! err, 'err should be negative');
			done();
		});
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
				assert( ! err, 'err should be negative');
				assert.deepEqual(rows.length, 0);
				cb();
			});
		});

		async.series(tasks, done);
	});
});