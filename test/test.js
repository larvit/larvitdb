'use strict';

var assert = require('assert'),
    log    = require('winston'),
    db     = require('../larvitdb.js'),
    fs     = require('fs');

// Set up winston
log.remove(log.transports.Console);
log.add(log.transports.Console, {
	'level': 'warn',
	'colorize': true,
	'timestamp': true,
	'json': false
});

before(function(done) {
	var confFile;

	function runDbSetup(confFile) {
		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		db.setup(require(confFile), function(err) {
			assert( ! err, 'err should be negative');

			done();
		});
	}

	if (process.argv[3] === undefined)
		confFile = __dirname + '/../../../config/db_test.json';
	else
		confFile = process.argv[3].split('=')[1];

	log.verbose('DB config file: "' + confFile + '"');

	fs.stat(confFile, function(err) {
		var altConfFile = __dirname + '/../config/' + confFile;

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
});