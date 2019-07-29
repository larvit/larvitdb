'use strict';

const assert = require('assert');
const LUtils = require('larvitutils');
const lUtils = new LUtils();
const test = require('tape');
const log = new lUtils.Log('warn');
const Db = require('../index.js');
const fs = require('fs');

let db;

Error.stackTraceLimit = 10;

async function dbTests(dbCon) {
	await dbCon.query('CREATE TABLE `fjant` (`test` int NOT NULL) ENGINE=\'InnoDB\';');
	await dbCon.query('INSERT INTO `fjant` VALUES(13);');

	const testFromFjant = await dbCon.query('SELECT test FROM fjant');

	assert.strictEqual(testFromFjant.rows.length, 1);
	assert.strictEqual(testFromFjant.rows[0].test, 13);

	await dbCon.query('UPDATE fjant SET test = ?', 7);
	const testFromFjantAgain = await dbCon.query('SELECT test FROM fjant');
	assert.strictEqual(testFromFjantAgain.rows.length, 1);
	assert.strictEqual(testFromFjantAgain.rows[0].test, 7);

	await dbCon.query('DELETE FROM fjant');
	const testFromFjantLast = await dbCon.query('SELECT test FROM fjant');
	assert.strictEqual(testFromFjantLast.rows.length, 0);

	await dbCon.query('DROP TABLE fjant');
}

test('Setup db and do checks', t => {
	let confFile;

	async function checkEmptyDb() {
		const { rows } = await db.query('SHOW TABLES');
		if (rows.length) throw new Error('Database is not empty. To make a test, you must supply an empty database!');

		t.end();
	}

	async function runDbSetup(confFile) {
		let conf;

		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		conf = require(confFile);
		conf.log = log;

		db = new Db(conf);
		await db.ready();
		await checkEmptyDb();
	}

	if (process.env.TRAVIS) {
		confFile = __dirname + '/../config/db_travis.json';
	} else if (process.env.CONFFILE) {
		confFile = process.env.CONFFILE;
	} else {
		confFile = __dirname + '/../config/db_test.json';
	}

	log.verbose('DB config file: "' + confFile + '"');

	fs.stat(confFile, err => {
		const altConfFile = __dirname + '/../config/' + confFile;

		if (err) {
			log.info('Failed to find config file "' + confFile + '", retrying with "' + altConfFile + '"');

			fs.stat(altConfFile, err => {
				if (err) throw err;

				runDbSetup(altConfFile);
			});
		} else {
			runDbSetup(confFile);
		}
	});
});


test('Simple queries', async (t) => {
	await dbTests(db);
	t.end();
});

test('Queries on a single connection from the pool', async (t) => {
	const dbCon = await db.getConnection();
	await dbTests(dbCon);
	t.end();
});

test('Remove all tables from database', async (t) => {
	// Create tables with internal relations
	await db.query(`CREATE TABLE foo (
			id int(11) NOT NULL AUTO_INCREMENT,
			name int(11) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`);

	await db.query(`CREATE TABLE bar (
			fooId int(11) NOT NULL,
			stuff varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
			KEY fooId (fooId),
			CONSTRAINT bar_ibfk_1 FOREIGN KEY (fooId) REFERENCES foo (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`);


	// Try to remove all tables from the database
	await db.removeAllTables();

	// Check so no tables exists
	const result = await db.query('SHOW TABLES');
	assert.strictEqual(result.rows.length, 0);
	t.end();
});

test('Stream rows', async (t) => {
	let rowNr = 0;

	// Create table with data
	await db.query('CREATE TABLE `plutt` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` varchar(191) NOT NULL);');
	await db.query('INSERT INTO plutt (name) VALUES(\'bosse\'),(\'hasse\'),(\'vråkbert\');');

	// Get dbCon
	const dbCon = await db.getConnection();

	// Check contents
	const query = db.streamQuery('SELECT * FROM plutt');

	query.on('error', err => {
		throw err;
	});

	query.on('fields', fields => {
		assert.strictEqual(fields[0].name, 'id');
		assert.strictEqual(fields[1].name, 'name');
	});

	query.on('result', row => {
		dbCon.pause(); // Pause streaming while handling row

		rowNr++;

		if (rowNr === 1) {
			assert.strictEqual(row.id, 1);
			assert.strictEqual(row.name, 'bosse');
		} else if (rowNr === 2) {
			assert.strictEqual(row.id, 2);
			assert.strictEqual(row.name, 'hasse');
		} else if (rowNr === 2) {
			assert.strictEqual(row.id, 3);
			assert.strictEqual(row.name, 'vråkbert');
		}

		dbCon.resume(); // Resume streaming when processing of row is done (this is normally done in async, doh)
	});

	query.on('end', async () => {
		assert.strictEqual(rowNr, 3);
		dbCon.release();

		await db.removeAllTables();

		t.end();
	});
});

test('Time zone dependent data', async (t) => {
	// Create table
	const sql = 'CREATE TABLE tzstuff (id int(11), tzstamp timestamp, tzdatetime datetime);';
	await db.query(sql);

	// Set datetime as javascript Date object
	const dateObj = new Date('2018-03-04T17:38:20Z');
	await db.query('INSERT INTO tzstuff VALUES(?,?,?);', [1, dateObj, dateObj]);

	// Check the values
	const result = await db.query('SELECT * FROM tzstuff ORDER BY id');
	let foundRows = 0;

	for (let i = 0; result.rows[i] !== undefined; i++) {
		const row = result.rows[i];

		if (row.id === 1) {
			foundRows++;
			assert.strictEqual(row.tzstamp.toISOString(), '2018-03-04T17:38:20.000Z');
			assert.strictEqual(row.tzdatetime.toISOString(), '2018-03-04T17:38:20.000Z');
		}
	}

	assert.strictEqual(foundRows, 1);

	// Remove table
	await db.query('DROP TABLE tzstuff;');
	t.end();
});

test('Close the db pool', async (t) => {
	await db.pool.end();

	// There are several active handles at this stage, don't know why
	// process._getActiveHandles()[0];

	t.end();
	process.exit();
});
