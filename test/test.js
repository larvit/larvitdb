'use strict';

const LUtils = require('larvitutils');
const test = require('tape');
const log = new LUtils.Log('info');
const Db = require('../index.js');
const fs = require('fs');

let db;

Error.stackTraceLimit = 10;

async function dbTests(dbCon, t) {
	await dbCon.query('CREATE TABLE `fjant` (`test` int NOT NULL) ENGINE=\'InnoDB\';');
	await dbCon.query('INSERT INTO `fjant` VALUES(13);');

	const testFromFjant = await dbCon.query('SELECT test FROM fjant');

	t.strictEqual(testFromFjant.rows.length, 1);
	t.strictEqual(testFromFjant.rows[0].test, 13);

	await dbCon.query('UPDATE fjant SET test = ?', 7);
	const testFromFjantAgain = await dbCon.query('SELECT test FROM fjant');
	t.strictEqual(testFromFjantAgain.rows.length, 1);
	t.strictEqual(testFromFjantAgain.rows[0].test, 7);

	await dbCon.query('DELETE FROM fjant');
	const testFromFjantLast = await dbCon.query('SELECT test FROM fjant');
	t.strictEqual(testFromFjantLast.rows.length, 0);

	await dbCon.query('DROP TABLE fjant');
}

async function setup(options) {
	options = options || {};
	options.log = options.log || log;

	async function runDbSetup(confFile) {
		let conf;

		log.verbose('DB config: ' + JSON.stringify(require(confFile)));

		conf = require(confFile);
		const dbOptions = {
			...conf
		};

		for (const option in options) {
			dbOptions[option] = options[option];
		}

		const dbInstance = new Db(dbOptions);
		await dbInstance.ready();

		return dbInstance;
	}

	let confFile;
	if (process.env.TRAVIS) {
		confFile = __dirname + '/../config/db_travis.json';
	} else if (process.env.CONFFILE) {
		confFile = process.env.CONFFILE;
	} else {
		confFile = __dirname + '/../config/db_test.json';
	}

	log.verbose('DB config file: "' + confFile + '"');

	return new Promise((res, rej) => {
		fs.stat(confFile, async (err) => {
			if (err) {
				const altConfFile = __dirname + '/../config/' + confFile;

				log.info('Failed to find config file "' + confFile + '", retrying with "' + altConfFile + '"');

				fs.stat(altConfFile, async (err) => {
					if (err) return rej(err);

					try {
						const dbInstance = await runDbSetup(altConfFile);

						return res(dbInstance);
					} catch (err) {
						return rej(err);
					}
				});
			} else {
				try {
					const dbInstance = await runDbSetup(confFile);

					return res(dbInstance);
				} catch (err) {
					return rej(err);
				}
			}
		});
	});
}

test('Setup db and do checks', async (t) => {
	async function checkEmptyDb() {
		const { rows } = await db.query('SHOW TABLES');
		if (rows.length) throw new Error('Database is not empty. To make a test, you must supply an empty database!');
	}

	db = await setup();
	await db.removeAllTables();
	await checkEmptyDb();
	t.end();
});

test('Simple queries', async (t) => {
	await dbTests(db, t);
	t.end();
});

test('Queries on a single connection from the pool', async (t) => {
	const dbCon = await db.getConnection();
	await dbTests(dbCon, t);
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
	t.strictEqual(result.rows.length, 0);
	t.end();
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
			t.strictEqual(row.tzstamp.toISOString(), '2018-03-04T17:38:20.000Z');
			t.strictEqual(row.tzdatetime.toISOString(), '2018-03-04T17:38:20.000Z');
		}
	}

	t.strictEqual(foundRows, 1);

	// Remove table
	await db.query('DROP TABLE tzstuff;');
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

	async function handleStream(query) {
		return new Promise((res, rej) => {
			query.on('error', err => {
				rej(err);
			});

			query.on('fields', fields => {
				t.strictEqual(fields[0].name, 'id');
				t.strictEqual(fields[1].name, 'name');
			});

			query.on('result', row => {
				dbCon.pause(); // Pause streaming while handling row

				rowNr++;

				if (rowNr === 1) {
					t.strictEqual(row.id, 1);
					t.strictEqual(row.name, 'bosse');
				} else if (rowNr === 2) {
					t.strictEqual(row.id, 2);
					t.strictEqual(row.name, 'hasse');
				} else if (rowNr === 2) {
					t.strictEqual(row.id, 3);
					t.strictEqual(row.name, 'vråkbert');
				}

				dbCon.resume(); // Resume streaming when processing of row is done (this is normally done in async, doh)
			});

			query.on('end', async () => {
				t.strictEqual(rowNr, 3);
				dbCon.release();

				res();
			});
		});
	}

	await handleStream(query);
	t.end();
});

test('Configure data change log level', async (t) => {
	let loggedDebugStr = '';
	const specialLogger = {
		error: str => console.log(str),
		warn: str => console.log(str),
		info: str => console.log(str),
		verbose: str => console.log(str),
		debug: str => console.log(str),
		silly: str => console.log(str),
		specialDebug: str => loggedDebugStr = str
	};
	const dbInstance = await setup({log: specialLogger, dataChangeLogLevel: 'specialDebug'});

	await dbInstance.query('CREATE TABLE logTestTable (id int(11));');
	await dbInstance.query('INSERT INTO logTestTable VALUES(?);', [1]);

	t.ok(loggedDebugStr.includes('Ran SQL: "INSERT INTO logTestTable VALUES(?);" with dbFields: [1] in '));
	t.end();
});

test('Configure data change log level with invalid logger should throw', async (t) => {
	const specialLogger = {
		error: str => console.log(str)
	};

	try {
		await setup({log: specialLogger, dataChangeLogLevel: 'nonExistingLogFunction'});
		t.fail('Did not get expected exception');
	} catch (err) {
		t.ok(true, 'Got expected exception: ' + err.message);
	}

	t.end();
});

test('Close the db pool', async (t) => {
	await db.pool.end();

	// There are several active handles at this stage, don't know why
	// process._getActiveHandles()[0];

	t.end();
	process.exit();
});
