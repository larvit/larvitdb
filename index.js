'use strict';

const topLogPrefix = 'larvitdb: index.js: ';
const LUtils = require('larvitutils');
const lUtils = new LUtils();
const mysql = require('mysql2/promise');
const mysqlSync = require('mysql2');
const events = require('events');

class Db {

	/**
	 * Constructor
	 *
	 * @param {object} options - {}
	 * @param {object} [options.log] - Logging instance
	 */
	constructor(options) {
		options = options || {};

		this.options = options;

		if (!this.options.log) this.options.log = new lUtils.Log('info');

		// Default to 3 retries on recoverable errors
		if (this.options.retries === undefined) {
			this.options.retries = 3;
		}

		// Default to setting recoverable errors to lost connection
		if (this.options.recoverableErrors === undefined) {
			this.options.recoverableErrors = ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'];
		}

		// Default slow running queries to 10 seconds
		if (this.options.longQueryTime === undefined) {
			this.options.longQueryTime = 10000;
		}

		this.log = this.options.log;

		this.eventEmitter = new events.EventEmitter();

		this.eventEmitter.setMaxListeners(50); // There is no problem with a lot of listeneres on this one

		this.dbIsReady = false;
		this.connecting = false;
	}

	/**
	 * Connect to the database
	 *
	 * @return {promise} - resolves if connected
	 */
	async connect() {
		const validDbOptions = [];
		const logPrefix = topLogPrefix + 'connect() - ';
		const that = this;

		this.dbConf = {};

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

		for (const option of Object.keys(this.options)) {
			if (validDbOptions.indexOf(option) !== -1) {
				this.dbConf[option] = this.options[option];
			}
		}

		async function tryToConnect() {
			const subLogPrefix = logPrefix + 'tryToConnect() - ';

			try {
				const dbCon = await mysql.createConnection(that.dbConf);
				dbCon.destroy();
			} catch (err) {
				const retryIntervalSeconds = 1;

				that.log.warn(subLogPrefix + 'Could not connect to database, retrying in ' + retryIntervalSeconds + ' seconds. err: ' + err.message);

				await lUtils.setTimeout(retryIntervalSeconds * 1000);

				return await tryToConnect();
			}
		}

		// Wait for database to become available
		await tryToConnect();

		// Start pool and check connection
		this.pool = mysql.createPool(this.dbConf); // Expose pool

		// Start a sync pool (only to use with streaming API for now)
		this.poolSync = mysqlSync.createPool(this.dbConf);

		// Set timezone
		await this.pool.query('SET time_zone = \'+00:00\';');

		// Make connection test to database
		const [rows] = await this.pool.query('SELECT 1');

		if (rows.length === 0) {
			const err = new Error('No rows returned on database connection test');
			this.log.error(logPrefix + err.message);
			throw err;
		}

		this.log.verbose(logPrefix + 'Database connection test succeeded.');
		this.dbIsReady = true;
		this.eventEmitter.emit('dbIsReady');
	}

	/**
	 * Wrap getConnection to log errors
	 *
	 * @returns {promise} Promise object that resolves to a connection
	 */
	async getConnection() {
		const logPrefix = topLogPrefix + 'getConnection() - ';

		await this.ready();

		if (this.pool === undefined) {
			const err = new Error('No pool configured. sql: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));
			this.log.error(logPrefix + err.message);
			throw err;
		}

		const dbCon = await this.pool.getConnection();

		dbCon.org_query = dbCon.query;

		dbCon.query = async (sql, dbFields, options) => {
			const logPrefix = topLogPrefix + 'getConnection() - query() - connectionId: ' + dbCon.connection.connectionId + ' - ';

			options = options || {};

			if (options.retryNr === undefined) {
				options.retryNr = 0;
			}
			if (options.ignoreLongQueryWarning === undefined) {
				options.ignoreLongQueryWarning = true;
			}

			dbFields = this.formatDbFields(dbFields);

			const startTime = process.hrtime();

			try {
				const [rows, rowFields] = await dbCon.org_query(sql, dbFields);
				const queryTime = lUtils.hrtimeToMs(startTime, 4);

				// Always log all data modifying queries specifically so they can be fetched later on to replicate a state
				if (this.isSqlModifyingData(sql)) {
					this.log.verbose(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
				}

				if (this.options.longQueryTime !== false && this.options.longQueryTime < queryTime && options.ignoreLongQueryWarning !== true) {
					this.log.warn(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
				} else if (sql.toUpperCase().startsWith('SELECT')) {
					this.log.debug(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields) + ' in ' + queryTime + 'ms');
				}

				return {rows, rowFields};
			} catch (err) {
				err.sql = sql;
				err.fields = dbFields;

				// If this is a coverable error, simply try again.
				if (this.options.recoverableErrors.indexOf(err.code) !== -1) {
					options.retryNr = options.retryNr + 1;
					if (options.retryNr <= this.options.retries) {
						this.log.warn(logPrefix + 'Retrying database recoverable error: ' + err.message + ' retryNr: ' + options.retryNr + ' SQL: "' + sql + '" dbFields: ' + JSON.stringify(dbFields));

						await lUtils.setTimeout(50);

						return dbCon.query(sql, dbFields, options);
					}

					this.log.error(logPrefix + 'Exhausted retries (' + options.retryNr + ') for database recoverable error: ' + err.message + ' SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));

					throw err;
				}

				this.log.error(logPrefix + 'Database error msg: ' + err.message + ', code: "' + err.code + '" SQL: "' + err.sql + '" dbFields: ' + JSON.stringify(dbFields));

				throw err;
			}
		};

		return dbCon;
	}

	/**
	 * Run database query
	 *
	 * Wrap the query function to log database errors or slow running queries and promisify
	 *
	 * @param {string} sql - The SQL to be ran on the database
	 * @param {array} [dbFields] - Fields to replace ?:s in the SQL
	 * @param {object} [options] - Some options
	 * @param {integer} [options.retryNr] - What retry is this same query ran at
	 * @param {boolean} [options.ignoreLongQueryWarning] - Set to true to ignore long query warning for this specific query
	 * @return {promise} - Resolves with a result
	 */
	async query(sql, dbFields, options) {
		await this.ready();
		const dbCon = await this.getConnection();
		const result = await dbCon.query(sql, dbFields, options);
		dbCon.release();

		return result;
	};

	/**
	 * Checks if database is ready to accept queries
	 *
	 * @returns {Promise} Promise object that resolves when database is ready
	 */
	async ready() {
		if (this.dbIsReady) return;

		if (!this.connecting) {
			this.connectiong = true;
			this.connect();
		}

		return new Promise(resolve => this.eventEmitter.once('dbIsReady', resolve));
	}

	async removeAllTables() {
		const tables = [];

		await this.ready();

		const dbCon = await this.getConnection();
		await dbCon.query('SET FOREIGN_KEY_CHECKS=0;');

		// Gather table names
		const tableRows = await dbCon.query('SHOW TABLES');

		for (let i = 0; tableRows.rows[i] !== undefined; i++) {
			tables.push(tableRows.rows[i]['Tables_in_' + this.options.database]);
		}

		// Actually remove tables
		for (let i = 0; tables[i] !== undefined; i++) {
			let tableName = tables[i];

			await dbCon.query('DROP TABLE `' + tableName + '`;');
		}

		// Set foreign key checks back to normal
		await dbCon.query('SET FOREIGN_KEY_CHECKS=1;');
		dbCon.release();
	}

	streamQuery(sql, dbFields) {
		const logPrefix = topLogPrefix + 'streamQuery() - connectionId: x - ';
		const { log } = this;

		dbFields = this.formatDbFields(dbFields);

		// Always log all data modifying queries specifically so they can be fetched later on to replicate a state
		if (this.isSqlModifyingData(sql)) {
			log.verbose(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields));
		} else if (sql.toUpperCase().startsWith('SELECT')) {
			log.debug(logPrefix + 'Ran SQL: "' + sql + '" with dbFields: ' + JSON.stringify(dbFields));
		}

		return this.poolSync.query(sql, dbFields);
	}

	formatDbFields(dbFields) {
		if (!dbFields) dbFields = [];

		if (dbFields && !Array.isArray(dbFields)) {
			dbFields = [dbFields];
		}

		// Convert datetimes to UTC
		if (Array.isArray(dbFields)) {
			for (let i = 0; dbFields[i] !== undefined; i++) {
				if (typeof dbFields[i] === Date) {
					const dbField = dbFields[i];
					dbField = dbField.toISOString();
					dbField[10] = ' '; // Replace T with a space
					dbField = dbField.substring(0, dbField.length - 1); // Cut the last Z off
				}
			}
		}

		return dbFields;
	}

	isSqlModifyingData(sql) {
		if (
			sql.toUpperCase().startsWith('SELECT')
			|| sql.toUpperCase().startsWith('SHOW')
		) {
			return false;
		} else {
			return true;
		}
	}
}

exports = module.exports = Db;
