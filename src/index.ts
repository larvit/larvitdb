import { Utils, Log, LogInstance } from 'larvitutils';
import * as events from 'events';
import { Pool, DbInitOptions, DbOptions } from './models.d';
import { Driver as DriverMySQL } from './drivers/mysql';
// import { Driver as DriverPostgres } from './drivers/postgres';

const topLogPrefix = 'larvitdb: index.ts: ';
const lUtils = new Utils();

class Db {
	public pool?: Pool;
	public dbCon?: any;
	private options: DbOptions;
	private log: LogInstance;
	private eventEmitter = new events.EventEmitter();
	private dbIsReady: boolean;
	private connecting: boolean;

	/**
	 * Constructor
	 *
	 * @param options - {}
	 * @param options.log - Logging instance
	 */
	constructor(options: DbInitOptions) {
		if (!options || !options.driver) {
			throw new Error('Driver option required');
		}

		if (!options.log) {
			options.log = new Log('info');
		}

		// Default to 3 retries on recoverable errors
		if (isNaN(Number(options.retries))) {
			options.retries = 3;
		}

		// Default to setting recoverable errors to lost connection
		if (!Array.isArray(options.recoverableErrors)) {
			options.recoverableErrors = ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'];
		}

		// Default slow running queries to 10 seconds
		if (isNaN(Number(options.longQueryTime))) {
			options.longQueryTime = 10000;
		}

		if (!options.connectOptions) {
			options.connectOptions = {};
		}

		this.log = options.log;

		this.eventEmitter.setMaxListeners(50); // There is no problem with a lot of listeners on this one

		this.dbIsReady = false;
		this.connecting = false;

		this.options = options as DbOptions;
	}

	/**
	 * Connect to the database
	 *
	 * @return {promise} - resolves if connected
	 */
	public async connect(): Promise<void> {
		this.dbCon = await this.options.driver.connect(this.options.connectOptions);

		this.dbIsReady = true;
		this.eventEmitter.emit('dbIsReady');
	}

	/**
	 * Wrap getConnection to log errors
	 *
	 * @returns {promise} Promise object that resolves to a connection
	 */
	public async getConnection() {
		const logPrefix = topLogPrefix + 'getConnection() - ';

		await this.ready();

		if (this.pool === undefined) {
			const err = new Error('No pool configured.');
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

export { Db, DriverMySQL };
