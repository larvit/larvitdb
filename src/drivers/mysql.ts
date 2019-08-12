import { DriverOptions, Pool } from '../models.d';
import { Utils, LogInstance } from 'larvitutils';
import * as mysql from 'mysql2/promise';
import * as mysqlSync from 'mysql2';

const topLogPrefix = 'drivers/mysql.ts: ';

type ConnectOptions = {
	host?: string;
	port?: number;
	localAddress?: string;
	socketPath?: string;
	user?: string;
	password?: string;
	database?: string;
	charset?: string;
	timezone?: string;
	connectTimeout?: number;
	stringifyObjects?: string;
	insecureAuth?: string;
	typeCast?: string;
	queryFormat?: string;
	supportBigNumbers?: boolean;
	bigNumberStrings?: boolean;
	dateStrings?: string;
	debug?: boolean;
	trace?: string;
	multipleStatements?: boolean;
	flags?: string;
	ssl?: boolean;

	// Valid for pools
	waitForConnections?: boolean;
	connectionLimit?: number;
	queueLimit?: number;
};

class Driver {
	private log: LogInstance;

	constructor(options: DriverOptions) {
		this.log = options.log;
	}

	public async connect(options: ConnectOptions) {
		const logPrefix = topLogPrefix + 'connect() - ';
		const { log } = this;
		const lUtils = new Utils({ log });

		async function tryToConnect(): Promise<boolean> {
			const subLogPrefix = logPrefix + 'tryToConnect() - ';

			try {
				const dbCon = await mysql.createConnection(options);
				dbCon.destroy();
				return true;
			} catch (err) {
				const retryIntervalSeconds = 1;

				log.warn(subLogPrefix + 'Could not connect to database, retrying in ' + retryIntervalSeconds + ' seconds. err: ' + err.message);

				await lUtils.setTimeout(retryIntervalSeconds * 1000);

				return await tryToConnect();
			}
		}

		// Wait for database to become available
		await tryToConnect();

		// Start pool and check connection
		this.pool = this.  mysql.createPool(this.connectOptions) as Pool; // Expose pool

		// Start a sync pool (only to use with streaming API for now)
		this.poolSync = mysqlSync.createPool(this.connectOptions);

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
}

export { Driver };
