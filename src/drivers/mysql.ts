import { DriverOptions, DriverReturn } from '../models.d';
import { Utils, LogInstance } from 'larvitutils';
import * as mysql from 'mysql2/promise';
import * as mysqlSync from 'mysql2';

const topLogPrefix = 'larvitdb: drivers/mysql.ts: ';

class Driver {
	private log: LogInstance;

	constructor(options: DriverOptions) {
		this.log = options.log;
	}

	public async connect(options: mysql.ConnectionOptions): Promise<DriverReturn> {
		const logPrefix = topLogPrefix + 'connect() - ';
		const { log } = this;
		const lUtils = new Utils({ log });

		async function tryToConnect(): Promise<void> {
			const subLogPrefix = logPrefix + 'tryToConnect() - ';

			try {
				const dbCon = await mysql.createConnection(options);
				dbCon.destroy();
				return;
			} catch (err) {
				const retryIntervalSeconds = 1;

				log.warn(subLogPrefix + 'Could not connect to database, retrying in ' + retryIntervalSeconds + ' seconds. err: ' + err.message);

				await lUtils.setTimeout(retryIntervalSeconds * 1000);

				return await tryToConnect();
			}
		}

		// Wait for database to become available
		await tryToConnect();

		const returnObj: DriverReturn = {
			// Start pool and check connection
			pool: mysql.createPool(options), // Expose pool

			// Start a sync pool (only to use with streaming API for now)
			poolSync: mysqlSync.createPool(options),
		};

		// Set timezone
		await returnObj.pool.query('SET time_zone = \'+00:00\';');
		await new Promise((resolve, reject) => {
			returnObj.poolSync('SET time_zone = \'+00:00\';', (err: Error) => {
				if (err) {
					reject(err);
				} else {
					resolve();
				}
			});
		});

		// Make connection test to database
		const [rows] = await returnObj.pool.query('SELECT 1');

		if (rows.length === 0) {
			const err = new Error('No rows returned on database connection test');
			this.log.error(logPrefix + err.message);
			throw err;
		}

		this.log.verbose(logPrefix + 'Database connection test succeeded.');

		return returnObj;
	}
}

export { Driver };
