import { LogInstance } from 'larvitutils';

type DriverReturn = {
	pool: any;
	poolSync: any;
};

type Row = {
	[key: string]: any;
};

type QueryResponse = {
	rows: Row[];
	fields: string[];
};

type Pool = {
	query(sql: string): Promise<QueryResponse>;
	getConnection(): any;
};

type DriverInstance = {
	connectOptions: any;
	createConnection: any;
	connect(connectOptions: any): Promise<void>;
};

type DbInitOptions = {
	driver: DriverInstance;
	log?: LogInstance;
	retries?: number;
	recoverableErrors?: string[];
	longQueryTime?: number;
	connectOptions?: any;
};

type DbOptions = {
	driver: DriverInstance;
	log: LogInstance;
	retries: number;
	recoverableErrors: string[];
	longQueryTime: number;
	connectOptions: any;
};

type DriverOptions = {
	log: LogInstance;
};

export { Row, QueryResponse, Pool, DriverInstance, DbInitOptions, DbOptions, DriverOptions, DriverReturn };
