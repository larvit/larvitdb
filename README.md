[![Build Status](https://travis-ci.org/larvit/larvitdb.svg?branch=master)](https://travis-ci.org/larvit/larvitdb) [![Dependencies](https://david-dm.org/larvit/larvitdb.svg)](https://david-dm.org/larvit/larvitdb.svg)

# larvitdb

Database wrapper layer for node.js. Builds on top of mysql2. Is used to share a mysql/mariadb pool of connections, logs, error handling and async/await.

## Installation

```bash
npm i larvitdb
```

## Usage

### Initialization

The module must first be required and then configured.
Make this in your main application file:

```javascript
const Db = require('larvitdb');
const db = new Db({
	host:              '127.0.0.1',                   // Do not use with socketPath
	socketPath:        '/var/run/mysqld/mysqld.sock', // Do not use with host
	connectionLimit:   10,                            // Connections in the pool
	user:              'foo',
	password:          'bar',
	charset:           'utf8_general_ci',
	supportBigNumbers: true,
	database:          'my_database_name',
	log:               log                            // Logging object. Will default to a simple console logger if not provided
	// See list of native options [here](https://github.com/felixge/node-mysql/#connection-options).
});
```

### Important about time zones!

All sessions with the database will be set to UTC time!

When setting datetime stuff, use the javascript native Date object, like this:

```javascript
await db.query('INSERT INTO users (created, username) VALUES(?,?)', [new Date(), 'foobar']);
```

If you do, this library will convert the time zone info for you.

However, please note that all date time you get back from the database will be in UTC.

### Check if database connection is ready

To see when the database connection is ready to recieve commands. Will automatically connect if that is not done already.

```javascript
await db.ready();
console.log('ready');
```

However, a query can be ran before ready(), it will wait until the database connection is ready.

### Queries

#### Simple query

A direct query to any connection in the pool

```javascript
const dbRes = await db.query('SELECT 1 + 1 AS solution');
console.log('solution is: ' + dbRes.rows[0].solution); // 2
console.log('All fields: ' + dbRes.fields); // ['solution']
```

#### Simple query to specific connection

For example if you want to lock tables and run multiple queries on the samme connection.

```javascript
const dbCon = await db.pool.getConnection();
const sql   = 'SELECT * FROM users WHERE username LIKE ' + dbCon.escape(dataToBeEscaped);
const dbRes = await dbCon.query(sql);
dbCon.release(); // Always release your connection when the query is done
```

#### Escape data

Either you use a database connection as in the example above, or you do like this:

```javascript
const firstName = 'Bosse';
const lastName  = 'Nilsson';
const dbRes     = await db.query('SELECT * FROM users WHERE firstname = ? AND lastname = ?', [firstName, lastName]);
```

#### Stream query

When working with big data sets, you do not want to load it all into memory before starting to work through it. Instead you want to stream the result from the database.

```javascript
const stream = db.streamQuery('SELECT * FROM bigAssTable');

stream.on('fields', fields => {
	console.log('Array with field names that will be returned');
});

stream.on('result', row => {
	console.log('Handle row');
});

stream.on('error', err => {
	throw err;
});

stream.on('end', () => {
	console.log('No more rows will come');
});
```

## Advanced configuration - recoverable errors

Sometimes recoverable errors happend in the database. One such example is deadlocks. Here we'll provide an example of how to make the database layer retry a query 5 times if a deadlock happends, before giving up.

```javascript
const Db = require('larvitdb');
const db = new Db({
	socketPath:        '/var/run/mysqld/mysqld.sock',
	user:              'foo',
	password:          'bar',
	database:          'my_database_name',
	retries:           5,                                               // Defaults to 3 if omitted
	recoverableErrors: ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'] // What error codes to retry, these are the defaults
});

// If this query fails with a deadlock, it will be retried up to 5 times.
// On each retry a warning will be logged with the provided logging instance
// If the 5th retry fails, an error will be logged and an error will be thrown
await db.query('DELETE FROM tmpTable LIMIT 10');
```

## Advanced configuration - long running queries

By default a warning is logged if a query runs longer than 10k ms (10 seconds). This number can be tweaked like this for 20 seconds:

```javascript
const db = new Db({
	...
	longQueryTime: 20000
});
```

or like this to disable the warnings:

```javascript
const db = new Db({
	...
	longQueryTime: false
});
```

## Custom functions

### Remove all tables from current database

This function will clean the current database from all tables.

```javascript
await db.removeAllTables();
```

## Version history

### 3.0.0
* Redesign of initialization to not be a singleton
* Promisify everything
* De-couple winston and make logging more agnostic

### 2.0.0

* Always set all new sessions to UTC time zone
* Convert Date objects to UTC datetimestamps that fits MariaDB and MySQL

Major from 1.x to 2.0 since this might break functionality for some implementations.
