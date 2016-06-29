[![Build Status](https://travis-ci.org/larvit/larvitdb.svg?branch=master)](https://travis-ci.org/larvit/larvitdb) [![Dependencies](https://david-dm.org/larvit/larvitdb.svg)](https://david-dm.org/larvit/larvitdb.svg)

# larvitdb

DB pool wrapper for node.js
This module is used to share a mysql/mariadb pool of connections between modules, classes, files etc.

It also logs with [winston](https://www.npmjs.com/package/winston) if there is a database error, so you do not need to fetch the database error manually each time.

## Installation

```bash
npm i larvitdb
```

## Usage

The module must first be required and then configured.
Make this in your main application file:

```javascript
const db = require('larvitdb');

db.setup({
	'connectionLimit':   10,
	'socketPath':        '/var/run/mysqld/mysqld.sock',
	'user':              'foo',
	'password':          'bar',
	'charset':           'utf8_general_ci',
	'supportBigNumbers': true,
	'database':          'my_database_name'
});
```

See list of native options [here](https://github.com/felixge/node-mysql/#connection-options). Then you can just require the module in your other files for usage, like this:

A direct query

```javascript
const db = require('larvitdb');

db.query('SELECT 1 + 1 AS solution', function(err, rows, fields) {
	console.log('dbmodel: The solution is: ', rows[0].solution);
});
```

Or, if a connection is needed:

```javascript
const db = require('larvitdb');

db.pool.getConnection(function(err, dbCon) {
	const sql = 'SELECT * FROM users WHERE username LIKE ' + dbCon.escape(postData);

	dbCon.query(sql, function(err, rows) {
		dbCon.release(); // Always release your connection when the query is done

		if (err)
			throw err;
	});
});
```

You dont need to get a connection to escape though. You can do like this:

```javascript
const db = require('larvitdb');

db.query('SELECT * FROM users WHERE id = ?', [userId], function(err, results) {
  // ...
});
```

## Advanced configuration - recoverable errors

Sometimes recoverable errors happend in the database. One such example is deadlocks in a cluster. Here we'll provide an example of how to make the database layer retry a query 5 times if a deadlock happends, before giving up.

```javascript
const db = require('larvitdb');

db.setup({
	'connectionLimit':   10,
	'socketPath':        '/var/run/mysqld/mysqld.sock',
	'user':              'foo',
	'password':          'bar',
	'charset':           'utf8_general_ci',
	'supportBigNumbers': true,
	'database':          'my_database_name',
	'retries':           5, // Defaults to 3 if omitted
	'recoverableErrors': ['PROTOCOL_CONNECTION_LOST', 'ER_LOCK_DEADLOCK'] // What error codes to retry, these are the defaults
});

// If this query fails with a deadlock, it will be retried up to 5 times.
// On each retry a warning will be logged with winston
// If the 5th retry fails, an error will be logged and the callback will be called with an error
db.query('DELETE FROM tmpTable LIMIT 10');
```

## Advanced configuration - long running queries

By default a warning is logged if a query runs longer than 10k ms (10 seconds). This number can be tweaked like this for 20 seconds:

```javascript
db.setup({
	...
	'longQueryTime': 20000
});
```

or like this to disable the warnings:

```javascript
db.setup({
	...
	'longQueryTime': false
});

## Custom functions

### Remove all tables from current database

This function will clean the current database from all tables.

```javascript
const db = require('larvitdb');

db.removeAllTables();
```
