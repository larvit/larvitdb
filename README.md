# larvitdb

DB pool wrapper for node.js
This module is used to share a mysql/mariadb pool of connections between modules, classes, files etc.

It also logs with [winston](https://www.npmjs.com/package/winston) if there is a database error, so you do not need to fetch the database error manually each time.

## Installation
    npm i larvitdb

## Configuration
Create a configuration file: <application path>/config/db.json and fill it like this:

    {
    	"connectionLimit":   10,
    	"socketPath":        "/var/run/mysqld/mysqld.sock",
    	"user":              "foo",
    	"password":          "bar",
    	"charset":           "utf8_general_ci",
    	"supportBigNumbers": true,
    	"database":          "my_database_name"
    }

## Usage

    var db = require('larvitdb');

A direct query

    db.query('SELECT 1 + 1 AS solution', function(err, rows, fields) {
    	console.log('dbmodel: The solution is: ', rows[0].solution);
    });

Or, if a connection is needed:

    db.getConnection(function(err, dbCon) {
    	var sql = 'SELECT * FROM users WHERE username LIKE ' +
    	    	dbCon.escape(postData);

    	dbCon.query(sql, function(err, rows) {
    		dbCon.release(); // Always release your connection when the query is done

    		if (err)
    			throw err;


You dont need to get a connection to escape though. You can do like this:

    db.query('SELECT * FROM users WHERE id = ?', [userId], function(err, results) {
      // ...
    });
