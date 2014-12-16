'use strict';

var path    = require('path'),
    appPath = path.dirname(require.main.filename),
    mysql   = require('mysql'),
    dbConf  = require(appPath + '/config/db.json');

exports = module.exports = mysql.createPool(dbConf);