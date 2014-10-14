var mssql = require('mssql'),
    winston     = require("winston"),
    Moment      = require('moment');


var config = {
    user: '',
    password: '',
    server: '', // You can use 'localhost\\instance' to connect to named instance
    database: '',

    options: {
    }
}

function initLogger() {
    "use strict";
    var levelIndex = process.argv.indexOf("--level"),
        level = ((levelIndex > -1) ? process.argv[levelIndex + 1] : "debug");

    winston.clear();
    winston.add(winston.transports.Console,
                { colorize: true, level: level,
                    'timestamp': function () {var m = new Moment(); return m.format("(YYYY-MM-DD HH:mm:ss)"); } });
}

initLogger();

module.exports = {executeQuery : executeQuery, config : config, run : run, createConnection: createConnection, runSync: runSync, isInTransaction:isInTransaction, beginTran:beginTran, commitTran:commitTran, rollbackTran:rollbackTran};

var callbackTrans = null;

function executeQuery(context, sql, params, callback){
	if (context.sqlConnection == undefined) {
		createConnection( function(cn){
			context.sqlConnection = cn;
			context.isInTransaction = false;
			context.isWaitingTransaction = false;
			runSync(context, context.sqlConnection, sql, params, callback);
		});
		return;
	}
	runSync(context, context.sqlConnection, sql, params, callback);
}

function createConnection(callback) { 
	var cn = new mssql.Connection(config, function (err){
		winston.info("mssql-helper: createConnection");

		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		callback(cn);
	}); 
}

function runSync(context, cn, sql, params, callback){
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	winston.debug("mssql-helper: runSync: " + sql);
	if (context.dbRunning == undefined){
		context.dbRunning = false;
	}
	if (!context.dbRunning) {
		context.dbQueue = [];
	}
	context.dbQueue.push({cn:cn, sql:sql, params:params, callback:callback});
	if (!context.dbRunning) runQueue(context);
}

function runQueue(context){
	if (context.dbQueue.length == 0 || context.isWaitingTransaction){
		context.dbRunning = false;
		return;	
	}
	context.dbRunning = true;
	winston.debug("mssql-helper: runQueue.");
	run(context.isInTransaction ? context.sqlTransaction : context.sqlConnection, context.dbQueue[0].sql, context.dbQueue[0].params, function(err, recordset){
		context.dbQueue[0].callback(err, recordset);
		if (err) {
			context.dbRunning = false;
			winston.error(err);
			throw new Error(err);
		}
		context.dbQueue.splice(0, 1);
		runQueue(context);
	});
}

function run(cn, sql, params, callback){
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	winston.debug("mssql-helper: run: " + sql);
	var request = new mssql.Request(cn);
	if (params instanceof Array) params.forEach(function (item){
		request.input(item.name, item.value);
	});
	request.query(sql, function (err, recordset){
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		callback(err, recordset);
	});
}

function isInTransaction(context){
	return context.isInTransaction != undefined && context.isInTransaction == true;
}

function beginTran(context, callback){
	winston.debug("mssql-helper: beginTran.");
	context.isWaitingTransaction = true;
	var cn = createConnection(function(con){
		cn = con;
		context.sqlConnection = cn;
		context.sqlTransaction = context.sqlConnection.transaction();
		context.sqlTransaction.begin(function(){
			context.isInTransaction = true;
			context.isWaitingTransaction = false;
			callback();
			if (!context.dbRunning && context.dbQueue.length != 0) runQueue(context);
		});
	});
}

function commitTran(context, callback){
	winston.debug("mssql-helper: commitTran.");
	context.isWaitingTransaction = true;
	context.sqlTransaction.commit(function(err){
		context.isInTransaction = false;
		context.isWaitingTransaction = false;
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		callback(err);
	});
}

function rollbackTran(context, callback){
	winston.debug("mssql-helper: rollbackTran.");
	context.isWaitingTransaction = true;	
	context.sqlTransaction.rollback(function(err){
		context.isInTransaction = false;
		context.isWaitingTransaction = false;
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		callback(err);
	});
}
