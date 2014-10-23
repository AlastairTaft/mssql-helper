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
                    'timestamp': function () {var m = new Moment(); return m.format("(YYYY-MM-DD HH:mm:ss)") + '; mssql-helper ;'; } });
}

initLogger();

module.exports = {executeQuery : executeQuery, config : config, run : run, createConnection: createConnection, isInTransaction:isInTransaction, beginTran:beginTran, commitTran:commitTran, rollbackTran:rollbackTran};

var callbackTrans = null;

function executeQuery(context, sql, params, callback){
	winston.debug("executeQuery: " + sql);
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	pushCmd(context, sql, params, callback);
	
	if (context.sqlConnecting == undefined) {
		createConnection(context, function(cn){
			if (!context.dbRunning) runQueue(context);
		});
		return;
	}

	if (!context.dbRunning) runQueue(context);
}

function createConnection(context, callback) { 
	winston.debug("createConnection");
	if (typeof context == 'function'){
		callback = context;
		context = null;
	}
	if (context != null) {
		prepareContext(context);
		context.sqlConnected = false;
		context.sqlConnecting = true;
	}
	var cn = new mssql.Connection(config, function (err){
		winston.debug("createConnection callback.");

		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (context != null) {
			context.sqlConnection = cn;
			context.sqlTransaction = context.sqlConnection.transaction();
			context.sqlConnected = true;
			context.sqlConnecting = false;
		}
		callback(cn);
	}); 
}

function runQueue(context){
	winston.debug("runQueue " + context.isWaitingTransaction + " - " + context.sqlConnecting + " - " + context.dbQueue.length);
	if (context.dbQueue.length == 0){
		context.dbRunning = false;
		return;	
	}
	if (context.isWaitingTransaction || context.sqlConnecting)	return;	
	context.dbRunning = true;
	context.runningCommand = true;
	if (context.isInTransaction) debugger;
	run(context.isInTransaction ? context.sqlTransaction : context.sqlConnection, context.dbQueue[0].sql, context.dbQueue[0].params, function(err, recordset){
		var cb = context.dbQueue[0].callback
		if (err) {
			context.dbRunning = false;
			context.runningCommand = false;
			winston.error(err);
			throw new Error(err);
		}
		context.dbQueue.splice(0, 1);
		context.runningCommand = false;
		cb(err, recordset);
		runQueue(context);
	});
}

function run(cn, sql, params, callback){
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	winston.debug("run: " + sql);
	var request = new mssql.Request(cn);
	if (params instanceof Array) params.forEach(function (item){
		request.input(item.name, item.value);
	});
	request.query(sql, function (err, recordset){
		winston.debug("run ends: " + sql);
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

function prepareContext(context) {
	winston.debug("prepareContext");
	//if (context.sqlConnection != undefined) return;
	context.sqlConnection = null;
	context.sqlTransaction = null;
	context.isInTransaction = false;
	context.isWaitingTransaction = false;
	if (context.dbQueue == undefined) context.dbQueue = [];
	context.dbRunning = false;
	context.sqlConnecting= false;
	context.runningCommand = false;
}

function beginTran(context, callback){
	winston.debug("beginTran.");
	if (context.sqlConnecting || context.sqlConnected){
		context.isWaitingTransaction = true;
		context.sqlTransaction.begin(function(){
			winston.debug("beginTran callback.");
			context.isInTransaction = true;
			context.isWaitingTransaction = false;
			callback();
			if (!context.dbRunning && context.dbQueue.length != 0) runQueue(context);
		});
		return;
	}
	createConnection(context, function(cn){
		context.isWaitingTransaction = true;
		context.sqlTransaction.begin(function(){
			winston.debug("beginTran callback.");
			context.isInTransaction = true;
			context.isWaitingTransaction = false;
			callback();
			if (!context.dbRunning && context.dbQueue.length != 0) runQueue(context);
		});
	});
}

function commitTran(context, callback){
	winston.debug("commitTran.");
	if (context.dbQueue.length != 0) {
		lastCallback = context.dbQueue[context.dbQueue.length-1].callback;
		context.dbQueue[context.dbQueue.length-1].callback = function(err, recordset){
			lastCallback(err, recordset);
			doCommit(context, callback);
		}
		return;
	}
	doCommit(context, callback);
}

function doCommit(context, callback){
	context.isWaitingTransaction = true;
	context.sqlTransaction.commit(function(err){
		winston.debug("commitTran callback.");
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
	winston.debug("rollbackTran.", { dbQueue : context.dbQueue.length, dbRunning: context.dbRunning, runningCommand: context.runningCommand, sqlConnecting: context.sqlConnecting});
	if (context.dbQueue.length != 0) {
		lastCallback = context.dbQueue[context.dbQueue.length-1].callback;
		context.dbQueue[context.dbQueue.length-1].callback = function(err, recordset){
			lastCallback(err, recordset);
			doRollback(context, callback);
		}
		return;
	}
	doRollback(context, callback);
}

function doRollback(context, callback){
	winston.debug("doRollback.");
	context.isWaitingTransaction = true;
	context.sqlTransaction.rollback(function(err){
		winston.debug("rollbackTran callback.");
		context.isInTransaction = false;
		context.isWaitingTransaction = false;
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		callback(err);
	});
}

function pushCmd(context, sql, params, callback){
	if (context.dbQueue == undefined) context.dbQueue = [];
	context.dbQueue.push({sql:sql, params:params, callback:callback});
}