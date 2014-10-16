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

module.exports = {executeQuery : executeQuery, config : config, run : run, createConnection: createConnection, isInTransaction:isInTransaction, beginTran:beginTran, commitTran:commitTran, rollbackTran:rollbackTran};

var callbackTrans = null;

function executeQuery(context, sql, params, callback){
	winston.debug("mssql-helper: executeQuery: " + sql);
	prepareContext(context);
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	pushCmd(context, sql, params, callback);
	
	if (!context.sqlConnecting && !context.sqlConnected) {
		context.sqlConnecting = true;
		createConnection(context, function(cn){
			context.sqlConnection = cn;
			context.sqlConnection.connect(function (err) {
				context.sqlConnected = true;
				context.sqlConnecting = false;
				if (!context.dbRunning) runQueue(context);
			});
		});
		return;
	}

	if (!context.dbRunning) runQueue(context);
}

function createConnection(context, callback) { 
	winston.debug("mssql-helper: createConnection");
	if (typeof context == 'function'){
		callback = context;
		context = null;
	}
	context.sqlConnected = false;
	context.sqlConnecting = true;

	var cn = new mssql.Connection(config, function (err){
		winston.debug("mssql-helper: createConnection callbackTrans.");

		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (context != null) {
			context.sqlConnection = cn;
			context.sqlTransaction = context.sqlConnection.transaction();
		}
		callback(cn);
	}); 
}

function runQueue(context){
	winston.debug("mssql-helper: runQueue " + context.isWaitingTransaction + " - " + context.sqlConnecting + " - " + context.dbQueue.length);
	if (context.dbQueue.length == 0){
		context.dbRunning = false;
		return;	
	}
	if (context.isWaitingTransaction || context.sqlConnecting)	return;	
	context.dbRunning = true;
	winston.debug("mssql-helper: runQueue 2.");
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
		winston.debug("mssql-helper: run callbackTrans: " + sql);
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
	if (context.dbQueue != undefined) return;
	context.sqlConnection = null;
	context.sqlTransaction = null;
	context.isInTransaction = false;
	context.isWaitingTransaction = false;
	context.dbQueue = [];
	context.dbRunning = false;
	context.sqlConnecting= false;
}

function beginTran(context, callback){
	winston.debug("mssql-helper: beginTran.");
	prepareContext(context);
	context.isWaitingTransaction = true;
	createConnection(context, function(cn){
		context.sqlConnection = cn;
		context.sqlConnection.connect(function (err) {
			context.sqlConnected = true;
			context.sqlConnecting = false;
			context.sqlTransaction.begin(function(){
				winston.debug("mssql-helper: beginTran callback.");
				context.isInTransaction = true;
				context.isWaitingTransaction = false;
				callback();
				if (!context.dbRunning && context.dbQueue.length != 0) runQueue(context);
			});
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

function pushCmd(context, sql, params, callback){
	context.dbQueue.push({sql:sql, params:params, callback:callback});
}