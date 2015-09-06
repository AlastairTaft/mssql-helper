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
        level = ((levelIndex > -1) ? process.argv[levelIndex + 1] : "info");

    winston.clear();
    winston.add(winston.transports.Console,
                { colorize: true, level: level,
                    'timestamp': function () {var m = new Moment(); return m.format("(YYYY-MM-DD HH:mm:ss)") + '; mssql-helper ;'; } });
}

initLogger();

module.exports = {executeQuery : executeQuery, config : config, run : run, createConnection: createConnection, isInTransaction:isInTransaction, beginTran:beginTran, commitTran:commitTran, rollbackTran:rollbackTran};

function executeQuery(context, sql, params, callback){
	winston.debug("executeQuery: " + sql, debugContext(context));
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	if (context.commiting || context.rollbacking){
		throw new Error("can't executeQuery while transaction ending.");
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
	winston.debug("createConnection", debugContext(context));

	var cn;
	if (typeof context == 'function'){
		callback = context;
		context = null;
	}
	if (context != null) {
		if (context.sqlConnected || context.sqlConnecting){
			throw new Error("can't open multiple connections in the same context. Use another context.");
		}
		prepareContext(context);
		context.sqlConnected = false;
		context.sqlConnecting = true;
		context.createConnectionCallback = function(cx, cb, err, cn){
			winston.debug("createConnection callback.", debugContext(cx));

			if (err) {
				winston.error(err);
				throw new Error(err);
			}
			if (cx != null) {
				cx.sqlConnection = cn;
				cx.sqlTransaction = cx.sqlConnection.transaction();
				cx.sqlConnected = true;
				cx.sqlConnecting = false;
			}
			if (cb) cb(cn);
			return;
		}
	}
	cn = new mssql.Connection(config, function (err){
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (context != null) {
			context.createConnectionCallback(context, callback, err, cn);
			return;			
		}
		if (callback) callback(cn);
	}); 
}

function runQueue(context){
	winston.debug("runQueue " , debugContext(context));
	if (context.isWaitingTransaction) return;	
	if (context.dbQueue.length == 0){
		context.dbRunning = false;
		return;	
	}
	if (context.isWaitingTransaction || context.sqlConnecting)	return;	
	context.dbRunning = true;
	context.runningCommand = true;
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
		if (cb) cb(err, recordset);
		runQueue(context);
	});
}

function run(cn, sql, params, callback){
	if (typeof params == 'function'){
		callback = params;
		params = null;
	}
	//winston.debug("run: " + sql, debugContext(context));
	var request = new mssql.Request(cn);
	if (params instanceof Array) params.forEach(function (item){
		request.input(item.name, item.value);
	});
	request.query(sql, function (err, recordset){
		//winston.debug("run ends: " + sql, debugContext(context));
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (callback) callback(err, recordset);
	});
}

function isInTransaction(context){
	return context.isInTransaction != undefined && context.isInTransaction == true;
}

function prepareContext(context) {
	winston.debug("prepareContext");
	context.sqlConnection = null;
	context.sqlTransaction = null;
	if (context.isInTransaction == undefined) context.isInTransaction = false;
	context.isWaitingTransaction = false;
	if (context.dbQueue == undefined) context.dbQueue = [];
	context.dbRunning = false;
	context.sqlConnecting= false;
	context.runningCommand = false;
	context.commiting = false;
	context.rollbacking = false;
}

function beginTran(context, callback){
	winston.debug("beginTran.", debugContext(context));
	if (context.isInTransaction) {
		throw new Error("can't nest transactions.");
	}
	context.isInTransaction = true;

	context.beginCallback = function(cx, cb){
		winston.debug("beginTran callback.", debugContext(cx));
		cx.isWaitingTransaction = false;
		if (cb) cb();
		if (!cx.dbRunning && cx.dbQueue.length != 0) runQueue(cx);
		return;
	}
	if (context.sqlConnecting){
		var cnCallback = context.createConnectionCallback;
		context.createConnectionCallback = function(cx, cb, err, cn){
			cx.createConnectionCallback = undefined;
			cnCallback(cx, cb, err, cn);
			context.isWaitingTransaction = true;
			doBegin(context, callback);
			return;
		}
		return;
	}
	context.isWaitingTransaction = true;
	if (context.sqlConnecting || context.sqlConnected){
		doBegin(context, callback);
		return;
	}
	createConnection(context, function(cn){
		doBegin(context, callback);
	});
}

function doBegin(context, callback){
	winston.debug("doBegin.", debugContext(context));
	context.sqlTransaction.begin(function(){
		context.beginCallback(context, callback);
		context.beginCallback = undefined;
	});
}

function commitTran(context, callback){
	winston.debug("commitTran.", debugContext(context));
	context.commiting = true;
	if (context.dbQueue.length != 0) {
		lastCallback = context.dbQueue[context.dbQueue.length-1].callback;
		context.dbQueue[context.dbQueue.length-1].callback = function(err, recordset){
			if (lastCallback) lastCallback(err, recordset);
			doCommit(context, callback);
		}
		return;
	}
	if (context.beginCallback) {
		lastCallback = context.beginCallback;
		context.beginCallback = function(cx, cb){
			cx.beginCallback = undefined;
			lastCallback(cx, cb);
			doCommit(cx, cb);
		}
		return;
	}
	doCommit(context, callback);
}

function doCommit(context, callback){
	context.isWaitingTransaction = true;
	context.sqlTransaction.commit(function(err){
		winston.debug("commitTran callback.", debugContext(context));
		context.isInTransaction = false;
		context.isWaitingTransaction = false;
		context.commiting = false
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (callback) callback(err);
	});
}

function rollbackTran(context, callback){
	winston.debug("rollbackTran.", debugContext(context));
	context.rollbacking = true;
	if (context.dbQueue.length != 0) {
		lastCallback = context.dbQueue[context.dbQueue.length-1].callback;
		context.dbQueue[context.dbQueue.length-1].callback = function(err, recordset){
			if (lastCallback) lastCallback(err, recordset);
			doRollback(context, callback);
		}
		return;
	}
	if (context.beginCallback) {
		lastCallback = context.beginCallback;
		context.beginCallback = function(cx, cb){
			cx.beginCallback = undefined;
			lastCallback(cx, cb);
			doRollback(cx, cb);
		}
		return;
	}
	doRollback(context, callback);
}

function doRollback(context, callback){
	winston.debug("doRollback.", debugContext(context));
	context.isWaitingTransaction = true;
	context.sqlTransaction.rollback(function(err){
		winston.debug("rollbackTran callback.", debugContext(context));
		context.isInTransaction = false;
		context.isWaitingTransaction = false;
		context.rollbacking = false;
		if (err) {
			winston.error(err);
			throw new Error(err);
		}
		if (callback) callback(err);
	});
}

function pushCmd(context, sql, params, callback){
	if (context.dbQueue == undefined) context.dbQueue = [];
	context.dbQueue.push({sql:sql, params:params, callback:callback});
}

function debugContext(context){
	if (!context) return {context: context};
	return { 
		dbQueue : context.dbQueue ? context.dbQueue.length : 0
//		, dbRunning: context.dbRunning
//		, runningCommand: context.runningCommand
		, connecting: context.sqlConnecting
		, isInT: context.isInTransaction
		, isWaitT:context.isWaitingTransaction};
}

