var mssql = require('mssql');

var config = {
    user: '',
    password: '',
    server: '', // You can use 'localhost\\instance' to connect to named instance
    database: '',

    options: {
    }
}

module.exports = {executeQuery : executeQuery, config : config, run : run, createConnection: createConnection, runSync: runSync, isInTransaction:isInTransaction, beginTran:beginTran, commitTran:commitTran, rollbackTran:rollbackTran};

function executeQuery(context, sql, params, callback){
	if (context.sqlConnection == undefined) {
		createConnection( function(cn){
			context.sqlConnection = cn;
			context.isInTransaction = false;
			runSync(context, context.sqlConnection, sql, params, callback);
		});
		return;
	}
	runSync(context, context.sqlConnection, sql, params, callback);
}

function createConnection(callback) { 
	var cn =  new mssql.Connection(config, function (err){
		if (err) {
			console.log(err);
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
	if (context.dbQueue.length == 0){
		context.dbRunning = false;
		return;	
	}
	context.dbRunning = true;
	run(context.isInTransaction ? context.sqlTransaction : context.sqlConnection, context.dbQueue[0].sql, context.dbQueue[0].params, function(err, recordset){
		context.dbQueue[0].callback(err, recordset);
		if (err) {
			context.dbRunning = false;
			console.log(err);
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
	var request = new mssql.Request(cn);
	if (params instanceof Array) params.forEach(function (item){
		request.input(item.name, item.value);
	});
	request.query(sql, function (err, recordset){
		if (err) {
			console.log(err);
			throw new Error(err);
		}
		callback(err, recordset);
	});
}

function isInTransaction(context, callback){
	return context.isInTransaction != undefined && context.isInTransaction == true;
}

function beginTran(context, callback){
	createConnection(function(cn){
		context.sqlConnection = cn;
		context.sqlTransaction = context.sqlConnection .transaction();
		context.sqlTransaction.begin(function(){
			context.isInTransaction = true;
			callback();
		});
	});
}

function commitTran(context, callback){
	context.sqlTransaction.commit(function(err){
		context.isInTransaction = false;
		if (err) {
			console.log(err);
			throw new Error(err);
		}
		callback(err);
	});
}

function rollbackTran(context, callback){
	context.sqlTransaction.rollback(function(err){
		context.isInTransaction = false;
		if (err) {
			console.log(err);
			throw new Error(err);
		}
		callback(err);
	});
}
