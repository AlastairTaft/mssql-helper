# mssql-helper

Microsoft Sql Server helper that support transactions per request, query queueing.

For use with Node.js and mssql: https://github.com/patriksimek/node-mssql

## Background

implement a simple layer for mssql supporting queueing commands per request (or per object used as context)

## Install

`npm install mssql-helper`

## Usage

```javascript
var db = require('mssql-helper');

db.config.user= 'test';
db.config.password= 'test';
db.config.server= 'localhost';
db.config.database = 'test';

var context = req; // the request obect or any object that you want to use to share a connection and syncronice commands. 
db.executeQuery(context, 'SELECT TOP 1 * FROM test WHERE id = @id', [{name: 'id', value : '1'}], function (err, recordset) {
  console.log(recordset[0].id);
  console.log(recordset[0].test);
});

```
### Transaction

	var contexto = {}; //can be the request object
	db.beginTran(contexto,function(err){
		console.log('BEGIN TRAN');
	});
	db.executeQuery(contexto, "WAITFOR DELAY '00:00:01'; insert into Table VALUES(1, 'text 1')", function(err, recordset){
		console.log('first INSERT');
	});
	db.executeQuery(contexto, "insert into Table VALUES(2, 'text 2')", function(err, recordset){
		console.log('second INSERT');
	});
	db.commitTran(contexto, function(err){
		console.log('commit');
	});
## How it works


## API

### client.executeQuery(context, sql, [params, callback])

push the command into a queue made in context object and run it. The context param can be the request to use a connection (or transaction) per request. If there is no active connection in the context, a connection is created.

### client.run(cn, sql, [params, callback])

execute a command without queuing.

### client.isInTransaction(context)

return true if it is in transaction.

### client.beginTran(context, [callback])

begin a transaction.

### client.commitTran(context, [callback])

commit a transaction.

### client.rollbackTran(context, [callback])

rollback a transaction.
