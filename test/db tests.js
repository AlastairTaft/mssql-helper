var assert = require("assert");
var db = require('../db');
var chai = require('chai')
  , expect = chai.expect
  , should = chai.should();

db.config.user= 'test';
db.config.password= 'test';
db.config.server= 'localhost';
db.config.database = 'test';

describe('mssql db access', function (){
	describe('acceso por contexto debe ser sincronico', function(){
		it('si ejecuto 2 queries con runSync, el segundo debe esperar que termine el primero para ejecutarse', function(done){
			var contexto = {};
			var hora1, hora2;
			var fin1 = false, fin2 = false;
			db.executeQuery(contexto, "WAITFOR DELAY '00:00:01'; SELECT GETDATE() AS hora", function (err, recordset){
				fin1 = true;
				hora1 = recordset[0].hora;
				fin2.should.equal(false);
			});
			db.executeQuery(contexto, ' SELECT GETDATE() AS hora', function (err, recordset){
				fin2 = true;
				hora2 = recordset[0].hora;
				fin1.should.equal(true);
				done();
			});

		});
	});	
	describe('executeQuery', function (){
		beforeEach(function(done){
			db.executeQuery({}, "TRUNCATE TABLE test; insert INTO test VALUES (1,'test')", [] , function(err, recordset){
				done();
			});
	  	});
		it('ejecuta query parametrizado que retorna 1 reg.', function(done){
		    db.executeQuery({}, 'SELECT TOP 1 * FROM test WHERE id = @id', [{name: 'id', value : '1'}], function (err, recordset) {
			  should.not.exist(err);
			  should.exist(recordset);
			  recordset.should.be.an('array');
			  recordset.length.should.equal(1);
			  recordset[0].should.be.an('object');
			  recordset[0].id.should.equal(1);
			  recordset[0].text.should.equal('test');

			  done();
		    });
		});
		it('si el query no tiene parametros en lugar de parametros recibe el callback', function (done){
			db.executeQuery({}, 'SELECT 1 as uno', function(err, recordset){
			  should.not.exist(err);
			  should.exist(recordset);
			  recordset.should.be.an('array');
			  recordset[0].uno.should.equal(1);
			  done();
			});
		});
	});
	describe('transacciones.', function(){
		beforeEach(function(done){
			db.executeQuery({}, "TRUNCATE TABLE test; insert INTO test VALUES (1,'test')", function(err, recordset){
				done();
			});
	  	});
		it('isInTransaction debe retornar true luego de llamar a Begin Tran.', function(done){
			var contexto = {};
			db.beginTran(contexto,function(err){
				should.not.exist(err);
			});
			var text = new Date();
			db.executeQuery(contexto, "insert into test VALUES(1, '" + text + "')", function(err, recordset){
				db.isInTransaction(contexto).should.equal(true);
				db.rollbackTran(contexto, function(){
					db.executeQuery({}, "SELECT * FROM test WHERE text = '" + text + "'", function (err, recordset){
						recordset.length.should.equal(0);
						done();
					});
				});
			});
		});
		it('rollbackTran debe abortar la transaccion', function(done){
			var contexto = {};
			db.beginTran(contexto,function(err){});
			var text = new Date();
			db.executeQuery(contexto, "insert into test VALUES(1, '" + text + "')", function(err, recordset){});
			db.executeQuery(contexto, "insert into test VALUES(2, '" + text + "')", function(err, recordset){
				db.rollbackTran(contexto, function(err){
					should.not.exist(err);
					db.executeQuery({}, "SELECT * FROM test WHERE text = '" + text + "'", function (err, recordset){
						recordset.length.should.equal(0);
						done();
					});
				});
			});
		});
	});
});

