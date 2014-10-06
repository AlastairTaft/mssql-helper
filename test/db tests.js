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
			db.createConnection(function(cn){
				db.runSync(contexto, cn, "WAITFOR DELAY '00:00:01'; SELECT getdate() as hora", function (err, recordset){
					fin1 = true;
					hora1 = recordset[0].hora;
					fin2.should.equal(false);
				});
				db.runSync(contexto, cn, 'SELECT getdate() as hora', function (err, recordset){
					fin2 = true;
					hora2 = recordset[0].hora;
					fin1.should.equal(true);
					done();
				});
			});

		});
	});	
	describe('executeQuery', function (){
		it('ejecuta query parametrizado que retorna 1 reg.', function(done){
		    db.executeQuery({}, 'SELECT TOP 1 * FROM test WHERE id = @id', [{name: 'id', value : '1'}], function (err, recordset) {
			  should.not.exist(err);
			  should.exist(recordset);
			  recordset.should.be.an('array');
			  recordset[0].should.be.an('object');
			  recordset.length.should.equal(1);
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
});

