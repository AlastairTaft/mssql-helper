-- DROP LOGIN test
-- DROP DATABASE test
USE [master]
GO
create database test
GO 
CREATE LOGIN [test] WITH PASSWORD=N'test', DEFAULT_DATABASE=[test], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE test
go
create user test for login test
go
sp_addrolemember 'db_owner','test'
go
create table test (id int, text varchar(100))
go 
insert into test VALUES (1, 'test')