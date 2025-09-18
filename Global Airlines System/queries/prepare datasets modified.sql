create database global_airlines_system;

use global_airlines_system;

/*adding columns- createdDate and modifiedDate in all tables and adding the today's date to it*/
/*airlines table */

alter table airlines 
add createdDate DATETIME default getdate(),
	modifiedDate DATETIME default getdate();

update airlines
set createdDate=GETDATE(),
	modifiedDate=GETDATE();

select * from airlines;

/*airplanes table */
alter table airplanes 
add createdDate DATETIME default getdate(),
	modifiedDate DATETIME default getdate();

update airplanes
set createdDate=GETDATE(),
	modifiedDate=GETDATE();

	
select * from airplanes;

/*routes table */
alter table routes 
add createdDate DATETIME default getdate(),
	modifiedDate DATETIME default getdate();

update routes
set createdDate=GETDATE(),
	modifiedDate=GETDATE();

select * from routes;

/*airports table */
alter table airports 
add createdDate DATETIME default getdate(),
	modifiedDate DATETIME default getdate();

update airports
set createdDate=GETDATE(),
	modifiedDate=GETDATE();

select * from airports;