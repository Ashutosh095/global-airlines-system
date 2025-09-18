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


SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airplanes') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airplanes') 
    AND name = 'createdDate'
);

-- Replace 'DF__airlines__create__3D5E1FD2' with the actual name of your constraint
ALTER TABLE airlines 
DROP CONSTRAINT DF__airlines__create__3D5E1FD2;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airplanes') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airplanes') 
    AND name = 'createdDate'
);

ALTER TABLE airplanes 
DROP CONSTRAINT DF__airplanes__creat__3F466844;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('routes') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('routes') 
    AND name = 'createdDate'
);

ALTER TABLE routes 
DROP CONSTRAINT DF__routes__createdD__412EB0B6;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airports') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airports') 
    AND name = 'createdDate'
);

ALTER TABLE airports 
DROP CONSTRAINT DF__airports__create__4316F928;


ALTER TABLE airlines 
ALTER COLUMN createdDate DATETIME;


ALTER TABLE airplanes 
ALTER COLUMN createdDate DATETIME;

ALTER TABLE airports 
ALTER COLUMN createdDate DATETIME;

ALTER TABLE routes 
ALTER COLUMN createdDate DATETIME;

SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airlines') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airlines') 
    AND name = 'modifiedDate'
);

-- Replace 'DF__airlines__create__3D5E1FD2' with the actual name of your constraint
ALTER TABLE airlines 
DROP CONSTRAINT DF__airlines__modifi__3E52440B;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airplanes') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airplanes') 
    AND name = 'modifiedDate'
);

ALTER TABLE airplanes 
DROP CONSTRAINT DF__airplanes__modif__403A8C7D;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('routes') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('routes') 
    AND name = 'modifiedDate'
);

ALTER TABLE routes 
DROP CONSTRAINT DF__routes__modified__4222D4EF;

-- Find the name of the default constraint
SELECT name 
FROM sys.default_constraints 
WHERE parent_object_id = OBJECT_ID('airports') 
AND parent_column_id = (
    SELECT column_id 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('airports') 
    AND name = 'modifiedDate'
);

ALTER TABLE airports 
DROP CONSTRAINT DF__airports__modifi__440B1D61;


ALTER TABLE airlines 
ALTER COLUMN modifiedDate DATETIME;


ALTER TABLE airplanes 
ALTER COLUMN modifiedDate DATETIME;

ALTER TABLE airports 
ALTER COLUMN modifiedDate DATETIME;

ALTER TABLE routes 
ALTER COLUMN modifiedDate DATETIME;

-- Add default constraints back to airlines table
ALTER TABLE airlines 
ADD CONSTRAINT DF_airlines_createdDate DEFAULT GETDATE() FOR createdDate;
ALTER TABLE airlines 
ADD CONSTRAINT DF_airlines_modifiedDate DEFAULT GETDATE() FOR modifiedDate;

-- Add default constraints back to airplanes table
ALTER TABLE airplanes 
ADD CONSTRAINT DF_airplanes_createdDate DEFAULT GETDATE() FOR createdDate;

ALTER TABLE airplanes 
ADD CONSTRAINT DF_airplanes_modifiedDate DEFAULT GETDATE() FOR modifiedDate;

-- Add default constraints back to routes table
ALTER TABLE routes 
ADD CONSTRAINT DF_routes_createdDate DEFAULT GETDATE() FOR createdDate;

ALTER TABLE routes 
ADD CONSTRAINT DF_routes_modifiedDate DEFAULT GETDATE() FOR modifiedDate;

-- Add default constraints back to airports table
ALTER TABLE airports 
ADD CONSTRAINT DF_routes_createdDate DEFAULT GETDATE() FOR createdDate;

ALTER TABLE airports 
ADD CONSTRAINT DF_routes_modifiedDate DEFAULT GETDATE() FOR modifiedDate;
