/*

Object:			Stored procedure 
Author:			Nebiyu Sahlu
Script Date:	June, 2020
Description:	This procedure creates a Data Mart in star schema model and loads data from the source staging database
				into the 'Fact' table and uses SCD type 2 to load data into the dimension tables.  
			
*/

USE [Retail_Sites_DM]
GO

CREATE PROC [dbo].[usp_Load_Retail_Sites_DM]
AS
BEGIN

SET NOCOUNT ON								

IF NOT EXISTS 
		( 
			SELECT SCHEMA_NAME 
			FROM   INFORMATION_SCHEMA.SCHEMATA
			WHERE  SCHEMA_NAME = 'Dim' 
		)

BEGIN
	EXEC sp_executesql N'CREATE SCHEMA Dim'
END

IF NOT EXISTS 
		(
			SELECT  SCHEMA_NAME
			FROM	INFORMATION_SCHEMA.SCHEMATA
			WHERE	SCHEMA_NAME = 'Fact'
		)
BEGIN
	EXECUTE sp_executesql N'CREATE SCHEMA Fact'
END

--===================== Dimension Table 'Customers'=================================


IF NOT EXISTS (SELECT * FROM SYSOBJECTS where name='Customers' and xtype='U')
BEGIN
CREATE TABLE Dim.Customers 
						( 
						 Customer_Key int primary key identity(1,1),
						 Customer_ID nvarchar(25),
						 Customer_Name varchar(25),
						 IsCurrent BIT DEFAULT(1),
						 EndDate datetime DEFAULT '12/31/2999',
						 UpdatedBy nvarchar(50) Constraint CustomerDim_UpdatedBy default suser_sname(),
						 CustomersBinaryCheck int not null

						 )
END

DECLARE @AuditCustomers TABLE
				
						( 
						 
						 Customer_ID nvarchar(25),
						 Customer_Name varchar(25),
						 IsCurrent BIT DEFAULT(1),
						 EndDate datetime DEFAULT '12/31/2999',
						 UpdatedBy nvarchar(50)  default suser_sname(),
						 CustomersBinaryCheck int not null

						 )


INSERT		INTO @AuditCustomers
SELECT		Customer_ID,Customer_Name,1,'12/31/2999',suser_sname(),CustomersBinaryCheck
FROM		(
MERGE		Dim.Customers		
			AS TARGET
USING		( 
				SELECT  DISTINCT A.Customer_ID, A.Customer_Name,
						BINARY_CHECKSUM(Customer_ID, Customer_Name) AS CustomersBinaryCheck
				FROM	[Stg_Retail_Sites].[dbo].[Stg_Customers] A 
			) 
			AS SOURCE
			ON    TARGET.Customer_ID = SOURCE.Customer_ID
WHEN		MATCHED AND 
			TARGET.CustomersBinaryCheck <> SOURCE.CustomersBinaryCheck
AND			TARGET.IsCurrent = 1
THEN
UPDATE 
SET			IsCurrent = 0,
			EndDate = GETDATE()
WHEN NOT	MATCHED 
THEN
INSERT		( 
			Customer_ID,Customer_Name,CustomersBinaryCheck
			)
VALUES		(
			SOURCE.Customer_ID,SOURCE.Customer_Name,SOURCE.CustomersBinaryCheck
			) 
OUTPUT 
			$Action Action_out,
			SOURCE.Customer_ID,
			SOURCE.Customer_Name,
			SOURCE.CustomersBinaryCheck
			) AS Merge_Out
WHERE		MERGE_OUT.Action_Out = 'UPDATE';

INSERT INTO Dim.Customers
SELECT		[Customer_ID], 
			[Customer_Name], [IsCurrent], [EndDate], [UpdatedBy], [CustomersBinaryCheck]
FROM		@AuditCustomers


--SELECT *
--FROM [Dim].[Customers]



--====================== Dimension Table 'Customers'=============================================
IF NOT EXISTS ( SELECT * FROM SYSOBJECTS where name='Products' and xtype='U') 

BEGIN

CREATE  TABLE Dim.Products
						(
						Product_Key int not null primary key identity(1,1) ,
						Product_ID nvarchar(25), 
						Product_Name nvarchar(25), 
						Cost money,
						Original_Sale_Price money, 
						Discount money, 
						Current_Price money, 
						Taxes money,
						IsCurrent BIT DEFAULT 1,
						EndDate Datetime DEFAULT '12/31/2999',
						UpdatedBy nvarchar(50) Constraint ProductDim_UpdatedBy default suser_sname(),
						ProductsBinaryCheck int not null
						)
END

DECLARE @AuditProducts TABLE 

					( 
						Product_ID nvarchar(25), 
						Product_Name nvarchar(25), 
						Cost money,
						Original_Sale_Price money, 
						Discount money, 
						Current_Price money, 
						Taxes money,
						IsCurrent BIT DEFAULT 1,
						EndDate Datetime DEFAULT '12/31/2999',
						UpdatedBy nvarchar(50)  default suser_sname(),
						ProductsBinaryCheck int not null
						)

INSERT INTO @AuditProducts 
SELECT		Product_ID,Product_Name,Cost,Original_Sale_Price,Discount,Current_Price,Taxes,
			1,'12/31/2999',suser_sname(),ProductsBinaryCheck
FROM
( 
MERGE		Dim.Products  AS TARGET
USING	( 
			SELECT DISTINCT [Product_ID], [Product_Name], [Cost], 
						[Original_Sale_Price], [Discount], [Current_Price], [Taxes],
						Binary_CheckSum([Product_ID], [Product_Name],[Current_Price]) 
						AS ProductsBinaryCheck
			FROM			[Stg_Retail_Sites].[dbo].[Stg_Products]
		)	AS SOURCE
ON			TARGET.Product_ID = SOURCE.Product_ID
WHEN	NOT MATCHED 
THEN
INSERT		(
			Product_ID,Product_Name,Cost,Original_Sale_Price,Discount,Current_Price,Taxes,
			ProductsBinaryCheck
			)
VALUES			(
					SOURCE.[Product_ID], SOURCE.[Product_Name], SOURCE.[Cost], 
					SOURCE.[Original_Sale_Price], SOURCE.[Discount], 
					SOURCE.[Current_Price],SOURCE.[Taxes],
					SOURCE.ProductsBinaryCheck
				)
WHEN MATCHED AND
			TARGET.ProductsBinaryCheck <> SOURCE.ProductsBinaryCheck
			AND IsCurrent = 1
THEN
UPDATE
SET        	IsCurrent = 0,
			EndDate = getdate()
OUTPUT
		
			$ACTION Action_Out,
			SOURCE.[Product_ID], SOURCE.[Product_Name], SOURCE.[Cost], 
			SOURCE.[Original_Sale_Price], SOURCE.[Discount], 
			SOURCE.[Current_Price],SOURCE.[Taxes],SOURCE.ProductsBinaryCheck
				) As MERGE_OUT
WHERE		MERGE_OUT.Action_Out = 'Update' ;

INSERT	INTO Dim.Products
SELECT	*
FROM	@AuditProducts

--SELECT *
--FROM	Dim.Products

--===================Dimension Table 'Locations'==========================================


IF NOT EXISTS (SELECT * FROM SYSOBJECTS where name='Locations' and xtype='U') 

BEGIN

CREATE TABLE Dim.Locations 
							(
							Location_Key int primary key identity(1,1),
							Location_ID nvarchar(25) not null, 
							[Name] nvarchar(25), 
							[County] nvarchar(255), 
							[State_Code] varchar(25), 
							[State] varchar(25), 
							[Zip_Codes] nvarchar(max) , 
							[Type] varchar(25), 
							[Latitude] decimal, 
							[Longitude] decimal, 
							[Area_Code] int, 
							[Population] int, 
							[Households] int, 
							[Median_Income] money,
							IsCurrent bit default 1,			  
							Enddate datetime default '12/31/2999',
							UpdatedBy nvarchar(50) Constraint LocationDim_UpdatedBy default suser_sname(),
							LocationsBinaryCheck int not null
							)

END

DECLARE @AuditLocations TABLE 
						( 
							
							Location_ID nvarchar(25) not null, 
							[Name] nvarchar(25), 
							[County] nvarchar(255), 
							[State_Code] varchar(25), 
							[State] varchar(25), 
							[Zip_Codes] nvarchar(max) , 
							[Type] varchar(25), 
							[Latitude] decimal, 
							[Longitude] decimal, 
							[Area_Code] int, 
							[Population] int, 
							[Households] int, 
							[Median_Income] money,
							IsCurrent bit default 1,			  
							Enddate datetime default '12/31/2999',
							UpdatedBy nvarchar(50)  default suser_sname(),
							LocationsBinaryCheck int not null
							)

INSERT INTO		@AuditLocations
SELECT			[Location_ID], [Name], [County], [State_Code], [State], [Zip_Codes], [Type], [Latitude], [Longitude], 
				[Area_Code], [Population], [Households], 
				[Median_Income],1,'12/31/2999',suser_sname(),  [LocationsBinaryCheck]
FROM
		(
			MERGE INTO Dim.Locations  AS TARGET
			USING
		( 
			SELECT DISTINCT [Location_ID], [Name], [County], [State_Code], [State], 
					    [Zip_Codes], [Type], [Latitude], [Longitude], [Area_Code], 
					    [Population], [Households], [Median_Income],
						BINARY_CHECKSUM([Location_ID],[Population],
						[Households], [Median_Income]) AS LocationsBinaryCheck
		FROM           [Stg_Retail_Sites].[dbo].[Stg_Locations]
	) AS SOURCE
ON TARGET.Location_ID = SOURCE.Location_ID
WHEN NOT MATCHED 
THEN 
INSERT  (
		[Location_ID], [Name], [County], [State_Code], 
		[State], [Zip_Codes], [Type], [Latitude], 
		[Longitude], [Area_Code], [Population], 
		[Households], [Median_Income],LocationsBinaryCheck
		)
VALUES (
		SOURCE.[Location_ID], SOURCE.[Name], SOURCE.[County], SOURCE.[State_Code], 
		SOURCE.[State], SOURCE.[Zip_Codes],
		SOURCE.[Type], SOURCE.[Latitude], SOURCE.[Longitude], SOURCE.[Area_Code],
		SOURCE.[Population], SOURCE.[Households], SOURCE.[Median_Income],
		SOURCE.LocationsBinaryCheck
		)
WHEN MATCHED 
AND		TARGET.LocationsBinaryCheck <> SOURCE.LocationsBinaryCheck
AND		IsCurrent = 1
THEN
UPDATE
SET		IsCurrent = 0,
		EndDate = Getdate()
OUTPUT
			$Action Action_Out,
			SOURCE.[Location_ID], SOURCE.[Name], SOURCE.[County], SOURCE.[State_Code], 
			SOURCE.[State], SOURCE.[Zip_Codes],
			SOURCE.[Type], SOURCE.[Latitude], SOURCE.[Longitude], SOURCE.[Area_Code],
			SOURCE.[Population], SOURCE.[Households], SOURCE.[Median_Income],
			SOURCE.LocationsBinaryCheck
		) AS Merger_Out
WHERE	Merger_Out.Action_Out = 'Update'
;
INSERT	INTO Dim.Locations
SELECT	*
FROM	@AuditLocations


--=================Dimension Table 'Sales People'=============================================

IF NOT EXISTS (SELECT * FROM SYSOBJECTS where name='SalesPeople' and xtype='U') 

BEGIN


CREATE TABLE Dim.SalesPeople
							(
							 SalesPerson_Key int primary key identity(1,1),
							 SalesPerson_ID nvarchar(25) not null,
							 SalesPerson_Name varchar(25) not null,
							 IsCurrent bit default 1,
							 EndDate Datetime default '12/31/2999',
							 UpdatedBy nvarchar(50) Constraint SalesPeopleDim_UpdatedBy default suser_sname(),
						     SalesPeopleBinaryCheck int not null
							 )

END

DECLARE @Audit_SalesPeople TABLE 
							(
							 SalesPerson_Key int primary key identity(1,1),
							 SalesPerson_ID nvarchar(25) not null,
							 SalesPerson_Name varchar(25) not null,
							 IsCurrent bit default 1,
							 EndDate Datetime default '12/31/2999',
							 UpdatedBy nvarchar(50)  default suser_sname(),
						     SalesPeopleBinaryCheck int not null
							 )
INSERT INTO		@Audit_SalesPeople
SELECT			SalesPerson_ID,SalesPerson_Name,1,'12/31/2999',suser_sname(),SalesPeopleBinaryCheck
FROM
			(
MERGE	Dim.SalesPeople AS TARGET
USING 
			( 
			SELECT DISTINCT [SalesPerson_Id], [SalesPerson_Name],
			BINARY_CHECKSUM([SalesPerson_Id], [SalesPerson_Name]) AS SalesPeopleBinaryCheck
			FROM	[Stg_Retail_Sites].[dbo].[Stg_SalesPeople]
			) AS SOURCE
ON  TARGET.SalesPerson_Id = SOURCE.SalesPerson_Id
WHEN NOT	MATCHED 
THEN
INSERT		(SalesPerson_Id,SalesPerson_Name,SalesPeopleBinaryCheck)
VALUES		(SOURCE.SalesPerson_Id, SOURCE.SalesPerson_Name,SOURCE.SalesPeopleBinaryCheck)
WHEN	MATCHED 
AND			TARGET.SalesPeopleBinaryCheck <> SOURCE.SalesPeopleBinaryCheck 
AND			IsCurrent = 1
THEN 
UPDATE 
SET  
		IsCurrent = 0,
		EndDate = GETDATE() 

OUTPUT 
$Action Action_Out,
	    SOURCE.SalesPerson_ID,
	    SOURCE.SalesPerson_Name,
		SOURCE.SalesPeopleBinaryCheck
		) AS Merge_Out
WHERE	Merge_Out.Action_Out = 'UPDATE' ;

INSERT INTO 	Dim.SalesPeople
SELECT			SalesPerson_ID,SalesPerson_Name,1,'12/31/2999',suser_sname(),SalesPeopleBinaryCheck
FROM			 @Audit_SalesPeople


/*

SELECT *
FROM	Dim.SalesPeople

*/
--========================Fact table 'Retail_Sales' ==========================================
IF NOT EXISTS (SELECT * FROM SYSOBJECTS where name ='Retail_Sales' and xtype='U') 

BEGIN

CREATE TABLE Fact.Retail_Sales 
								
			(
			Fact_Sales_Key int primary key identity(1,1),
			Customer_Key int Foreign key References [Dim].[Customers]([Customer_Key]),
			Location_Key int Foreign key References [Dim].[Locations] ([Location_Key]),
			Product_Key int Foreign key References [Dim].[Products]([Product_Key]),
			SalesPerson_Key int Foreign key References [Dim].[SalesPeople] (SalesPerson_Key),
			Quantity int,
			Purchase_Date datetime
			 )

END

MERGE  Fact.Retail_Sales AS TARGET 
USING
	(
			SELECT		B.Customer_Key,C.Location_Key,D.Product_Key,E.SalesPerson_Key,
						Quantity,Purchase_Date
			FROM		[Stg_Retail_Sites].[dbo].[Stg_RetailSales] A
			LEFT JOIN	[Dim].[Customers] B
			ON			A.Customer_ID = B.Customer_ID
			LEFT JOIN	[Dim].[Locations] C
			ON			A.Location_ID = C.Location_ID
			LEFT JOIN	[Dim].[Products] D
			ON			A.Product_ID = D.Product_ID
			LEFT JOIN	[Dim].[SalesPeople] E
			ON			A.SalesPerson_ID = E.SalesPerson_ID
	) AS SOURCE
ON			TARGET.Customer_Key = SOURCE.Customer_Key AND
            TARGET.Location_Key = SOURCE.Location_Key AND
		    TARGET.Product_Key =  SOURCE.Product_Key AND
			TARGET.SalesPerson_Key  = SOURCE.SalesPerson_Key 
WHEN NOT MATCHED 
THEN
INSERT
	VALUES (		
			SOURCE.Customer_Key,SOURCE.Location_Key,SOURCE.Product_Key,
			SOURCE.SalesPerson_Key,	Quantity,Purchase_Date
			) ;
		   
END 							


/* EXEC usp_Load_Retail_Sites_DM */
	
GO


