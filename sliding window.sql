USE [partition_demo]
GO

--make orderid biginto identity
SELECT TOP 1 *  FROM parttable.ORDERs

SET IDENTITY_INSERT sw.OrdersArchive on
INSERT INTO sw.OrdersArchive (orderid, itemname,qtysold,orderdate)
SELECT *  FROM sw.OrdersCurrent
SET IDENTITY_INSERT sw.OrdersArchive OFF

SELECT MAX(orderid) FROM sw.ordersarchive
--------------------------------------------------------------
-- Orders Archive
--------------------------------------------------------------

USE [partition_demo]
GO

/*
	Set up our initial archive partition function with 9 partitions explicitly defined.
	
	Range Right indicates in which partition the boundry value falls. We want
	January 1st to fall with the rest of the January for example.

	Range Left would indicate that February 1st would be in the same bucket as most 
	of January's data.
*/
CREATE PARTITION FUNCTION [SW_OrdersArchiveDateRangePF](date) AS RANGE RIGHT FOR VALUES 
(
	  '2015-01-01'
	, '2015-02-01'
	, '2015-04-01'
	, '2015-05-01'
	, '2015-06-01'
	, '2015-07-01'
	, '2015-08-01'
	, '2015-09-01'
)
GO
/*
	Now that we've defined our partitions, we need to create a partition scheme so SQL Server
	knows which filegroup(s) to send each partition to.
*/
CREATE PARTITION SCHEME [SW_OrdersArchiveDatePartitionScheme] AS PARTITION [SW_OrdersArchiveDateRangePF] TO 
(
	[PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]
)
GO

--Now We specify which filegroup to use if we create a new partition.
ALTER PARTITION SCHEME [SW_OrdersArchiveDatePartitionScheme] NEXT USED [PRIMARY]
GO


--Do the same for OrdersCurrent
--------------------------------------------------------------
-- Orders Current
--------------------------------------------------------------
CREATE PARTITION FUNCTION [SW_OrdersCurrentDateRangePF](date) AS RANGE RIGHT FOR VALUES 
(
	  '2015-10-01'
	, '2015-11-01'
	, '2015-12-01'
)
GO

CREATE PARTITION SCHEME [SW_OrdersCurrentDatePartitionScheme] AS PARTITION SW_OrdersCurrentDateRangePF ALL TO ([PRIMARY]) 
GO

ALTER PARTITION SCHEME [SW_OrdersCurrentDatePartitionScheme] NEXT USED [PRIMARY]
GO

CREATE CLUSTERED INDEX idx_clst_OrderID ON sw.OrdersCurrent (orderid)
ON SW_OrdersCurrentDatePartitionScheme(OrderDate)
GO

CREATE CLUSTERED INDEX idx_clst_OrderID ON sw.OrdersArchive (orderid)
ON SW_OrdersArchiveDatePartitionScheme(OrderDate) 

ALTER INDEX idx_clst_OrderID ON sw.OrdersArchive REBUILD 
WITH (STATISTICS_INCREMENTAL = ON)

-----------------------------------------------------------------
-----------------------------------------------------------------

--Verify our partitions
SELECT partition_id, p.object_id, s.name AS schema_name, t.name AS table_name, p.index_id, p.partition_number, rows FROM sys.partitions AS p
INNER JOIN sys.tables t ON t.object_id = p.object_id
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE p.object_id in (834102012,770101784)
ORDER BY s.name,t.name,p.index_id

--SELECT MAX(orderdate) FROM sw.orderscurrent

--SELECT * FROM sys.partition_functions AS pf

--SELECT * FROM sys.partition_range_values AS prv
--WHERE prv.function_id = 65539

--SELECT * FROM sys.partition_parameters AS pp

CREATE DATABASE partition_demo__reset_snap on
( NAME = partition_demo, FILENAME = 
'D:\sqldata\backup\partition_demo_reset_snap.ss' )
AS SNAPSHOT OF partition_demo
GO


select top 10 * from sw.OrdersArchive
order by OrderDate desc

select top 10 * from sw.OrdersCurrent
order by OrderDate ASC

truncate table sw.OrdersArchive 
-----------------------------------------------------------------
-----------------------------------------------------------------
--Sliding window
-----------------------------------------------------------------
-----------------------------------------------------------------
ALTER PARTITION SCHEME [SW_OrdersArchiveDatePartitionScheme] NEXT USED [primary]
GO

ALTER PARTITION SCHEME [SW_OrdersCurrentDatePartitionScheme] NEXT USED [PRIMARY]
GO
------------------
ALTER PARTITION FUNCTION [SW_OrdersArchiveDateRangePF] () SPLIT range ('10/1/2015')
GO

ALTER PARTITION FUNCTION [SW_OrdersCurrentDateRangePF] () SPLIT range ('1/1/2016')
GO
------------------
DECLARE @CurrentPartitionID int
SELECT @CurrentPartitionID = $partition.SW_OrdersCurrentDateRangePF('10/1/2015')
SELECT @CurrentPartitionID

DECLARE @ArchivePartitionID int
SELECT @ArchivePartitionID = $partition.SW_OrdersArchiveDateRangePF('10/1/2015')
SELECT @ArchivePartitionID
--2,10
--------------------
ALTER TABLE sw.OrdersCurrent SWITCH PARTITION @CurrentPartitionID TO sw.OrdersArchive PARTITION @ArchivePartitionID
GO
--------------------
ALTER PARTITION FUNCTION SW_OrdersCurrentDateRangePF () MERGE range ('10/1/2015')
go

use master
go
RESTORE DATABASE partition_demo FROM DATABASE_SNAPSHOT = 'partition_demo__reset_snap'
go
use partition_demo
go



