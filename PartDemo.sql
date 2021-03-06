USE [partition_demo]
GO

/****** Object:  View [pv].[ALLOrders]    Script Date: 10/24/2015 3:30:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create view [pv].[ALLOrders]
as
	select * from pv.CurrentMonthOrders
	union ALL
	select * from pv.OrdersArchive
GO

USE [partition_demo]
GO


SELECT * FROM sys.check_constraints AS cc

ALTER TABLE [pv].[OrdersArchive]  WITH CHECK 
ADD  CONSTRAINT [CK_OrdersArchive] CHECK  (([OrderDate]<='10/31/2013'))
GO


ALTER TABLE [pv].[CurrentMonthOrders]  WITH CHECK 
ADD  CONSTRAINT [CK_CurrentMonthOrders] CHECK  (([OrderDate]>='11/2/2013'))
GO

select * from pv.ALLOrders where OrderDate = '10/30/2013'
select * from pv.ALLOrders where OrderDate = '11/2/2013'
select * from pv.ALLOrders where OrderDate = '11/1/2013'

ALTER PARTITION FUNCTION OrderDateRangePF ()
Merge range ('3/1/2013');

--create new range
ALTER PARTITION FUNCTION OrderDateRangePF ()
SPLIT range ('1/1/2014')



CREATE PARTITION FUNCTION [OrderDateRangePF](date) 
AS RANGE RIGHT FOR VALUES 
('1/1/2013'
, '2/1/2013'
, '3/1/2013'
, '4/1/2013'
, '5/1/2013'
, '6/1/2013'
, '7/1/2013'
, '8/1/2013'
, '9/1/2013'
, '10/1/2013'
, '11/1/2013'
, '12/1/2013')
GO

CREATE PARTITION SCHEME [OrderDatePartitionScheme] 
AS 
PARTITION [OrderDateRangePF] ALL TO ([PRIMARY])
GO


select object_name(object_id), * from sys.partitions
where object_id > 100

select object_name(object_id), * from sys.dm_db_partition_stats

create clustered index cl_idx on parttable.OrdersArchive (OrderID)
with (DROP_EXISTING = ON)
ON OrderDatePartitionScheme(OrderDate)

truncate table parttable.ordersarchive

select count(*) from parttable.Orders

select count(*) from parttable.OrdersArchive


select object_name(object_id), * from sys.partitions
WHERE LOWER(OBJECT_NAME(object_id)) = 'orders'
--GROUP by object_id


select object_name(object_id), * from sys.partitions
--group by object_id

select * from sys.dm_db_partition_stats
USE [partition_demo]
GO

/****** Object:  Index [cl_idx]    Script Date: 4/9/2014 8:19:26 PM ******/

USE [partition_demo]
GO

/****** Object:  Index [cl_idx]    Script Date: 4/9/2014 8:30:12 PM ******/
CREATE CLUSTERED INDEX [cl_idx] ON [parttable].[OrdersArchive2]
(
	[OrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 70)
GO

truncate table [parttable].[OrdersArchive]

select object_name(object_id), * from sys.partitions
WHERE LOWER(OBJECT_NAME(object_id)) = 'orders'
AND partition_number = 3

-----------------------------------------------
-----------------------------------------------
alter table parttable.Orders
	switch partition 3 to parttable.OrdersArchive partition 3

select object_name(object_id), * from sys.partitions
WHERE LOWER(OBJECT_NAME(object_id)) = 'orders'
AND partition_number = 3

select object_name(object_id), * from sys.partitions
WHERE LOWER(OBJECT_NAME(object_id)) = 'ordersarchive'
AND partition_number = 3

select count(*) from parttable.Orders

select count(*) from parttable.OrdersArchive

select * from parttable.OrdersArchive
--select * into parttable.OrdersArchive2 from parttable.OrdersArchive


Select * from parttable.orders
where orderdate = '7/3/2013' or OrderDate = '8/22/2013'

-------------------------------------
-- Reset constraints
USE [partition_demo]
GO

ALTER TABLE [pv].[OrdersArchive] DROP CONSTRAINT [CK_OrdersArchive]
GO

ALTER TABLE [pv].[OrdersArchive]  WITH CHECK ADD  CONSTRAINT [CK_OrdersArchive] CHECK  (([OrderDate]<='10/31/2013'))
GO

ALTER TABLE [pv].[OrdersArchive] CHECK CONSTRAINT [CK_OrdersArchive]
GO

USE [partition_demo]
GO

ALTER TABLE [pv].[CurrentMonthOrders] DROP CONSTRAINT [CK_CurrentMonthOrders]
GO

ALTER TABLE [pv].[CurrentMonthOrders]  WITH CHECK ADD  CONSTRAINT [CK_CurrentMonthOrders] CHECK  (([OrderDate]>='11/2/2013'))
GO

ALTER TABLE [pv].[CurrentMonthOrders] CHECK CONSTRAINT [CK_CurrentMonthOrders]
GO




