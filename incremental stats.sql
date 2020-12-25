use partition_demo
go


CREATE STATISTICS mystats ON sw.OrdersArchive(OrderDate) 
WITH FULLSCAN, INCREMENTAL = ON;

DBCC SHOW_STATISTICS('sw.OrdersArchive', mystats);

UPDATE STATISTICS sw.OrdersArchive(mystats) 
WITH RESAMPLE on partitions(3 to 5, 7);




