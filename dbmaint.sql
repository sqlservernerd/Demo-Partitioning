use partition_demo
go

truncate table [sw].[OrdersArchive] 
WITH (PARTITIONS (1 TO 3, 5));


CREATE STATISTICS mystatsOrderDate ON sw.orderscurrent(OrderDate) 
WITH FULLSCAN, INCREMENTAL = ON;

UPDATE STATISTICS dbo.sw.orderscurrent(mystatsOrderDate) 
WITH FULLSCAN ON PARTITIONS(1);


------------------------------------------
SELECT
	SCHEMA_NAME(tbl.schema_id) AS schema_name
  , OBJECT_NAME(physstats.object_id) AS table_name
  , idx.name
  , physstats.partition_number
  , physstats.avg_fragmentation_in_percent
FROM
	sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') physstats
	JOIN sys.indexes idx
		ON physstats.object_id = idx.object_id
		   AND physstats.index_id = idx.index_id
	INNER JOIN sys.tables tbl
		ON idx.object_id = tbl.object_id
WHERE
	SCHEMA_NAME(tbl.schema_id) = 'parttable'
	AND OBJECT_NAME(physstats.object_id) = 'OrdersArchive'
ORDER BY
	schema_name
  ,	OBJECT_NAME(physstats.object_id)
  , idx.name
  , physstats.partition_number; 


ALTER INDEX [cl_idx] ON parttable.OrdersArchive 
REBUILD PARTITION=3;
