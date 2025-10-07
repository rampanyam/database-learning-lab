/* ================================================
   Table sizes by schema (MB) — detailed breakdown
   - Works on user tables (excludes system tables)
   - Handles partitioned and non-partitioned tables
   - Breaks out DATA vs INDEX vs LOB/ROW_OVERFLOW
   - Sorts by total size (largest first)
   ================================================ */

SET NOCOUNT ON;

WITH base AS (
    SELECT
        t.object_id,
        SchemaName = s.name,
        TableName  = t.name,
        RowCounts  = SUM(CASE WHEN i.index_id IN (0,1) THEN p.rows ELSE 0 END),
        total_pages        = SUM(a.total_pages),
        used_pages         = SUM(a.used_pages),
        data_inrow_pages   = SUM(CASE WHEN a.type = 1 THEN a.data_pages ELSE 0 END), -- IN_ROW_DATA (data only)
        index_inrow_pages  = SUM(CASE WHEN a.type = 1 THEN (a.used_pages - a.data_pages) ELSE 0 END), -- IN_ROW_DATA (index portion)
        lob_pages          = SUM(CASE WHEN a.type = 2 THEN a.total_pages ELSE 0 END), -- LOB_DATA
        row_overflow_pages = SUM(CASE WHEN a.type = 3 THEN a.total_pages ELSE 0 END)  -- ROW_OVERFLOW_DATA
    FROM sys.tables t
    INNER JOIN sys.schemas s       ON s.schema_id = t.schema_id
    INNER JOIN sys.indexes i       ON i.object_id = t.object_id
    INNER JOIN sys.partitions p    ON p.object_id = i.object_id AND p.index_id = i.index_id
    INNER JOIN sys.allocation_units a ON a.container_id = p.partition_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.object_id, s.name, t.name
)
SELECT
    b.SchemaName,
    b.TableName,
    b.RowCounts,
    -- Convert 8 KB pages → MB (8 KB * pages / 1024)
    TotalMB        = CAST(ROUND((b.total_pages        * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    UsedMB         = CAST(ROUND((b.used_pages         * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    UnusedMB       = CAST(ROUND(((b.total_pages - b.used_pages) * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    DataMB         = CAST(ROUND((b.data_inrow_pages   * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    IndexMB        = CAST(ROUND((b.index_inrow_pages  * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    LOB_MB         = CAST(ROUND((b.lob_pages          * 8.0) / 1024, 2) AS NUMERIC(18,2)),
    RowOverflowMB  = CAST(ROUND((b.row_overflow_pages * 8.0) / 1024, 2) AS NUMERIC(18,2))
FROM base b
ORDER BY TotalMB DESC, b.SchemaName, b.TableName;
