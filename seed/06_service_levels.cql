-- ============================================================================
-- SERVICE LEVELS - ScyllaDB workload prioritization (OSS 6.x+)
-- Note: 'shares' property is Enterprise-only, using timeout only for OSS
-- ============================================================================

-- Premium service level for real-time dashboards and critical operations
CREATE SERVICE LEVEL IF NOT EXISTS sl_realtime
WITH timeout = '5s';

-- Standard service level for regular application queries
CREATE SERVICE LEVEL IF NOT EXISTS sl_standard
WITH timeout = '30s';

-- Analytics service level for reporting and batch queries
CREATE SERVICE LEVEL IF NOT EXISTS sl_analytics
WITH timeout = '2m';

-- Batch/ETL service level for data imports and exports
CREATE SERVICE LEVEL IF NOT EXISTS sl_batch
WITH timeout = '10m';

-- Research service level for scientific queries
CREATE SERVICE LEVEL IF NOT EXISTS sl_research
WITH timeout = '1m';
