-- ============================================================================
-- KEYSPACES - Demonstrating all replication strategies
-- ============================================================================

-- SimpleStrategy: Best for single datacenter deployments
-- Used for the main octopus catalog and research data
CREATE KEYSPACE IF NOT EXISTS ocean_catalog
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

-- NetworkTopologyStrategy: Production-ready multi-datacenter setup
-- Used for critical marine monitoring data
CREATE KEYSPACE IF NOT EXISTS marine_monitoring
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}
AND durable_writes = true;

-- Analytics keyspace with different settings
-- For batch processing and aggregations
CREATE KEYSPACE IF NOT EXISTS ocean_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

-- Archive keyspace - could have different compaction/TTL settings
CREATE KEYSPACE IF NOT EXISTS ocean_archive
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = false;
