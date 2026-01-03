-- ============================================================================
-- INDEXES - Secondary indexes, local indexes, and collection indexes
-- ============================================================================

-- ============================================================================
-- GLOBAL SECONDARY INDEXES (on non-partition key columns)
-- These create a new distributed data structure for lookups
-- ============================================================================

-- Species lookup by scientific name
CREATE INDEX IF NOT EXISTS idx_species_scientific_name
ON ocean_catalog.species (scientific_name);

-- Species lookup by venomous status
CREATE INDEX IF NOT EXISTS idx_species_venomous
ON ocean_catalog.species (is_venomous);

-- Species lookup by nocturnal status
CREATE INDEX IF NOT EXISTS idx_species_nocturnal
ON ocean_catalog.species (is_nocturnal);

-- Species lookup by tool use capability
CREATE INDEX IF NOT EXISTS idx_species_tool_use
ON ocean_catalog.species (can_use_tools);

-- Facility lookup by type
CREATE INDEX IF NOT EXISTS idx_facilities_type
ON ocean_catalog.facilities (facility_type);

-- Facility lookup by active status
CREATE INDEX IF NOT EXISTS idx_facilities_active
ON ocean_catalog.facilities (is_active);

-- Research project lookup by status
CREATE INDEX IF NOT EXISTS idx_projects_status
ON ocean_catalog.research_projects (status);

-- Research project lookup by principal investigator
CREATE INDEX IF NOT EXISTS idx_projects_pi
ON ocean_catalog.research_projects (principal_investigator);

-- Sensor lookup by device type
CREATE INDEX IF NOT EXISTS idx_sensors_device_type
ON marine_monitoring.sensors (device_type);

-- Sensor lookup by active status
CREATE INDEX IF NOT EXISTS idx_sensors_active
ON marine_monitoring.sensors (is_active);

-- Alert lookup by severity
CREATE INDEX IF NOT EXISTS idx_alerts_severity
ON marine_monitoring.alerts (severity);

-- Alert lookup by resolved status
CREATE INDEX IF NOT EXISTS idx_alerts_resolved
ON marine_monitoring.alerts (resolved);

-- Expedition lookup by status
CREATE INDEX IF NOT EXISTS idx_expeditions_status
ON marine_monitoring.expeditions (status);

-- Habitat lookup by protection status
CREATE INDEX IF NOT EXISTS idx_habitats_protected
ON ocean_catalog.habitats (is_protected);

-- Historical sightings lookup by verified status
CREATE INDEX IF NOT EXISTS idx_historical_verified
ON ocean_archive.historical_sightings (verified);

-- ============================================================================
-- LOCAL SECONDARY INDEXES (on clustering columns - query within partition)
-- More efficient than global indexes when partition key is known
-- ============================================================================

-- Specimens lookup by health status within a species
CREATE INDEX IF NOT EXISTS idx_specimens_health
ON ocean_catalog.specimens (health_status);

-- Specimens lookup by tag ID
CREATE INDEX IF NOT EXISTS idx_specimens_tag
ON ocean_catalog.specimens (tag_id);

-- Specimens lookup by nickname
CREATE INDEX IF NOT EXISTS idx_specimens_nickname
ON ocean_catalog.specimens (nickname);

-- Observations lookup by observer within specimen/date
CREATE INDEX IF NOT EXISTS idx_observations_observer
ON ocean_catalog.observations (observer_id);

-- Alert lookup by acknowledged status within type/date
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged
ON marine_monitoring.alerts (acknowledged);

-- ============================================================================
-- NOTE ON COLLECTION INDEXES
-- ============================================================================
-- Collection indexes (CONTAINS, KEYS(), VALUES(), ENTRIES()) can only be
-- created on non-frozen collections. Since our schema uses frozen collections
-- for data integrity, collection indexes are not demonstrated here.
--
-- To use collection indexes, define columns as:
--   tags set<text>  (non-frozen, allows CONTAINS queries)
-- Instead of:
--   tags frozen<set<text>>  (frozen, no collection index support)
-- ============================================================================
