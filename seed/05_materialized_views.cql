-- ============================================================================
-- MATERIALIZED VIEWS - Pre-computed query patterns for efficient access
-- ============================================================================

-- ============================================================================
-- SPECIES VIEWS
-- ============================================================================

-- Species by discovery year (for timeline displays)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.species_by_discovery_year AS
    SELECT * FROM ocean_catalog.species
    WHERE discovered_year IS NOT NULL AND species_id IS NOT NULL
    PRIMARY KEY ((discovered_year), species_id)
WITH comment = 'Species organized by year of discovery';

-- Species by max depth (for habitat research)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.species_by_depth AS
    SELECT * FROM ocean_catalog.species
    WHERE max_depth_meters IS NOT NULL AND species_id IS NOT NULL
    PRIMARY KEY ((max_depth_meters), species_id)
WITH comment = 'Species organized by maximum depth capability';

-- Species by lifespan (for longevity studies)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.species_by_lifespan AS
    SELECT * FROM ocean_catalog.species
    WHERE lifespan_years IS NOT NULL AND species_id IS NOT NULL
    PRIMARY KEY ((lifespan_years), species_id)
WITH comment = 'Species organized by lifespan';

-- ============================================================================
-- SPECIMENS VIEWS
-- ============================================================================

-- Specimens by tag ID (for field lookups)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.specimens_by_tag AS
    SELECT * FROM ocean_catalog.specimens
    WHERE tag_id IS NOT NULL
      AND species_id IS NOT NULL
      AND capture_date IS NOT NULL
      AND specimen_id IS NOT NULL
    PRIMARY KEY ((tag_id), species_id, capture_date, specimen_id)
WITH comment = 'Quick specimen lookup by tag ID';

-- Specimens by health status (for veterinary dashboards)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.specimens_by_health AS
    SELECT * FROM ocean_catalog.specimens
    WHERE health_status IS NOT NULL
      AND species_id IS NOT NULL
      AND capture_date IS NOT NULL
      AND specimen_id IS NOT NULL
    PRIMARY KEY ((health_status), capture_date, species_id, specimen_id)
WITH CLUSTERING ORDER BY (capture_date DESC, species_id ASC, specimen_id DESC)
AND comment = 'Specimens grouped by health status';

-- ============================================================================
-- FACILITY VIEWS
-- ============================================================================

-- Facilities by type (for directory listings)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.facilities_by_type AS
    SELECT * FROM ocean_catalog.facilities
    WHERE facility_type IS NOT NULL AND facility_id IS NOT NULL
    PRIMARY KEY ((facility_type), facility_id)
WITH comment = 'Facilities grouped by type';

-- ============================================================================
-- RESEARCH PROJECT VIEWS
-- ============================================================================

-- Projects by status (for project management)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.projects_by_status AS
    SELECT * FROM ocean_catalog.research_projects
    WHERE status IS NOT NULL
      AND facility_id IS NOT NULL
      AND start_date IS NOT NULL
      AND project_id IS NOT NULL
    PRIMARY KEY ((status), start_date, facility_id, project_id)
WITH CLUSTERING ORDER BY (start_date DESC, facility_id ASC, project_id ASC)
AND comment = 'Projects grouped by status for management views';

-- Projects by principal investigator (for researcher portfolios)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.projects_by_investigator AS
    SELECT * FROM ocean_catalog.research_projects
    WHERE principal_investigator IS NOT NULL
      AND facility_id IS NOT NULL
      AND start_date IS NOT NULL
      AND project_id IS NOT NULL
    PRIMARY KEY ((principal_investigator), start_date, facility_id, project_id)
WITH CLUSTERING ORDER BY (start_date DESC, facility_id ASC, project_id ASC)
AND comment = 'Projects grouped by principal investigator';

-- ============================================================================
-- OBSERVATION VIEWS
-- ============================================================================

-- Observations by observer (for researcher activity tracking)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.observations_by_observer AS
    SELECT * FROM ocean_catalog.observations
    WHERE observer_id IS NOT NULL
      AND specimen_id IS NOT NULL
      AND observation_date IS NOT NULL
      AND observation_time IS NOT NULL
      AND observation_id IS NOT NULL
    PRIMARY KEY ((observer_id), observation_date, observation_time, specimen_id, observation_id)
WITH CLUSTERING ORDER BY (observation_date DESC, observation_time DESC, specimen_id ASC, observation_id DESC)
AND comment = 'Observations grouped by observer for activity tracking';

-- ============================================================================
-- SENSOR VIEWS
-- ============================================================================

-- Sensors by device type (for equipment management)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.sensors_by_type AS
    SELECT * FROM marine_monitoring.sensors
    WHERE device_type IS NOT NULL AND sensor_id IS NOT NULL
    PRIMARY KEY ((device_type), sensor_id)
WITH comment = 'Sensors grouped by device type';

-- Sensors by habitat (for deployment tracking)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.sensors_by_habitat AS
    SELECT * FROM marine_monitoring.sensors
    WHERE habitat_id IS NOT NULL AND sensor_id IS NOT NULL
    PRIMARY KEY ((habitat_id), sensor_id)
WITH comment = 'Sensors grouped by deployment habitat';

-- ============================================================================
-- ALERT VIEWS
-- ============================================================================

-- Alerts by severity (for prioritized response)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.alerts_by_severity AS
    SELECT * FROM marine_monitoring.alerts
    WHERE severity IS NOT NULL
      AND alert_type IS NOT NULL
      AND alert_date IS NOT NULL
      AND alert_time IS NOT NULL
      AND alert_id IS NOT NULL
    PRIMARY KEY ((severity), alert_date, alert_time, alert_type, alert_id)
WITH CLUSTERING ORDER BY (alert_date DESC, alert_time DESC, alert_type ASC, alert_id DESC)
AND comment = 'Alerts prioritized by severity';

-- Alerts by sensor (for device troubleshooting)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.alerts_by_sensor AS
    SELECT * FROM marine_monitoring.alerts
    WHERE sensor_id IS NOT NULL
      AND alert_type IS NOT NULL
      AND alert_date IS NOT NULL
      AND alert_time IS NOT NULL
      AND alert_id IS NOT NULL
    PRIMARY KEY ((sensor_id), alert_date, alert_time, alert_type, alert_id)
WITH CLUSTERING ORDER BY (alert_date DESC, alert_time DESC, alert_type ASC, alert_id DESC)
AND comment = 'Alerts grouped by source sensor';

-- Unresolved alerts (for operations dashboard)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.unresolved_alerts AS
    SELECT * FROM marine_monitoring.alerts
    WHERE resolved IS NOT NULL
      AND alert_type IS NOT NULL
      AND alert_date IS NOT NULL
      AND alert_time IS NOT NULL
      AND alert_id IS NOT NULL
    PRIMARY KEY ((resolved), alert_date, alert_time, alert_type, alert_id)
WITH CLUSTERING ORDER BY (alert_date DESC, alert_time DESC, alert_type ASC, alert_id DESC)
AND comment = 'Alerts filtered by resolution status';

-- ============================================================================
-- EXPEDITION VIEWS
-- ============================================================================

-- Expeditions by status (for planning)
CREATE MATERIALIZED VIEW IF NOT EXISTS marine_monitoring.expeditions_by_status AS
    SELECT * FROM marine_monitoring.expeditions
    WHERE status IS NOT NULL AND expedition_id IS NOT NULL
    PRIMARY KEY ((status), expedition_id)
WITH comment = 'Expeditions grouped by status';

-- ============================================================================
-- HABITAT VIEWS
-- ============================================================================

-- Habitats by protection level (for conservation reporting)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_catalog.habitats_by_protection AS
    SELECT * FROM ocean_catalog.habitats
    WHERE protection_level IS NOT NULL
      AND region_code IS NOT NULL
      AND habitat_id IS NOT NULL
    PRIMARY KEY ((protection_level), region_code, habitat_id)
WITH comment = 'Habitats grouped by protection status';

-- ============================================================================
-- ARCHIVE VIEWS
-- ============================================================================

-- Historical sightings by matched species (for species history research)
CREATE MATERIALIZED VIEW IF NOT EXISTS ocean_archive.sightings_by_species AS
    SELECT * FROM ocean_archive.historical_sightings
    WHERE matched_species_id IS NOT NULL
      AND decade IS NOT NULL
      AND sighting_date IS NOT NULL
      AND sighting_id IS NOT NULL
    PRIMARY KEY ((matched_species_id), sighting_date, decade, sighting_id)
WITH CLUSTERING ORDER BY (sighting_date ASC, decade ASC, sighting_id ASC)
AND comment = 'Historical sightings grouped by matched species';
