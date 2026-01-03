-- ============================================================================
-- TABLES - Comprehensive coverage of all ScyllaDB data types and features
-- ============================================================================

-- ============================================================================
-- OCEAN_CATALOG KEYSPACE - Main data models
-- ============================================================================

-- Octopus Species Catalog
-- Demonstrates: All primitive types, UDTs, collections, static columns, comments
CREATE TABLE IF NOT EXISTS ocean_catalog.species (
    species_id uuid,
    -- Taxonomy and classification
    taxonomy frozen<taxonomy>,
    scientific_name text,
    -- Discovery information
    discovered_year smallint,
    discoverer_name text,
    type_locality text,
    -- Physical characteristics
    avg_measurements frozen<body_measurements>,
    coloration frozen<color_pattern>,
    -- Biological traits (using various numeric types)
    lifespan_years tinyint,
    max_depth_meters int,
    min_depth_meters int,
    typical_weight_kg float,
    max_weight_kg float,
    intelligence_score smallint,
    brain_to_body_ratio double,
    num_hearts tinyint,
    num_brains tinyint,
    -- Diet and reproduction
    diet frozen<list<frozen<diet_item>>>,
    reproduction frozen<reproduction_info>,
    -- Conservation
    conservation frozen<conservation_status>,
    -- Behavioral traits
    is_nocturnal boolean,
    is_venomous boolean,
    can_change_color boolean,
    can_use_tools boolean,
    known_behaviors frozen<set<text>>,
    -- Media and references
    photo_urls frozen<list<text>>,
    video_urls frozen<list<text>>,
    scientific_papers frozen<map<text, text>>,
    -- Binary data example
    genome_sample blob,
    -- Metadata
    created_at timestamp,
    updated_at timestamp,
    created_by text,
    data_version int,
    notes text,
    PRIMARY KEY (species_id)
) WITH comment = 'Comprehensive catalog of cephalopod species with full biological data'
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND compression = {'sstable_compression': 'LZ4Compressor'};

-- Species Media Gallery
-- Demonstrates: Static columns (shared across all photos in a species partition)
CREATE TABLE IF NOT EXISTS ocean_catalog.species_media (
    species_id uuid,
    media_id timeuuid,
    -- Static columns - same for all media of this species
    species_name text STATIC,
    species_scientific_name text STATIC,
    total_media_count int STATIC,
    -- Per-media data (clustered)
    media_type text,
    url text,
    thumbnail_url text,
    caption text,
    photographer text,
    location frozen<coordinates>,
    taken_at timestamp,
    uploaded_at timestamp,
    uploaded_by text,
    tags frozen<set<text>>,
    PRIMARY KEY ((species_id), media_id)
) WITH CLUSTERING ORDER BY (media_id DESC)
AND comment = 'Media gallery for species with static species info';

-- Individual Specimens (tagged/tracked octopuses)
-- Demonstrates: Composite partition key, clustering columns, TTL
CREATE TABLE IF NOT EXISTS ocean_catalog.specimens (
    species_id uuid,
    specimen_id timeuuid,
    -- Identification
    tag_id text,
    nickname text,
    -- Physical data at capture
    capture_date date,
    capture_location frozen<coordinates>,
    measurements frozen<body_measurements>,
    -- Genetic data
    genetic_markers frozen<map<text, text>>,
    dna_sequence_id uuid,
    -- Health status
    health_status text,
    last_health_check timestamp,
    known_injuries frozen<list<text>>,
    -- Tracking
    current_location frozen<coordinates>,
    location_updated_at timestamp,
    tracking_device_id text,
    -- Research
    research_project_ids frozen<set<uuid>>,
    observation_count int,
    -- Metadata
    registered_at timestamp,
    registered_by text,
    PRIMARY KEY ((species_id), capture_date, specimen_id)
) WITH CLUSTERING ORDER BY (capture_date DESC, specimen_id DESC)
AND comment = 'Individual specimen records for research tracking'
AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Research Stations and Aquariums
-- Demonstrates: Complex nested UDTs
CREATE TABLE IF NOT EXISTS ocean_catalog.facilities (
    facility_id uuid PRIMARY KEY,
    facility_type text,
    name text,
    details frozen<facility_details>,
    -- Capacity and resources
    tank_count int,
    total_volume_liters bigint,
    species_housed frozen<set<uuid>>,
    specimen_count int,
    -- Staff
    director_name text,
    staff_emails frozen<list<text>>,
    -- Certifications and permits
    certifications frozen<map<text, date>>,
    research_permits frozen<list<text>>,
    -- Status
    is_active boolean,
    founded_date date,
    last_inspection_date date,
    -- Metadata
    created_at timestamp,
    updated_at timestamp
) WITH comment = 'Research facilities, aquariums, and marine stations';

-- Research Projects
-- Demonstrates: Duration type, varint for large numbers
CREATE TABLE IF NOT EXISTS ocean_catalog.research_projects (
    project_id uuid,
    facility_id uuid,
    -- Project details
    title text,
    description text,
    principal_investigator text,
    team_members frozen<list<text>>,
    -- Timeline
    start_date date,
    end_date date,
    estimated_duration duration,
    actual_duration duration,
    -- Funding
    funding_source text,
    budget_usd varint,
    spent_usd varint,
    -- Species studied
    target_species_ids frozen<set<uuid>>,
    specimen_ids frozen<set<timeuuid>>,
    -- Status and results
    status text,
    publications frozen<list<text>>,
    datasets_generated frozen<map<text, text>>,
    -- Metadata
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((facility_id), start_date, project_id)
) WITH CLUSTERING ORDER BY (start_date DESC, project_id ASC)
AND comment = 'Research projects conducted at facilities';

-- Behavioral Observations
-- Demonstrates: Time type, high-cardinality time series pattern
CREATE TABLE IF NOT EXISTS ocean_catalog.observations (
    specimen_id timeuuid,
    observation_date date,
    observation_time time,
    observation_id timeuuid,
    -- Observer info
    observer_id uuid,
    observer_name text,
    -- Location and conditions
    location frozen<coordinates>,
    environment frozen<environment_snapshot>,
    -- Observation details
    behaviors frozen<list<frozen<behavior_observation>>>,
    interaction_with frozen<list<timeuuid>>,
    prey_captured frozen<list<text>>,
    -- Media
    photo_ids frozen<list<uuid>>,
    video_ids frozen<list<uuid>>,
    audio_ids frozen<list<uuid>>,
    -- Notes
    field_notes text,
    -- Quality metrics
    visibility_rating tinyint,
    confidence_level float,
    PRIMARY KEY ((specimen_id, observation_date), observation_time, observation_id)
) WITH CLUSTERING ORDER BY (observation_time DESC, observation_id DESC)
AND comment = 'Detailed behavioral observations of individual specimens'
AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Ocean Habitats
-- Demonstrates: Tuple type, ASCII type
CREATE TABLE IF NOT EXISTS ocean_catalog.habitats (
    region_code ascii,
    habitat_id uuid,
    -- Location
    name text,
    description text,
    coordinates frozen<coordinates>,
    boundary_polygon frozen<list<frozen<tuple<double, double>>>>,
    area_sq_km double,
    -- Depth range
    depth_range frozen<tuple<int, int>>,
    -- Environmental characteristics
    typical_conditions frozen<environment_snapshot>,
    seasonal_variations frozen<map<text, frozen<water_quality>>>,
    -- Biodiversity
    resident_species_ids frozen<set<uuid>>,
    migratory_species_ids frozen<set<uuid>>,
    estimated_octopus_population int,
    -- Threats
    pollution_level tinyint,
    fishing_pressure tinyint,
    climate_impact_score tinyint,
    -- Protected status
    is_protected boolean,
    protection_level text,
    managing_authority text,
    -- Metadata
    surveyed_at timestamp,
    surveyed_by text,
    PRIMARY KEY ((region_code), habitat_id)
) WITH comment = 'Marine habitat zones and their characteristics';

-- ============================================================================
-- MARINE_MONITORING KEYSPACE - IoT and Time Series Data
-- ============================================================================

-- Sensor Devices Registry
CREATE TABLE IF NOT EXISTS marine_monitoring.sensors (
    sensor_id uuid PRIMARY KEY,
    -- Device info
    device_type text,
    specs frozen<sensor_specs>,
    -- Deployment
    deployed_at timestamp,
    deployed_by text,
    deployment_location frozen<coordinates>,
    attached_to_specimen_id timeuuid,
    habitat_id uuid,
    -- Status
    is_active boolean,
    last_ping timestamp,
    battery_level tinyint,
    firmware_version text,
    -- Configuration
    reading_interval_seconds int,
    transmission_interval_seconds int,
    alert_thresholds frozen<map<text, frozen<tuple<double, double>>>>,
    -- Maintenance
    last_maintenance date,
    next_maintenance date,
    maintenance_notes frozen<list<text>>
) WITH comment = 'Registry of deployed monitoring sensors';

-- Sensor Readings (High-volume time series)
-- Demonstrates: Optimal time series pattern for ScyllaDB
CREATE TABLE IF NOT EXISTS marine_monitoring.sensor_readings (
    sensor_id uuid,
    bucket date,
    reading_time timestamp,
    -- Measurements
    water_quality frozen<water_quality>,
    pressure_bar double,
    light_level_lux int,
    sound_level_db float,
    -- Movement (if attached to specimen)
    acceleration_x float,
    acceleration_y float,
    acceleration_z float,
    heading_degrees smallint,
    speed_ms float,
    -- Device status
    battery_percent tinyint,
    signal_strength tinyint,
    gps_accuracy_m float,
    -- Raw data
    raw_payload blob,
    PRIMARY KEY ((sensor_id, bucket), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC)
AND comment = 'Time series sensor data with daily bucketing'
AND default_time_to_live = 2592000
AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Alerts and Events
CREATE TABLE IF NOT EXISTS marine_monitoring.alerts (
    alert_type text,
    alert_date date,
    alert_time timestamp,
    alert_id timeuuid,
    -- Source
    sensor_id uuid,
    specimen_id timeuuid,
    habitat_id uuid,
    -- Alert details
    severity tinyint,
    title text,
    description text,
    triggered_value double,
    threshold_value double,
    -- Location
    location frozen<coordinates>,
    -- Response
    acknowledged boolean,
    acknowledged_by text,
    acknowledged_at timestamp,
    resolved boolean,
    resolved_at timestamp,
    resolution_notes text,
    PRIMARY KEY ((alert_type, alert_date), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id DESC)
AND comment = 'Alert events from monitoring systems';

-- Diving Expeditions
-- Demonstrates: inet type for IP addresses
CREATE TABLE IF NOT EXISTS marine_monitoring.expeditions (
    expedition_id uuid,
    -- Planning
    title text,
    objective text,
    target_habitat_ids frozen<set<uuid>>,
    target_species_ids frozen<set<uuid>>,
    -- Team
    lead_diver_id uuid,
    team_member_ids frozen<list<uuid>>,
    support_vessel text,
    -- Schedule
    planned_start timestamp,
    planned_end timestamp,
    actual_start timestamp,
    actual_end timestamp,
    -- Location
    departure_port text,
    dive_sites frozen<list<frozen<coordinates>>>,
    max_depth_meters int,
    -- Equipment
    equipment_list frozen<map<text, int>>,
    sensor_ids frozen<set<uuid>>,
    -- Results
    status text,
    specimens_observed frozen<list<timeuuid>>,
    new_specimens_tagged frozen<list<timeuuid>>,
    photos_taken int,
    videos_recorded int,
    samples_collected int,
    -- Communication (demonstrates inet type)
    satellite_phone text,
    vessel_ip inet,
    -- Notes
    dive_log text,
    incidents frozen<list<text>>,
    PRIMARY KEY (expedition_id)
) WITH comment = 'Diving expedition records';

-- ============================================================================
-- OCEAN_ANALYTICS KEYSPACE - Aggregations and Reports
-- ============================================================================

-- Species Distribution by Region (pre-aggregated)
CREATE TABLE IF NOT EXISTS ocean_analytics.species_by_region (
    region_code ascii,
    year smallint,
    species_id uuid,
    -- Counts
    specimen_count int,
    observation_count int,
    -- Population estimates
    estimated_population bigint,
    population_density double,
    -- Trends
    population_change_pct float,
    sighting_frequency float,
    -- Geographic spread
    habitat_ids frozen<set<uuid>>,
    depth_range frozen<tuple<int, int>>,
    -- Last update
    computed_at timestamp,
    PRIMARY KEY ((region_code, year), species_id)
) WITH comment = 'Pre-aggregated species distribution data';

-- Research Output Statistics
CREATE TABLE IF NOT EXISTS ocean_analytics.research_stats (
    facility_id uuid,
    year smallint,
    month tinyint,
    -- Publication metrics
    papers_published int,
    citations_received int,
    -- Project metrics
    projects_active int,
    projects_completed int,
    -- Discovery metrics
    new_species_discovered int,
    specimens_tagged int,
    -- Funding
    total_funding_usd bigint,
    -- Computed at
    computed_at timestamp,
    PRIMARY KEY ((facility_id), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC)
AND comment = 'Monthly research statistics per facility';

-- ============================================================================
-- OCEAN_ARCHIVE KEYSPACE - Historical/Archived Data
-- ============================================================================

-- Historical Sightings (pre-digital era records)
CREATE TABLE IF NOT EXISTS ocean_archive.historical_sightings (
    decade smallint,
    sighting_id uuid,
    -- When
    sighting_date date,
    approximate_year smallint,
    date_precision text,
    -- Where
    location_description text,
    approximate_coordinates frozen<coordinates>,
    -- What
    species_description text,
    matched_species_id uuid,
    specimen_count int,
    -- Source
    source_type text,
    source_reference text,
    source_author text,
    source_publication text,
    -- Verification
    verified boolean,
    verified_by text,
    verification_notes text,
    -- Digitization
    digitized_at timestamp,
    digitized_by text,
    original_document_id uuid,
    PRIMARY KEY ((decade), sighting_date, sighting_id)
) WITH CLUSTERING ORDER BY (sighting_date ASC, sighting_id ASC)
AND comment = 'Digitized historical octopus sighting records';

-- Archived Sensor Data (compressed long-term storage)
CREATE TABLE IF NOT EXISTS ocean_archive.sensor_data_archive (
    sensor_id uuid,
    year smallint,
    month tinyint,
    -- Aggregated data
    reading_count int,
    avg_temperature double,
    min_temperature double,
    max_temperature double,
    avg_salinity double,
    avg_depth double,
    -- Compressed raw data
    compressed_readings blob,
    compression_algorithm text,
    original_size_bytes bigint,
    compressed_size_bytes bigint,
    -- Metadata
    archived_at timestamp,
    PRIMARY KEY ((sensor_id, year), month)
) WITH CLUSTERING ORDER BY (month ASC)
AND comment = 'Archived and compressed sensor data'
AND compression = {'sstable_compression': 'ZstdCompressor', 'chunk_length_kb': 64};
