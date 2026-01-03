-- ============================================================================
-- COUNTER TABLES - Distributed counters for real-time metrics
-- ============================================================================

-- Species Sighting Counters (real-time aggregations)
CREATE TABLE IF NOT EXISTS ocean_analytics.species_sighting_counts (
    species_id uuid,
    region_code ascii,
    year smallint,
    month tinyint,
    -- Counters
    total_sightings counter,
    confirmed_sightings counter,
    unconfirmed_sightings counter,
    unique_specimens counter,
    photo_documented counter,
    video_documented counter,
    PRIMARY KEY ((species_id, region_code), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC)
AND comment = 'Real-time sighting counters per species and region';

-- Facility Activity Counters
CREATE TABLE IF NOT EXISTS ocean_analytics.facility_activity (
    facility_id uuid,
    activity_date date,
    -- Daily counters
    visitors counter,
    researchers counter,
    specimens_admitted counter,
    specimens_released counter,
    feedings_performed counter,
    health_checks counter,
    observations_logged counter,
    samples_collected counter,
    PRIMARY KEY ((facility_id), activity_date)
) WITH CLUSTERING ORDER BY (activity_date DESC)
AND comment = 'Daily activity counters for facilities';

-- Sensor Data Volume Counters
CREATE TABLE IF NOT EXISTS marine_monitoring.sensor_data_counters (
    sensor_id uuid,
    bucket_date date,
    -- Data volume counters
    readings_received counter,
    readings_processed counter,
    readings_failed counter,
    bytes_received counter,
    -- Alert counters
    alerts_triggered counter,
    alerts_acknowledged counter,
    alerts_resolved counter,
    PRIMARY KEY ((sensor_id), bucket_date)
) WITH CLUSTERING ORDER BY (bucket_date DESC)
AND comment = 'Sensor data volume and health counters';

-- API Usage Counters (for the catalog service)
CREATE TABLE IF NOT EXISTS ocean_analytics.api_usage (
    endpoint text,
    usage_date date,
    hour tinyint,
    -- Request counters
    total_requests counter,
    successful_requests counter,
    failed_requests counter,
    -- Response time buckets
    responses_under_100ms counter,
    responses_100ms_to_500ms counter,
    responses_500ms_to_1s counter,
    responses_over_1s counter,
    -- Data transfer
    bytes_sent counter,
    bytes_received counter,
    PRIMARY KEY ((endpoint, usage_date), hour)
) WITH CLUSTERING ORDER BY (hour ASC)
AND comment = 'Hourly API usage metrics';

-- Research Contribution Counters
CREATE TABLE IF NOT EXISTS ocean_analytics.researcher_contributions (
    researcher_id uuid,
    year smallint,
    -- Publication counters
    papers_authored counter,
    papers_coauthored counter,
    citations_received counter,
    -- Data contribution counters
    observations_submitted counter,
    specimens_tagged counter,
    photos_uploaded counter,
    videos_uploaded counter,
    datasets_published counter,
    -- Review counters
    papers_reviewed counter,
    data_verified counter,
    PRIMARY KEY ((researcher_id), year)
) WITH CLUSTERING ORDER BY (year DESC)
AND comment = 'Annual contribution metrics for researchers';

-- Habitat Health Counters
CREATE TABLE IF NOT EXISTS ocean_analytics.habitat_health_events (
    habitat_id uuid,
    event_type text,
    year smallint,
    month tinyint,
    -- Event counters
    event_count counter,
    severe_count counter,
    moderate_count counter,
    minor_count counter,
    -- Impact counters
    species_affected counter,
    area_affected_sqkm counter,
    PRIMARY KEY ((habitat_id, event_type), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC)
AND comment = 'Environmental event counters by habitat';

-- Website/App Analytics (if there is a public-facing component)
CREATE TABLE IF NOT EXISTS ocean_analytics.page_views (
    page_path text,
    view_date date,
    hour tinyint,
    -- Traffic counters
    page_views counter,
    unique_visitors counter,
    -- Engagement counters
    avg_time_on_page_ms counter,
    bounce_count counter,
    scroll_to_bottom counter,
    -- Interaction counters
    clicks counter,
    shares counter,
    favorites counter,
    PRIMARY KEY ((page_path, view_date), hour)
) WITH CLUSTERING ORDER BY (hour ASC)
AND comment = 'Hourly page view analytics';

-- Expedition Success Metrics
CREATE TABLE IF NOT EXISTS ocean_analytics.expedition_metrics (
    expedition_type text,
    year smallint,
    quarter tinyint,
    -- Expedition counters
    expeditions_planned counter,
    expeditions_completed counter,
    expeditions_cancelled counter,
    -- Success metrics
    specimens_found counter,
    new_species_discovered counter,
    tags_deployed counter,
    sensors_deployed counter,
    -- Safety counters
    dive_hours_logged counter,
    incidents_reported counter,
    near_misses counter,
    PRIMARY KEY ((expedition_type), year, quarter)
) WITH CLUSTERING ORDER BY (year DESC, quarter DESC)
AND comment = 'Quarterly expedition performance metrics';
