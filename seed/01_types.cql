-- ============================================================================
-- USER-DEFINED TYPES (UDTs) - Demonstrating nested and complex structures
-- ============================================================================

-- Geographic coordinates with precision
CREATE TYPE IF NOT EXISTS ocean_catalog.coordinates (
    latitude double,
    longitude double,
    depth_meters double,
    accuracy_meters float,
    recorded_at timestamp
);

-- Physical measurements for specimens
CREATE TYPE IF NOT EXISTS ocean_catalog.body_measurements (
    mantle_length_cm decimal,
    total_length_cm decimal,
    weight_kg decimal,
    arm_span_cm decimal,
    eye_diameter_mm float
);

-- Color pattern description
CREATE TYPE IF NOT EXISTS ocean_catalog.color_pattern (
    primary_color text,
    secondary_colors frozen<set<text>>,
    pattern_type text,
    bioluminescent boolean,
    chromatophore_density int
);

-- Taxonomy classification
CREATE TYPE IF NOT EXISTS ocean_catalog.taxonomy (
    kingdom text,
    phylum text,
    class_name text,
    order_name text,
    family text,
    genus text,
    species text,
    subspecies text,
    common_names frozen<list<text>>
);

-- Research station contact info
CREATE TYPE IF NOT EXISTS ocean_catalog.contact_info (
    email text,
    phone text,
    fax text,
    website text,
    social_media frozen<map<text, text>>
);

-- Address for facilities
CREATE TYPE IF NOT EXISTS ocean_catalog.address (
    street_line1 text,
    street_line2 text,
    city text,
    state_province text,
    postal_code text,
    country text,
    coordinates frozen<coordinates>
);

-- Coordinates for marine_monitoring (duplicate for cross-keyspace use)
CREATE TYPE IF NOT EXISTS marine_monitoring.coordinates (
    latitude double,
    longitude double,
    depth_meters double,
    accuracy_meters float,
    recorded_at timestamp
);

-- Coordinates for ocean_archive (duplicate for cross-keyspace use)
CREATE TYPE IF NOT EXISTS ocean_archive.coordinates (
    latitude double,
    longitude double,
    depth_meters double,
    accuracy_meters float,
    recorded_at timestamp
);

-- Equipment specifications
CREATE TYPE IF NOT EXISTS marine_monitoring.sensor_specs (
    manufacturer text,
    model text,
    serial_number text,
    calibration_date date,
    accuracy_range frozen<tuple<double, double>>,
    operating_depth_range frozen<tuple<int, int>>,
    battery_life_hours int,
    waterproof_rating text
);

-- Water quality measurements (used in sensor readings)
CREATE TYPE IF NOT EXISTS marine_monitoring.water_quality (
    temperature_celsius double,
    salinity_ppt double,
    ph_level float,
    dissolved_oxygen_mgl float,
    turbidity_ntu float,
    chlorophyll_ugl float
);

-- Environmental conditions snapshot
CREATE TYPE IF NOT EXISTS marine_monitoring.environment_snapshot (
    water_quality frozen<water_quality>,
    current_speed_ms float,
    current_direction_degrees int,
    visibility_meters int,
    wave_height_meters float,
    weather_conditions text
);

-- Water quality for ocean_catalog (duplicate for cross-keyspace use)
CREATE TYPE IF NOT EXISTS ocean_catalog.water_quality (
    temperature_celsius double,
    salinity_ppt double,
    ph_level float,
    dissolved_oxygen_mgl float,
    turbidity_ntu float,
    chlorophyll_ugl float
);

-- Environmental conditions for ocean_catalog
CREATE TYPE IF NOT EXISTS ocean_catalog.environment_snapshot (
    water_quality frozen<water_quality>,
    current_speed_ms float,
    current_direction_degrees int,
    visibility_meters int,
    wave_height_meters float,
    weather_conditions text
);

-- Nested type demonstrating deep nesting: Research facility with full details
CREATE TYPE IF NOT EXISTS ocean_catalog.facility_details (
    name text,
    facility_type text,
    address frozen<address>,
    contact frozen<contact_info>,
    established_year int,
    staff_count int,
    specializations frozen<set<text>>
);

-- Behavioral observation record
CREATE TYPE IF NOT EXISTS ocean_catalog.behavior_observation (
    behavior_type text,
    duration_seconds int,
    intensity_level tinyint,
    description text,
    associated_behaviors frozen<list<text>>,
    environmental_triggers frozen<map<text, text>>
);

-- Diet composition
CREATE TYPE IF NOT EXISTS ocean_catalog.diet_item (
    prey_type text,
    prey_species text,
    percentage_of_diet float,
    hunting_method text,
    preferred_size_range frozen<tuple<float, float>>
);

-- Reproduction info
CREATE TYPE IF NOT EXISTS ocean_catalog.reproduction_info (
    mating_season_start int,
    mating_season_end int,
    clutch_size_range frozen<tuple<int, int>>,
    incubation_days int,
    parental_care_type text,
    sexual_dimorphism_notes text
);

-- Conservation status details
CREATE TYPE IF NOT EXISTS ocean_catalog.conservation_status (
    iucn_status text,
    population_trend text,
    estimated_population bigint,
    threats frozen<list<text>>,
    conservation_actions frozen<list<text>>,
    last_assessment_date date
);
