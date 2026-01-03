-- ============================================================================
-- SAMPLE DATA - Creative octopus-themed records
-- ============================================================================

-- ============================================================================
-- SPECIES DATA - Real and fictional octopus species
-- ============================================================================

-- Giant Pacific Octopus (Real species - most well-known)
INSERT INTO ocean_catalog.species (
    species_id,
    taxonomy,
    scientific_name,
    discovered_year,
    discoverer_name,
    type_locality,
    lifespan_years,
    max_depth_meters,
    min_depth_meters,
    intelligence_score,
    num_hearts,
    num_brains,
    is_nocturnal,
    is_venomous,
    can_change_color,
    can_use_tools,
    known_behaviors,
    created_at
) VALUES (
    11111111-1111-1111-1111-111111111111,
    {
        kingdom: 'Animalia',
        phylum: 'Mollusca',
        class_name: 'Cephalopoda',
        order_name: 'Octopoda',
        family: 'Enteroctopodidae',
        genus: 'Enteroctopus',
        species: 'dofleini',
        subspecies: null,
        common_names: ['Giant Pacific Octopus', 'North Pacific Giant Octopus', 'GPO']
    },
    'Enteroctopus dofleini',
    1910,
    'Wilhelm Wülker',
    'Pacific Ocean, Japan',
    5,
    2000,
    0,
    95,
    3,
    9,
    true,
    false,
    true,
    true,
    {'hunting', 'camouflage', 'problem-solving', 'tool-use', 'den-building', 'jet-propulsion'},
    toTimestamp(now())
);

-- Blue-Ringed Octopus (Real species - most venomous)
INSERT INTO ocean_catalog.species (
    species_id,
    taxonomy,
    scientific_name,
    discovered_year,
    discoverer_name,
    type_locality,
    lifespan_years,
    max_depth_meters,
    min_depth_meters,
    intelligence_score,
    num_hearts,
    num_brains,
    is_nocturnal,
    is_venomous,
    can_change_color,
    can_use_tools,
    known_behaviors,
    created_at
) VALUES (
    22222222-2222-2222-2222-222222222222,
    {
        kingdom: 'Animalia',
        phylum: 'Mollusca',
        class_name: 'Cephalopoda',
        order_name: 'Octopoda',
        family: 'Octopodidae',
        genus: 'Hapalochlaena',
        species: 'lunulata',
        subspecies: null,
        common_names: ['Blue-Ringed Octopus', 'Greater Blue-Ringed Octopus']
    },
    'Hapalochlaena lunulata',
    1832,
    'Jean René Constant Quoy',
    'Indo-Pacific Ocean',
    2,
    50,
    0,
    75,
    3,
    9,
    true,
    true,
    true,
    false,
    {'warning-display', 'hunting', 'camouflage', 'hiding'},
    toTimestamp(now())
);

-- Mimic Octopus (Real species - master of disguise)
INSERT INTO ocean_catalog.species (
    species_id,
    taxonomy,
    scientific_name,
    discovered_year,
    discoverer_name,
    type_locality,
    lifespan_years,
    max_depth_meters,
    min_depth_meters,
    intelligence_score,
    num_hearts,
    num_brains,
    is_nocturnal,
    is_venomous,
    can_change_color,
    can_use_tools,
    known_behaviors,
    created_at
) VALUES (
    33333333-3333-3333-3333-333333333333,
    {
        kingdom: 'Animalia',
        phylum: 'Mollusca',
        class_name: 'Cephalopoda',
        order_name: 'Octopoda',
        family: 'Octopodidae',
        genus: 'Thaumoctopus',
        species: 'mimicus',
        subspecies: null,
        common_names: ['Mimic Octopus', 'Indonesian Mimic Octopus']
    },
    'Thaumoctopus mimicus',
    1998,
    'Mark Norman',
    'Sulawesi, Indonesia',
    2,
    37,
    0,
    98,
    3,
    9,
    false,
    false,
    true,
    false,
    {'mimicry', 'impersonation', 'color-changing', 'burrowing', 'hunting'},
    toTimestamp(now())
);

-- Dumbo Octopus (Real deep-sea species)
INSERT INTO ocean_catalog.species (
    species_id,
    taxonomy,
    scientific_name,
    discovered_year,
    discoverer_name,
    type_locality,
    lifespan_years,
    max_depth_meters,
    min_depth_meters,
    intelligence_score,
    num_hearts,
    num_brains,
    is_nocturnal,
    is_venomous,
    can_change_color,
    can_use_tools,
    known_behaviors,
    created_at
) VALUES (
    44444444-4444-4444-4444-444444444444,
    {
        kingdom: 'Animalia',
        phylum: 'Mollusca',
        class_name: 'Cephalopoda',
        order_name: 'Octopoda',
        family: 'Opisthoteuthidae',
        genus: 'Grimpoteuthis',
        species: 'sp.',
        subspecies: null,
        common_names: ['Dumbo Octopus', 'Ear-fin Octopus']
    },
    'Grimpoteuthis sp.',
    1932,
    'Grace Evelyn Pickford',
    'Atlantic Ocean, deep sea',
    5,
    7000,
    1000,
    60,
    3,
    9,
    false,
    false,
    false,
    false,
    {'fin-swimming', 'hovering', 'pouncing', 'deep-sea-foraging'},
    toTimestamp(now())
);

-- Common Octopus (Real species - widely studied)
INSERT INTO ocean_catalog.species (
    species_id,
    taxonomy,
    scientific_name,
    discovered_year,
    discoverer_name,
    type_locality,
    lifespan_years,
    max_depth_meters,
    min_depth_meters,
    intelligence_score,
    num_hearts,
    num_brains,
    is_nocturnal,
    is_venomous,
    can_change_color,
    can_use_tools,
    known_behaviors,
    created_at
) VALUES (
    55555555-5555-5555-5555-555555555555,
    {
        kingdom: 'Animalia',
        phylum: 'Mollusca',
        class_name: 'Cephalopoda',
        order_name: 'Octopoda',
        family: 'Octopodidae',
        genus: 'Octopus',
        species: 'vulgaris',
        subspecies: null,
        common_names: ['Common Octopus', 'Atlantic Common Octopus']
    },
    'Octopus vulgaris',
    1797,
    'Georges Cuvier',
    'Mediterranean Sea',
    2,
    200,
    0,
    90,
    3,
    9,
    true,
    false,
    true,
    true,
    {'hunting', 'camouflage', 'den-building', 'escape-artistry', 'learning', 'memory'},
    toTimestamp(now())
);

-- ============================================================================
-- FACILITIES DATA
-- ============================================================================

INSERT INTO ocean_catalog.facilities (
    facility_id,
    facility_type,
    name,
    details,
    tank_count,
    total_volume_liters,
    species_housed,
    specimen_count,
    is_active,
    founded_date,
    created_at
) VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    'research_station',
    'Tentacle Bay Marine Research Center',
    {
        name: 'Tentacle Bay Marine Research Center',
        facility_type: 'research_station',
        address: {
            street_line1: '123 Ocean Drive',
            street_line2: 'Building A',
            city: 'Monterey',
            state_province: 'California',
            postal_code: '93940',
            country: 'USA',
            coordinates: {
                latitude: 36.6002,
                longitude: -121.8947,
                depth_meters: 0,
                accuracy_meters: 5.0,
                recorded_at: null
            }
        },
        contact: {
            email: 'info@tentaclebay.org',
            phone: '+1-831-555-0100',
            fax: '+1-831-555-0101',
            website: 'https://tentaclebay.org',
            social_media: {'twitter': '@tentaclebay', 'instagram': '@tentaclebayresearch'}
        },
        established_year: 1985,
        staff_count: 45,
        specializations: {'cephalopod-behavior', 'deep-sea-research', 'conservation'}
    },
    12,
    500000,
    {11111111-1111-1111-1111-111111111111, 55555555-5555-5555-5555-555555555555},
    28,
    true,
    '1985-06-15',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.facilities (
    facility_id,
    facility_type,
    name,
    details,
    tank_count,
    total_volume_liters,
    species_housed,
    specimen_count,
    is_active,
    founded_date,
    created_at
) VALUES (
    bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,
    'aquarium',
    'Deep Blue Aquarium',
    {
        name: 'Deep Blue Aquarium',
        facility_type: 'aquarium',
        address: {
            street_line1: '456 Harbor Boulevard',
            street_line2: null,
            city: 'Sydney',
            state_province: 'New South Wales',
            postal_code: '2000',
            country: 'Australia',
            coordinates: {
                latitude: -33.8688,
                longitude: 151.2093,
                depth_meters: 0,
                accuracy_meters: 10.0,
                recorded_at: null
            }
        },
        contact: {
            email: 'contact@deepblueaquarium.com.au',
            phone: '+61-2-5555-1234',
            fax: null,
            website: 'https://deepblueaquarium.com.au',
            social_media: {'twitter': '@deepblueaq', 'facebook': 'DeepBlueAquariumSydney'}
        },
        established_year: 1998,
        staff_count: 120,
        specializations: {'public-education', 'reef-systems', 'cephalopod-exhibition'}
    },
    35,
    2000000,
    {22222222-2222-2222-2222-222222222222, 33333333-3333-3333-3333-333333333333, 55555555-5555-5555-5555-555555555555},
    45,
    true,
    '1998-11-22',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.facilities (
    facility_id,
    facility_type,
    name,
    details,
    tank_count,
    total_volume_liters,
    species_housed,
    specimen_count,
    is_active,
    founded_date,
    created_at
) VALUES (
    cccccccc-cccc-cccc-cccc-cccccccccccc,
    'research_vessel',
    'R/V Kraken Explorer',
    {
        name: 'R/V Kraken Explorer',
        facility_type: 'research_vessel',
        address: {
            street_line1: 'Pier 7',
            street_line2: 'Dock B',
            city: 'Woods Hole',
            state_province: 'Massachusetts',
            postal_code: '02543',
            country: 'USA',
            coordinates: {
                latitude: 41.5245,
                longitude: -70.6693,
                depth_meters: 0,
                accuracy_meters: 2.0,
                recorded_at: null
            }
        },
        contact: {
            email: 'kraken@oceanexpeditions.org',
            phone: '+1-508-555-8888',
            fax: null,
            website: 'https://krakenexplorer.org',
            social_media: {'twitter': '@krakenrv', 'youtube': 'KrakenExplorerOfficial'}
        },
        established_year: 2015,
        staff_count: 22,
        specializations: {'deep-sea-exploration', 'rov-operations', 'specimen-collection'}
    },
    4,
    50000,
    {44444444-4444-4444-4444-444444444444},
    3,
    true,
    '2015-03-01',
    toTimestamp(now())
);

-- ============================================================================
-- HABITATS DATA
-- ============================================================================

INSERT INTO ocean_catalog.habitats (
    region_code,
    habitat_id,
    name,
    description,
    coordinates,
    depth_range,
    is_protected,
    protection_level,
    surveyed_at
) VALUES (
    'NPAC',
    dddddddd-dddd-dddd-dddd-dddddddddddd,
    'Monterey Canyon',
    'Deep submarine canyon off the coast of California, home to diverse cephalopod populations including giant Pacific octopus',
    {latitude: 36.7875, longitude: -122.0139, depth_meters: 1500, accuracy_meters: 100, recorded_at: null},
    (50, 3600),
    true,
    'national_marine_sanctuary',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.habitats (
    region_code,
    habitat_id,
    name,
    description,
    coordinates,
    depth_range,
    is_protected,
    protection_level,
    surveyed_at
) VALUES (
    'INDO',
    eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee,
    'Lembeh Strait',
    'Renowned muck diving site in Indonesia, famous for mimic octopus and other unusual cephalopods',
    {latitude: 1.4748, longitude: 125.2341, depth_meters: 25, accuracy_meters: 50, recorded_at: null},
    (5, 40),
    false,
    'none',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.habitats (
    region_code,
    habitat_id,
    name,
    description,
    coordinates,
    depth_range,
    is_protected,
    protection_level,
    surveyed_at
) VALUES (
    'SATL',
    ffffffff-ffff-ffff-ffff-ffffffffffff,
    'Mariana Trench - Challenger Deep',
    'The deepest known point in Earth oceans, potential habitat for undiscovered deep-sea cephalopods',
    {latitude: 11.3493, longitude: 142.1996, depth_meters: 10994, accuracy_meters: 10, recorded_at: null},
    (8000, 10994),
    true,
    'international_waters_protected',
    toTimestamp(now())
);

-- ============================================================================
-- SENSORS DATA
-- ============================================================================

INSERT INTO marine_monitoring.sensors (
    sensor_id,
    device_type,
    specs,
    deployed_at,
    deployed_by,
    deployment_location,
    is_active,
    last_ping,
    battery_level,
    firmware_version,
    reading_interval_seconds,
    transmission_interval_seconds
) VALUES (
    10000000-1000-1000-1000-100000000001,
    'temperature_salinity',
    {
        manufacturer: 'OceanTech Instruments',
        model: 'TS-3000',
        serial_number: 'OT-TS3K-2023-0001',
        calibration_date: '2023-06-15',
        accuracy_range: (0.001, 0.005),
        operating_depth_range: (0, 2000),
        battery_life_hours: 8760,
        waterproof_rating: 'IP68-2000m'
    },
    '2023-07-01 10:30:00',
    'Dr. Sarah Chen',
    {latitude: 36.7875, longitude: -122.0139, depth_meters: 150, accuracy_meters: 5, recorded_at: null},
    true,
    toTimestamp(now()),
    87,
    '3.2.1',
    300,
    3600
);

INSERT INTO marine_monitoring.sensors (
    sensor_id,
    device_type,
    specs,
    deployed_at,
    deployed_by,
    deployment_location,
    is_active,
    last_ping,
    battery_level,
    firmware_version,
    reading_interval_seconds,
    transmission_interval_seconds
) VALUES (
    10000000-1000-1000-1000-100000000002,
    'acoustic_hydrophone',
    {
        manufacturer: 'Marine Acoustics Ltd',
        model: 'HydroListen Pro',
        serial_number: 'MA-HLP-2023-0042',
        calibration_date: '2023-05-20',
        accuracy_range: (10, 200000),
        operating_depth_range: (0, 3000),
        battery_life_hours: 4380,
        waterproof_rating: 'IP68-3000m'
    },
    '2023-08-15 14:00:00',
    'Mike Rodriguez',
    {latitude: 36.7900, longitude: -122.0200, depth_meters: 500, accuracy_meters: 10, recorded_at: null},
    true,
    toTimestamp(now()),
    65,
    '2.1.0',
    1,
    1800
);

INSERT INTO marine_monitoring.sensors (
    sensor_id,
    device_type,
    specs,
    deployed_at,
    deployed_by,
    deployment_location,
    attached_to_specimen_id,
    is_active,
    last_ping,
    battery_level,
    firmware_version,
    reading_interval_seconds,
    transmission_interval_seconds
) VALUES (
    10000000-1000-1000-1000-100000000003,
    'animal_tracker',
    {
        manufacturer: 'WildTrack Systems',
        model: 'CephTrack Mini',
        serial_number: 'WT-CTM-2024-0007',
        calibration_date: '2024-01-10',
        accuracy_range: (1, 10),
        operating_depth_range: (0, 500),
        battery_life_hours: 2190,
        waterproof_rating: 'IP68-500m'
    },
    '2024-02-01 09:15:00',
    'Dr. Emily Watson',
    {latitude: 36.7850, longitude: -122.0100, depth_meters: 45, accuracy_meters: 3, recorded_at: null},
    now(),
    true,
    toTimestamp(now()),
    92,
    '4.0.2',
    60,
    600
);

-- ============================================================================
-- SPECIES MEDIA DATA
-- ============================================================================

INSERT INTO ocean_catalog.species_media (
    species_id, media_id, species_name, species_scientific_name, total_media_count,
    media_type, url, caption, photographer, uploaded_at, tags
) VALUES (
    11111111-1111-1111-1111-111111111111, now(), 'Giant Pacific Octopus', 'Enteroctopus dofleini', 150,
    'photo', 'https://example.com/gpo-hunting.jpg', 'GPO hunting a crab in Monterey Bay',
    'Dr. Sarah Chen', toTimestamp(now()), {'hunting', 'behavior', 'monterey'}
);

INSERT INTO ocean_catalog.species_media (
    species_id, media_id, species_name, species_scientific_name, total_media_count,
    media_type, url, caption, photographer, uploaded_at, tags
) VALUES (
    22222222-2222-2222-2222-222222222222, now(), 'Blue-Ringed Octopus', 'Hapalochlaena lunulata', 75,
    'video', 'https://example.com/bro-warning.mp4', 'Blue rings flashing warning display',
    'Mike Rodriguez', toTimestamp(now()), {'warning-display', 'venomous', 'australia'}
);

-- ============================================================================
-- SPECIMENS DATA
-- ============================================================================

INSERT INTO ocean_catalog.specimens (
    species_id, specimen_id, tag_id, nickname, capture_date, health_status,
    registered_at, registered_by
) VALUES (
    11111111-1111-1111-1111-111111111111, now(), 'GPO-2024-001', 'Neptune',
    '2024-01-15', 'healthy', toTimestamp(now()), 'Dr. Sarah Chen'
);

INSERT INTO ocean_catalog.specimens (
    species_id, specimen_id, tag_id, nickname, capture_date, health_status,
    registered_at, registered_by
) VALUES (
    11111111-1111-1111-1111-111111111111, now(), 'GPO-2024-002', 'Poseidon',
    '2024-02-20', 'healthy', toTimestamp(now()), 'Mike Rodriguez'
);

INSERT INTO ocean_catalog.specimens (
    species_id, specimen_id, tag_id, nickname, capture_date, health_status,
    registered_at, registered_by
) VALUES (
    33333333-3333-3333-3333-333333333333, now(), 'MIM-2024-001', 'Shifty',
    '2024-03-10', 'under_observation', toTimestamp(now()), 'Dr. Emily Watson'
);

INSERT INTO ocean_catalog.specimens (
    species_id, specimen_id, tag_id, nickname, capture_date, health_status,
    registered_at, registered_by
) VALUES (
    55555555-5555-5555-5555-555555555555, now(), 'COM-2023-015', 'Aristotle',
    '2023-11-05', 'healthy', toTimestamp(now()), 'Prof. James Miller'
);

-- ============================================================================
-- RESEARCH PROJECTS DATA
-- ============================================================================

INSERT INTO ocean_catalog.research_projects (
    project_id, facility_id, title, description, principal_investigator,
    team_members, start_date, status, created_at
) VALUES (
    12121212-1212-1212-1212-121212121212,
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    'Cephalopod Cognitive Abilities Study',
    'Long-term study examining problem-solving capabilities and memory in Giant Pacific Octopus',
    'Dr. Sarah Chen',
    ['Dr. Emily Watson', 'Mike Rodriguez', 'Prof. James Miller'],
    '2023-01-15',
    'active',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.research_projects (
    project_id, facility_id, title, description, principal_investigator,
    team_members, start_date, end_date, status, created_at
) VALUES (
    13131313-1313-1313-1313-131313131313,
    bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,
    'Mimic Octopus Behavioral Repertoire',
    'Cataloging and analyzing the full range of mimicry behaviors in Thaumoctopus mimicus',
    'Dr. Mark Norman',
    ['Dr. Lisa Park', 'Tom Anderson'],
    '2022-06-01',
    '2024-05-31',
    'completed',
    toTimestamp(now())
);

INSERT INTO ocean_catalog.research_projects (
    project_id, facility_id, title, description, principal_investigator,
    team_members, start_date, status, created_at
) VALUES (
    14141414-1414-1414-1414-141414141414,
    cccccccc-cccc-cccc-cccc-cccccccccccc,
    'Deep-Sea Cephalopod Discovery Expedition',
    'ROV-assisted exploration of hadal zone for new cephalopod species',
    'Dr. Grace Hopper',
    ['Dr. Alan Turing', 'Ada Lovelace', 'Charles Babbage'],
    '2024-03-01',
    'active',
    toTimestamp(now())
);

-- ============================================================================
-- EXPEDITIONS DATA
-- ============================================================================

INSERT INTO marine_monitoring.expeditions (
    expedition_id, title, objective, lead_diver_id, status,
    planned_start, planned_end, departure_port, max_depth_meters
) VALUES (
    21212121-2121-2121-2121-212121212121,
    'Monterey Canyon Deep Survey 2024',
    'Survey cephalopod populations in the deeper regions of Monterey Canyon',
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    'completed',
    '2024-01-10 08:00:00',
    '2024-01-15 18:00:00',
    'Monterey Harbor',
    1200
);

INSERT INTO marine_monitoring.expeditions (
    expedition_id, title, objective, lead_diver_id, status,
    planned_start, planned_end, departure_port, max_depth_meters
) VALUES (
    22222222-2121-2121-2121-212121212122,
    'Lembeh Strait Mimic Census',
    'Annual census of mimic octopus population in Lembeh Strait',
    bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,
    'active',
    '2024-06-01 06:00:00',
    '2024-06-14 18:00:00',
    'Bitung Port',
    40
);

INSERT INTO marine_monitoring.expeditions (
    expedition_id, title, objective, lead_diver_id, status,
    planned_start, planned_end, departure_port, max_depth_meters
) VALUES (
    23232323-2323-2323-2323-232323232323,
    'Pacific Hadal Zone Exploration',
    'First manned descent to search for cephalopods below 6000m',
    cccccccc-cccc-cccc-cccc-cccccccccccc,
    'planned',
    '2025-01-15 00:00:00',
    '2025-02-28 00:00:00',
    'Guam Naval Base',
    8000
);

-- ============================================================================
-- ALERTS DATA
-- ============================================================================

INSERT INTO marine_monitoring.alerts (
    alert_type, alert_date, alert_time, alert_id, sensor_id,
    severity, title, description, acknowledged, resolved
) VALUES (
    'temperature', '2024-01-20', '2024-01-20 14:32:00', now(),
    10000000-1000-1000-1000-100000000001,
    3, 'Temperature Spike Detected',
    'Water temperature rose 2.5C above normal threshold',
    true, true
);

INSERT INTO marine_monitoring.alerts (
    alert_type, alert_date, alert_time, alert_id, sensor_id,
    severity, title, description, acknowledged, resolved
) VALUES (
    'battery', '2024-02-15', '2024-02-15 09:00:00', now(),
    10000000-1000-1000-1000-100000000002,
    2, 'Low Battery Warning',
    'Sensor battery at 15%, replacement needed within 30 days',
    true, false
);

INSERT INTO marine_monitoring.alerts (
    alert_type, alert_date, alert_time, alert_id, sensor_id,
    severity, title, description, acknowledged, resolved
) VALUES (
    'movement', '2024-03-01', '2024-03-01 22:15:00', now(),
    10000000-1000-1000-1000-100000000003,
    1, 'Unusual Movement Pattern',
    'Tagged specimen showing erratic movement - possible predator encounter',
    false, false
);

-- ============================================================================
-- SENSOR READINGS DATA
-- ============================================================================

INSERT INTO marine_monitoring.sensor_readings (
    sensor_id, bucket, reading_time, water_quality, pressure_bar, light_level_lux, battery_percent
) VALUES (
    10000000-1000-1000-1000-100000000001, '2024-03-15', '2024-03-15 10:00:00',
    {temperature_celsius: 12.5, salinity_ppt: 35.2, ph_level: 8.1, dissolved_oxygen_mgl: 7.2, turbidity_ntu: 2.1, chlorophyll_ugl: 1.5},
    15.2, 850, 87
);

INSERT INTO marine_monitoring.sensor_readings (
    sensor_id, bucket, reading_time, water_quality, pressure_bar, light_level_lux, battery_percent
) VALUES (
    10000000-1000-1000-1000-100000000001, '2024-03-15', '2024-03-15 10:05:00',
    {temperature_celsius: 12.4, salinity_ppt: 35.2, ph_level: 8.1, dissolved_oxygen_mgl: 7.1, turbidity_ntu: 2.2, chlorophyll_ugl: 1.6},
    15.3, 820, 87
);

INSERT INTO marine_monitoring.sensor_readings (
    sensor_id, bucket, reading_time, water_quality, pressure_bar, light_level_lux, battery_percent
) VALUES (
    10000000-1000-1000-1000-100000000002, '2024-03-15', '2024-03-15 10:00:00',
    {temperature_celsius: 8.2, salinity_ppt: 35.5, ph_level: 8.0, dissolved_oxygen_mgl: 6.8, turbidity_ntu: 1.5, chlorophyll_ugl: 0.8},
    50.5, 50, 65
);

-- ============================================================================
-- ANALYTICS DATA
-- ============================================================================

INSERT INTO ocean_analytics.species_by_region (
    region_code, year, species_id, specimen_count, observation_count,
    estimated_population, computed_at
) VALUES (
    'NPAC', 2024, 11111111-1111-1111-1111-111111111111,
    45, 1250, 50000, toTimestamp(now())
);

INSERT INTO ocean_analytics.species_by_region (
    region_code, year, species_id, specimen_count, observation_count,
    estimated_population, computed_at
) VALUES (
    'INDO', 2024, 33333333-3333-3333-3333-333333333333,
    28, 890, 15000, toTimestamp(now())
);

INSERT INTO ocean_analytics.research_stats (
    facility_id, year, month, papers_published, projects_active, specimens_tagged, computed_at
) VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 2024, 1, 3, 5, 12, toTimestamp(now())
);

INSERT INTO ocean_analytics.research_stats (
    facility_id, year, month, papers_published, projects_active, specimens_tagged, computed_at
) VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 2024, 2, 2, 5, 8, toTimestamp(now())
);

-- ============================================================================
-- ARCHIVE DATA
-- ============================================================================

INSERT INTO ocean_archive.historical_sightings (
    decade, sighting_id, sighting_date, approximate_year, location_description,
    species_description, matched_species_id, verified, source_type, source_author
) VALUES (
    1950, uuid(), '1954-07-15', 1954,
    'Monterey Bay, California coast',
    'Large octopus estimated 4m arm span, reddish-brown coloration',
    11111111-1111-1111-1111-111111111111,
    true, 'scientific_paper', 'Dr. William Beebe'
);

INSERT INTO ocean_archive.historical_sightings (
    decade, sighting_id, sighting_date, approximate_year, location_description,
    species_description, matched_species_id, verified, source_type, source_author
) VALUES (
    1880, uuid(), '1883-03-20', 1883,
    'Mediterranean Sea, near Naples',
    'Common octopus observed in tidepools, noted tool use with coconut shell',
    55555555-5555-5555-5555-555555555555,
    true, 'naturalist_journal', 'Anton Dohrn'
);

INSERT INTO ocean_archive.sensor_data_archive (
    sensor_id, year, month, reading_count, avg_temperature, min_temperature, max_temperature,
    avg_salinity, avg_depth, archived_at
) VALUES (
    10000000-1000-1000-1000-100000000001, 2023, 12,
    8640, 11.5, 9.2, 14.8, 35.1, 150.0, toTimestamp(now())
);

-- ============================================================================
-- OBSERVATIONS DATA
-- ============================================================================

INSERT INTO ocean_catalog.observations (
    specimen_id, observation_date, observation_time, observation_id,
    observer_id, observer_name, field_notes, visibility_rating, confidence_level
) VALUES (
    now(), '2024-03-15', '10:30:00.000000000', now(),
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'Dr. Sarah Chen',
    'Observed hunting behavior - subject stalked and captured a Dungeness crab using ambush technique',
    8, 0.95
);

INSERT INTO ocean_catalog.observations (
    specimen_id, observation_date, observation_time, observation_id,
    observer_id, observer_name, field_notes, visibility_rating, confidence_level
) VALUES (
    now(), '2024-03-15', '14:15:00.000000000', now(),
    bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'Mike Rodriguez',
    'Subject displayed color-changing behavior - shifted from mottled brown to bright red when approached',
    9, 0.98
);

INSERT INTO ocean_catalog.observations (
    specimen_id, observation_date, observation_time, observation_id,
    observer_id, observer_name, field_notes, visibility_rating, confidence_level
) VALUES (
    now(), '2024-03-16', '09:00:00.000000000', now(),
    cccccccc-cccc-cccc-cccc-cccccccccccc, 'Dr. Emily Watson',
    'Tool use observation - subject used coconut shell as shelter, carried it while foraging',
    7, 0.92
);

-- ============================================================================
-- COUNTER UPDATES (examples of counter operations)
-- ============================================================================

-- Species sighting counts
UPDATE ocean_analytics.species_sighting_counts
SET total_sightings = total_sightings + 150,
    confirmed_sightings = confirmed_sightings + 142,
    photo_documented = photo_documented + 89
WHERE species_id = 11111111-1111-1111-1111-111111111111
  AND region_code = 'NPAC'
  AND year = 2024
  AND month = 1;

UPDATE ocean_analytics.species_sighting_counts
SET total_sightings = total_sightings + 45,
    confirmed_sightings = confirmed_sightings + 40,
    photo_documented = photo_documented + 35
WHERE species_id = 33333333-3333-3333-3333-333333333333
  AND region_code = 'INDO'
  AND year = 2024
  AND month = 1;

-- Facility activity
UPDATE ocean_analytics.facility_activity
SET visitors = visitors + 1250,
    researchers = researchers + 12,
    feedings_performed = feedings_performed + 84,
    observations_logged = observations_logged + 156
WHERE facility_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
  AND activity_date = '2024-01-15';

-- API usage
UPDATE ocean_analytics.api_usage
SET total_requests = total_requests + 5420,
    successful_requests = successful_requests + 5380,
    failed_requests = failed_requests + 40,
    responses_under_100ms = responses_under_100ms + 4800
WHERE endpoint = '/api/v1/species'
  AND usage_date = '2024-01-15'
  AND hour = 14;

-- Sensor data counters
UPDATE marine_monitoring.sensor_data_counters
SET readings_received = readings_received + 2880,
    readings_processed = readings_processed + 2875,
    readings_failed = readings_failed + 5,
    bytes_received = bytes_received + 1440000,
    alerts_triggered = alerts_triggered + 3
WHERE sensor_id = 10000000-1000-1000-1000-100000000001
  AND bucket_date = '2024-03-15';

UPDATE marine_monitoring.sensor_data_counters
SET readings_received = readings_received + 86400,
    readings_processed = readings_processed + 86350,
    readings_failed = readings_failed + 50,
    bytes_received = bytes_received + 43200000,
    alerts_triggered = alerts_triggered + 12,
    alerts_acknowledged = alerts_acknowledged + 10,
    alerts_resolved = alerts_resolved + 8
WHERE sensor_id = 10000000-1000-1000-1000-100000000002
  AND bucket_date = '2024-03-15';

-- Researcher contributions
UPDATE ocean_analytics.researcher_contributions
SET papers_authored = papers_authored + 3,
    papers_coauthored = papers_coauthored + 7,
    citations_received = citations_received + 142,
    observations_submitted = observations_submitted + 89,
    specimens_tagged = specimens_tagged + 12,
    photos_uploaded = photos_uploaded + 245
WHERE researcher_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
  AND year = 2024;

UPDATE ocean_analytics.researcher_contributions
SET papers_authored = papers_authored + 1,
    papers_coauthored = papers_coauthored + 4,
    citations_received = citations_received + 56,
    observations_submitted = observations_submitted + 156,
    photos_uploaded = photos_uploaded + 312,
    videos_uploaded = videos_uploaded + 28
WHERE researcher_id = bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
  AND year = 2024;

-- Habitat health events
UPDATE ocean_analytics.habitat_health_events
SET event_count = event_count + 5,
    severe_count = severe_count + 1,
    moderate_count = moderate_count + 2,
    minor_count = minor_count + 2,
    species_affected = species_affected + 3
WHERE habitat_id = dddddddd-dddd-dddd-dddd-dddddddddddd
  AND event_type = 'temperature_anomaly'
  AND year = 2024
  AND month = 1;

UPDATE ocean_analytics.habitat_health_events
SET event_count = event_count + 2,
    moderate_count = moderate_count + 1,
    minor_count = minor_count + 1,
    species_affected = species_affected + 8,
    area_affected_sqkm = area_affected_sqkm + 15
WHERE habitat_id = eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee
  AND event_type = 'pollution_event'
  AND year = 2024
  AND month = 2;

-- Page views
UPDATE ocean_analytics.page_views
SET page_views = page_views + 1250,
    unique_visitors = unique_visitors + 890,
    clicks = clicks + 456,
    shares = shares + 23,
    favorites = favorites + 78
WHERE page_path = '/species/giant-pacific-octopus'
  AND view_date = '2024-03-15'
  AND hour = 14;

UPDATE ocean_analytics.page_views
SET page_views = page_views + 2340,
    unique_visitors = unique_visitors + 1560,
    clicks = clicks + 890,
    shares = shares + 45,
    favorites = favorites + 156
WHERE page_path = '/species/blue-ringed-octopus'
  AND view_date = '2024-03-15'
  AND hour = 14;

UPDATE ocean_analytics.page_views
SET page_views = page_views + 567,
    unique_visitors = unique_visitors + 345,
    clicks = clicks + 123,
    scroll_to_bottom = scroll_to_bottom + 234
WHERE page_path = '/research/monterey-canyon-survey'
  AND view_date = '2024-03-15'
  AND hour = 10;

-- Expedition metrics
UPDATE ocean_analytics.expedition_metrics
SET expeditions_planned = expeditions_planned + 8,
    expeditions_completed = expeditions_completed + 6,
    expeditions_cancelled = expeditions_cancelled + 1,
    specimens_found = specimens_found + 45,
    new_species_discovered = new_species_discovered + 2,
    tags_deployed = tags_deployed + 28,
    sensors_deployed = sensors_deployed + 12,
    dive_hours_logged = dive_hours_logged + 480
WHERE expedition_type = 'deep_sea_survey'
  AND year = 2024
  AND quarter = 1;

UPDATE ocean_analytics.expedition_metrics
SET expeditions_planned = expeditions_planned + 15,
    expeditions_completed = expeditions_completed + 14,
    specimens_found = specimens_found + 89,
    tags_deployed = tags_deployed + 56,
    dive_hours_logged = dive_hours_logged + 1200,
    incidents_reported = incidents_reported + 2
WHERE expedition_type = 'shallow_reef_census'
  AND year = 2024
  AND quarter = 1;

UPDATE ocean_analytics.expedition_metrics
SET expeditions_planned = expeditions_planned + 3,
    expeditions_completed = expeditions_completed + 2,
    expeditions_cancelled = expeditions_cancelled + 1,
    specimens_found = specimens_found + 12,
    sensors_deployed = sensors_deployed + 8,
    dive_hours_logged = dive_hours_logged + 96,
    near_misses = near_misses + 1
WHERE expedition_type = 'hadal_exploration'
  AND year = 2024
  AND quarter = 1;
