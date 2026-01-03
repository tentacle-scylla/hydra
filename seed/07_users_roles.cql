-- ============================================================================
-- USERS AND ROLES - Authentication and authorization setup
-- ============================================================================

-- ============================================================================
-- ROLES (Groups without login capability)
-- ============================================================================

-- Admin role - full access to everything
CREATE ROLE IF NOT EXISTS role_admin
WITH LOGIN = false;

-- Data scientist role - read access to all data, write to analytics
CREATE ROLE IF NOT EXISTS role_data_scientist
WITH LOGIN = false;

-- Research role - read/write access to research-related data
CREATE ROLE IF NOT EXISTS role_researcher
WITH LOGIN = false;

-- Field operator role - manages sensors and monitoring
CREATE ROLE IF NOT EXISTS role_field_operator
WITH LOGIN = false;

-- Read-only role - view access only
CREATE ROLE IF NOT EXISTS role_readonly
WITH LOGIN = false;

-- Curator role - manages the species catalog
CREATE ROLE IF NOT EXISTS role_curator
WITH LOGIN = false;

-- ============================================================================
-- USERS (Roles with login capability)
-- ============================================================================

-- System administrator
CREATE ROLE IF NOT EXISTS admin
WITH PASSWORD = 'admin123'
AND LOGIN = true
AND SUPERUSER = false;

-- Lead data scientist
CREATE ROLE IF NOT EXISTS alice_scientist
WITH PASSWORD = 'science123'
AND LOGIN = true
AND SUPERUSER = false;

-- Marine biologist researcher
CREATE ROLE IF NOT EXISTS bob_researcher
WITH PASSWORD = 'research123'
AND LOGIN = true
AND SUPERUSER = false;

-- Field operations technician
CREATE ROLE IF NOT EXISTS carol_operator
WITH PASSWORD = 'field123'
AND LOGIN = true
AND SUPERUSER = false;

-- Catalog curator
CREATE ROLE IF NOT EXISTS david_curator
WITH PASSWORD = 'curator123'
AND LOGIN = true
AND SUPERUSER = false;

-- Public API service account (read-only)
CREATE ROLE IF NOT EXISTS api_public
WITH PASSWORD = 'api_public_123'
AND LOGIN = true
AND SUPERUSER = false;

-- Internal API service account
CREATE ROLE IF NOT EXISTS api_internal
WITH PASSWORD = 'api_internal_123'
AND LOGIN = true
AND SUPERUSER = false;

-- ETL/Batch processing service account
CREATE ROLE IF NOT EXISTS etl_service
WITH PASSWORD = 'etl_service_123'
AND LOGIN = true
AND SUPERUSER = false;

-- ============================================================================
-- ROLE MEMBERSHIPS (Hierarchy)
-- ============================================================================

-- Admin gets admin role
GRANT role_admin TO admin;

-- Scientists get data scientist role
GRANT role_data_scientist TO alice_scientist;

-- Researchers get researcher role
GRANT role_researcher TO bob_researcher;

-- Researchers also get read-only (inherited permissions)
GRANT role_readonly TO role_researcher;

-- Field operators get field role
GRANT role_field_operator TO carol_operator;

-- Curators get curator role
GRANT role_curator TO david_curator;

-- Public API gets read-only
GRANT role_readonly TO api_public;

-- Internal API gets researcher access
GRANT role_researcher TO api_internal;

-- ETL service gets data scientist role for analytics writes
GRANT role_data_scientist TO etl_service;

-- ============================================================================
-- PERMISSIONS - Read-only role
-- ============================================================================

GRANT SELECT ON KEYSPACE ocean_catalog TO role_readonly;
GRANT SELECT ON KEYSPACE marine_monitoring TO role_readonly;
GRANT SELECT ON KEYSPACE ocean_analytics TO role_readonly;
GRANT SELECT ON KEYSPACE ocean_archive TO role_readonly;

-- ============================================================================
-- PERMISSIONS - Researcher role
-- ============================================================================

-- Full access to catalog for research data entry
GRANT SELECT, MODIFY ON ocean_catalog.species TO role_researcher;
GRANT SELECT, MODIFY ON ocean_catalog.specimens TO role_researcher;
GRANT SELECT, MODIFY ON ocean_catalog.observations TO role_researcher;
GRANT SELECT, MODIFY ON ocean_catalog.research_projects TO role_researcher;
GRANT SELECT ON ocean_catalog.facilities TO role_researcher;
GRANT SELECT ON ocean_catalog.habitats TO role_researcher;

-- Read access to monitoring data
GRANT SELECT ON KEYSPACE marine_monitoring TO role_researcher;

-- ============================================================================
-- PERMISSIONS - Field operator role
-- ============================================================================

-- Full access to monitoring keyspace
GRANT SELECT, MODIFY ON KEYSPACE marine_monitoring TO role_field_operator;

-- Read access to catalog for reference
GRANT SELECT ON ocean_catalog.species TO role_field_operator;
GRANT SELECT ON ocean_catalog.specimens TO role_field_operator;
GRANT SELECT ON ocean_catalog.habitats TO role_field_operator;

-- ============================================================================
-- PERMISSIONS - Data scientist role
-- ============================================================================

-- Full access to analytics keyspace
GRANT ALL ON KEYSPACE ocean_analytics TO role_data_scientist;

-- Read access to all source data
GRANT SELECT ON KEYSPACE ocean_catalog TO role_data_scientist;
GRANT SELECT ON KEYSPACE marine_monitoring TO role_data_scientist;
GRANT SELECT ON KEYSPACE ocean_archive TO role_data_scientist;

-- ============================================================================
-- PERMISSIONS - Curator role
-- ============================================================================

-- Full management of species catalog
GRANT SELECT, MODIFY ON ocean_catalog.species TO role_curator;
GRANT SELECT, MODIFY ON ocean_catalog.habitats TO role_curator;
GRANT SELECT, MODIFY ON ocean_catalog.facilities TO role_curator;

-- Read access to specimens and research
GRANT SELECT ON ocean_catalog.specimens TO role_curator;
GRANT SELECT ON ocean_catalog.research_projects TO role_curator;
GRANT SELECT ON ocean_catalog.observations TO role_curator;

-- Archive management
GRANT SELECT, MODIFY ON KEYSPACE ocean_archive TO role_curator;

-- ============================================================================
-- PERMISSIONS - Admin role
-- ============================================================================

-- Full access to everything
GRANT ALL ON KEYSPACE ocean_catalog TO role_admin;
GRANT ALL ON KEYSPACE marine_monitoring TO role_admin;
GRANT ALL ON KEYSPACE ocean_analytics TO role_admin;
GRANT ALL ON KEYSPACE ocean_archive TO role_admin;

-- ============================================================================
-- ATTACH SERVICE LEVELS TO ROLES
-- ============================================================================

-- Admin and internal services get standard priority
ALTER ROLE admin WITH SERVICE LEVEL = 'sl_standard';
ALTER ROLE api_internal WITH SERVICE LEVEL = 'sl_standard';

-- Researchers get research priority
ALTER ROLE bob_researcher WITH SERVICE LEVEL = 'sl_research';

-- Data scientists get analytics priority
ALTER ROLE alice_scientist WITH SERVICE LEVEL = 'sl_analytics';

-- ETL gets batch priority (lowest)
ALTER ROLE etl_service WITH SERVICE LEVEL = 'sl_batch';

-- Public API gets standard priority
ALTER ROLE api_public WITH SERVICE LEVEL = 'sl_standard';

-- Field operators get realtime priority (for monitoring)
ALTER ROLE carol_operator WITH SERVICE LEVEL = 'sl_realtime';

-- Curators get standard priority
ALTER ROLE david_curator WITH SERVICE LEVEL = 'sl_standard';
