CREATE OR REPLACE VIEW cmip5_attributes AS
    SELECT
        *
      , split_part(table_id, ' ', 2) as mip_table
      , ('r' || realization ||
         'i' || initialization_method ||
         'p' || physics_version) as ensemble_member
    FROM cmip5_attributes_raw;
GRANT SELECT ON cmip5_attributes TO PUBLIC;

/* Dataset id of this file */
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_file_dataset_link AS
    SELECT
        md_hash
      , md5 ( 'cmip5'
            || '.' || product
            || '.' || institute_id 
            || '.' || model_id 
            || '.' || experiment_id 
            || '.' || frequency
            || '.' || modeling_realm 
            || '.' || mip_table
            || '.' || ensemble_member
            )::uuid as dataset_id
    FROM cmip5_attributes;
GRANT SELECT ON cmip5_file_dataset_link TO PUBLIC;

/* A CMIP5 dataset
 * Like what you find on ESGF, however version information is broken out into its
 * own table
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_dataset AS
    SELECT DISTINCT
        dataset_id
      , experiment_id
      , institute_id
      , model_id
      , modeling_realm
      , frequency
      , mip_table
      , ensemble_member
      , product
    FROM cmip5_attributes
    JOIN cmip5_file_dataset_link ON cmip5_attributes.md_hash = cmip5_file_dataset_link.md_hash;
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_dataset_dataset_id_idx ON cmip5_dataset(dataset_id);
GRANT SELECT ON cmip5_dataset TO PUBLIC;

/* Extra attributes from outside the file */
CREATE TABLE IF NOT EXISTS cmip5_external_attributes (
    md_hash UUID PRIMARY KEY
  , variable_id INTEGER REFERENCES cf_variable
  , date_range INT4RANGE
    );
GRANT SELECT ON cmip5_external_attributes TO PUBLIC;

/* Versions of the dataset */
CREATE TABLE IF NOT EXISTS cmip5_version (
    version_id SERIAL PRIMARY KEY
  , dataset_id UUID NOT NULL
  , version TEXT NOT NULL
  , is_latest BOOLEAN DEFAULT FALSE
  , UNIQUE (dataset_id, version)
    );
GRANT SELECT ON cmip5_version TO PUBLIC; 

/* View for the most recent version */
CREATE OR REPLACE VIEW cmip5_latest_version_link AS
    SELECT DISTINCT ON (dataset_id)
        dataset_id
        version_id
    FROM cmip5_version
    ORDER BY dataset_id, version, is_latest;
GRANT SELECT ON cmip5_latest_version_link TO PUBLIC; 

/* Version/File association */
CREATE TABLE IF NOT EXISTS cmip5_file_version_link (
    md_hash UUID NOT NULL
  , version_id INTEGER REFERENCES cmip5_version
  , PRIMARY KEY (md_hash, version_id)
    );
GRANT SELECT ON cmip5_file_version_link TO PUBLIC; 

/* Warnings attached to a dataset version */
CREATE TABLE IF NOT EXISTS cmip5_warning (
    id SERIAL PRIMARY KEY
  , version_id INTEGER REFERENCES cmip5_version
  , added_by NAME NOT NULL DEFAULT CURRENT_USER
  , added_on DATE NOT NULL DEFAULT CURRENT_DATE
  , warning TEXT NOT NULL
    );
GRANT SELECT ON cmip5_warning TO PUBLIC;

