/* Generic CF metadata */
CREATE MATERIALIZED VIEW cf_attributes TABLESPACE ceph AS
    SELECT
        md_hash
      , md_json->'attributes'->>'title'       as title
      , md_json->'attributes'->>'institution' as institution
      , md_json->'attributes'->>'source'      as source
    FROM
        metadata 
    WHERE
        md_json->'attributes'->>'Conventions' is not null;
CREATE UNIQUE INDEX ON cf_attributes (md_hash);

/* A variable in a CF-NetCDF file */
CREATE MATERIALIZED VIEW cf_variable TABLESPACE ceph AS
    SELECT
        md5(md_hash || ':' || v.key) as variable_id
      , md_hash
      , v.key as variable_name
      , v.value->'attributes'->>'units' as units
      , v.value->'attributes'->>'long_name' as long_name
      , v.value->'attributes'->>'attributes' as axis
    FROM
        metadata
      , jsonb_each(md_json->'variables') v
    WHERE md_hash IN (SELECT md_hash FROM cf_attributes);
CREATE UNIQUE INDEX ON cf_variable (variable_id);
CREATE INDEX ON cf_variable (md_hash);
CREATE INDEX ON cf_variable (variable_name);

/* Metadata from the file specific to CMIP5 
 * Gets the attributes out of JSON format into a more usable format
 */
CREATE MATERIALIZED VIEW cmip5_attributes TABLESPACE ceph AS
    SELECT
        md_hash
      , md_json->'attributes'->>'experiment_id'         as experiment_id
      , md_json->'attributes'->>'frequency'             as frequency
      , md_json->'attributes'->>'institute_id'          as institute_id
      , md_json->'attributes'->>'model_id'              as model_id
      , md_json->'attributes'->>'modeling_realm'        as modeling_realm
      , md_json->'attributes'->>'product'               as product
      , md_json->'attributes'->>'table_id'              as table_id
      , md_json->'attributes'->>'tracking_id'           as tracking_id
      , md_json->'attributes'->>'version_number'        as version_number
      , md_json->'attributes'->>'realization'           as realization
      , md_json->'attributes'->>'initialization_method' as initialization_method
      , md_json->'attributes'->>'physics_version'       as physics_version
    FROM metadata 
    WHERE
        md_json->'attributes'->>'Conventions' is not null
      AND
        md_json->'attributes'->>'project_id' = 'CMIP5';
CREATE UNIQUE INDEX ON cmip5_attributes (md_hash);
CREATE INDEX ON cmip5_attributes (
    experiment_id
  , institute_id
  , model_id
  , modeling_realm
  , frequency
    );

/* Derived CMIP5 attributes that are not specified in the file
 * Includes links to normalised tables and 'rXiYpZ' ensemble member
 */
CREATE MATERIALIZED VIEW cmip5_attributes_derived TABLESPACE ceph AS
    SELECT
        md_hash
      , ensemble_member
      , dataset_id
      , md5 ( dataset_id || ':' || COALESCE(version_number,'-') ) as version_id
    FROM (SELECT
        md_hash
      , version_number
      , ('r' || realization ||
        'i' || initialization_method ||
        'p' || physics_version) as ensemble_member
      , md5 ( experiment_id 
        || ':' || institute_id 
        || ':' || model_id 
        || ':' || modeling_realm 
        || ':' || frequency) as dataset_id
    FROM cmip5_attributes) AS s;
CREATE UNIQUE INDEX ON cmip5_attributes_derived (md_hash);
CREATE INDEX ON cmip5_attributes_derived (dataset_id);
CREATE INDEX ON cmip5_attributes_derived (version_id);

/* A CMIP5 dataset
 * Like what you find on ESGF, however version_number is broken out into its
 * own table
 */
CREATE MATERIALIZED VIEW cmip5_dataset TABLESPACE ceph AS
    SELECT DISTINCT
        dataset_id
      , experiment_id
      , institute_id
      , model_id
      , modeling_realm
      , frequency
    FROM cmip5_attributes NATURAL INNER JOIN cmip5_attributes_derived;
CREATE UNIQUE INDEX ON cmip5_dataset (dataset_id);

/* The version number specified in the file
 * This may not be accurate, so can be overridden by adding an entry to
 * `cmip5_override_version`
 * Normally the `cmip5_version` table should be used instead, as this contains
 * fixed values
 */ 
CREATE MATERIALIZED VIEW cmip5_file_version TABLESPACE ceph AS
    SELECT DISTINCT
        version_id
      , dataset_id
      , version_number
    FROM cmip5_attributes NATURAL INNER JOIN cmip5_attributes_derived;
CREATE UNIQUE INDEX ON cmip5_file_version (version_id);
CREATE INDEX ON cmip5_file_version (dataset_id);

/* Manually entered version information, for the case when the file is
 * inaccurate or missing data
 */
CREATE TABLE cmip5_override_version(
    version_id TEXT PRIMARY KEY,
    version_number TEXT,
    is_latest BOOLEAN,
    to_update BOOLEAN) TABLESPACE ceph;
CREATE UNIQUE INDEX ON cmip5_override_version (version_id);

/* View combining the file and override versions to provide a consistent view
 */
CREATE VIEW cmip5_version AS
    SELECT
        f.version_id as version_id
      , f.dataset_id as dataset_id
      , COALESCE(o.version_number, f.version_number) as version_number
      , COALESCE(o.is_latest, FALSE) as is_latest
    FROM cmip5_file_version AS f
    LEFT JOIN cmip5_override_version AS o ON (f.version_id = o.version_id);

/* Warnings associated with a dataset version
 */
CREATE TABLE cmip5_warning (
    id SERIAL PRIMARY KEY,
    version_id TEXT,
    warning TEXT,
    added_by TEXT,
    added_on DATE
    ) TABLESPACE ceph;
CREATE INDEX cmip5_warning (version_id);