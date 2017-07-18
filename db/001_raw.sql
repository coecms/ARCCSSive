/* Raw tables extract values from the metadata JSON for indexing */

/* The collection a cf-netcdf file belongs to (optimisation for cf_attributes view) */
CREATE MATERIALIZED VIEW IF NOT EXISTS cf_attributes_raw AS
    SELECT
        md_hash
      , COALESCE(md_json->'attributes'->>'project_id', 'unknown') as collection
    FROM
        metadata 
    WHERE
        md_type = 'netcdf'
    AND
        md_json->'attributes'->>'Conventions' is not null;
CREATE UNIQUE INDEX IF NOT EXISTS cf_attributes_raw_md_hash_idx ON cf_attributes_raw(md_hash);
CREATE INDEX IF NOT EXISTS cf_attributes_raw_collection_idx ON cf_attributes_raw(collection);

/*
 * View to more easily change file path filters
 */
CREATE OR REPLACE VIEW cmip5_paths_filter AS
    SELECT pa_hash
    FROM paths
    WHERE
      (
        pa_parents[4] IN (
            md5('/g/data1/ua6/authoritative')::uuid
          )
      OR
        pa_parents[5] = md5('/g/data1/ua6/unofficial-ESG-replica/IPCC')::uuid
      OR
        pa_parents[6] = md5('/g/data1/ua6/unofficial-ESG-replica/tmp/tree')::uuid
      )
    AND pa_type = 'file';

/* Metadata from the file specific to CMIP5 
 * Gets the attributes out of JSON format into a more usable format
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_attributes_raw AS
    SELECT
        metadata.md_hash                                as md_hash
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
    JOIN cmip5_paths_filter AS paths ON metadata.md_hash = paths.pa_hash
    JOIN cf_attributes_raw ON metadata.md_hash = cf_attributes_raw.md_hash
    WHERE
        md_type = 'netcdf'
    AND
        cf_attributes_raw.collection = 'CMIP5';
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_attributes_raw_md_hash_idx ON cmip5_attributes_raw(md_hash);
