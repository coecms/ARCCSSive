/* Generic CF metadata */
CREATE OR REPLACE VIEW cf_attributes AS
    SELECT
        a.md_hash
      , md_json->'attributes'->>'title'       as title
      , md_json->'attributes'->>'institution' as institution
      , md_json->'attributes'->>'source'      as source
      , a.collection
    FROM
        cf_attributes_raw AS a
    JOIN metadata AS m ON (a.md_hash = m.md_hash);

/* CF standard variable name */
CREATE TABLE IF NOT EXISTS cf_variable (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    canonical_unit TEXT,
    grib TEXT,
    amip TEXT,
    description TEXT
    );

/* Alias of a CF standard variable name */
CREATE TABLE IF NOT EXISTS cf_variable_alias (
    id SERIAL PRIMARY KEY,
    variable_id INTEGER REFERENCES cf_variable ON DELETE CASCADE,
    name TEXT
    );
CREATE INDEX IF NOT EXISTS cf_variable_alias_name_idx ON cf_variable_alias(name);

/* Link between CF variables and CF files
 * Ignores variables with an 'axis' attribute
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS cf_variable_link AS
    WITH file_vars AS (
        -- Variables in files that aren't an axis
        SELECT
            md_hash
          , vars.key as name
          , COALESCE(vars.value->'attributes'->>'standard_name'
                , vars.key) as standard_name
        FROM
            metadata
          , jsonb_each(md_json->'variables') as vars
        WHERE
            NOT vars.value->'attributes' ? 'axis'
    )
    SELECT DISTINCT
        md_hash
      , cf_variable.id as variable_id
      , file_vars.name
    FROM
        file_vars
    JOIN
        cf_variable_alias ON (cf_variable_alias.name = file_vars.standard_name)
    JOIN
        cf_variable ON (cf_variable.id = cf_variable_alias.variable_id)
    WHERE
        cf_variable.name NOT IN ('time', 'latitude', 'longitude');
CREATE INDEX IF NOT EXISTS cf_variable_link_md_hash_idx ON cf_variable_link(md_hash);
CREATE INDEX IF NOT EXISTS cf_variable_link_variable_id_idx ON cf_variable_link(variable_id);

CREATE OR REPLACE VIEW cmip5_attributes AS
    SELECT
        *
      , split_part(table_id, ' ', 2) as mip_table
      , ('r' || realization ||
         'i' || initialization_method ||
         'p' || physics_version) as ensemble_member
    FROM cmip5_attributes_raw;

/* Links to derived tables
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_attributes_links AS
    WITH s AS (
        SELECT
            md_hash
          , version_number
          , md5 ( experiment_id 
                || ':' || institute_id 
                || ':' || model_id 
                || ':' || modeling_realm 
                || ':' || frequency
                || ':' || mip_table
                || ':' || ensemble_member
                )::uuid as dataset_id
        FROM cmip5_attributes
    )
    , v AS (
        SELECT DISTINCT
            md_hash
          , array_agg(name) as variable_list
        FROM
            cf_variable_link
        GROUP BY md_hash
    )
    SELECT
        s.md_hash
      , dataset_id
      , md5 ( dataset_id || ':' || COALESCE(version_number,'-') )::uuid as version_id
      , variable_list
      , md5 ( array_to_string( variable_list, '.' ) )::uuid as variable_id
    FROM 
        s
    JOIN 
        v ON (s.md_hash = v.md_hash);
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_attributes_links_md_hash_idx ON cmip5_attributes_links(md_hash);
CREATE INDEX IF NOT EXISTS cmip5_attributes_links_dataset_id_idx ON cmip5_attributes_links(dataset_id);
CREATE INDEX IF NOT EXISTS cmip5_attributes_links_version_id_idx ON cmip5_attributes_links(version_id);
CREATE INDEX IF NOT EXISTS cmip5_attributes_links_variable_id_idx ON cmip5_attributes_links(variable_id);

/* A CMIP5 dataset
 * Like what you find on ESGF, however version_number is broken out into its
 * own table
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_dataset AS
    WITH datas AS (
        SELECT DISTINCT
            experiment_id
          , institute_id
          , model_id
          , modeling_realm
          , frequency
          , mip_table
          , ensemble_member
        FROM cmip5_attributes
    )
    SELECT
        md5 ( experiment_id 
            || ':' || institute_id 
            || ':' || model_id 
            || ':' || modeling_realm 
            || ':' || frequency
            || ':' || mip_table
            || ':' || ensemble_member
            )::uuid as dataset_id
      , *
    FROM datas;
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_dataset_dataset_id_idx ON cmip5_dataset(dataset_id);

/* The version number specified in the file
 * This may not be accurate, so can be overridden by adding an entry to
 * `cmip5_override_version`
 * Normally the `cmip5_version` table should be used instead, as this contains
 * fixed values
 */ 
CREATE MATERIALIZED VIEW IF NOT EXISTS cmip5_file_version  AS
    SELECT DISTINCT
        version_id
      , dataset_id
      , version_number
    FROM cmip5_attributes NATURAL INNER JOIN cmip5_attributes_links;
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_file_version_version_id_idx ON cmip5_file_version(version_id);
CREATE INDEX IF NOT EXISTS cmip5_file_version_dataset_id_idx ON cmip5_file_version(dataset_id);

/* Manually entered version information, for the case when the file is
 * inaccurate or missing data
 */
CREATE TABLE IF NOT EXISTS cmip5_override_version(
    version_id UUID PRIMARY KEY,
    version_number TEXT,
    is_latest BOOLEAN,
    to_update BOOLEAN) ;
CREATE UNIQUE INDEX IF NOT EXISTS cmip5_override_version_version_id_idx ON cmip5_override_version(version_id);

/* View combining the file and override versions to provide a consistent view
 */
CREATE OR REPLACE VIEW cmip5_version AS
    SELECT
        f.version_id as version_id
      , f.dataset_id as dataset_id
      , COALESCE(o.version_number, f.version_number) as version_number
      , COALESCE(o.is_latest, FALSE) as is_latest
    FROM cmip5_file_version AS f
    LEFT JOIN cmip5_override_version AS o ON (f.version_id = o.version_id);

/* Warnings associated with a dataset version
 */
CREATE TABLE IF NOT EXISTS cmip5_warning (
    id SERIAL PRIMARY KEY,
    version_id UUID,
    warning TEXT,
    added_by TEXT,
    added_on DATE
    ) ;
CREATE INDEX IF NOT EXISTS cmip5_warning_version_id_idx ON cmip5_warning(version_id);

CREATE OR REPLACE VIEW cmip5_timeseries_link AS
    SELECT DISTINCT
        dataset_id
      , version_id
      , variable_id
      , variable_list
    FROM
        cmip5_attributes_links;

CREATE OR REPLACE VIEW old_cmip5_instance AS
    WITH x AS (
        SELECT DISTINCT
            dataset_id
          , variable
          , variable_id
        FROM
            cmip5_attributes_links
          , UNNEST(variable_list) variable
    )
    SELECT
        x.dataset_id
      , x.variable
      , x.variable_id
      , d.experiment_id as experiment
      , d.mip_table as mip
      , d.model_id as model
      , d.ensemble_member as ensemble
      , d.modeling_realm as realm
    FROM
        cmip5_dataset AS d
    INNER JOIN x ON (x.dataset_id = d.dataset_id);

CREATE OR REPLACE VIEW old_cmip5_version AS
    WITH x AS (
        SELECT DISTINCT
            dataset_id
          , version_id
          , variable
          , variable_id
        FROM
            cmip5_attributes_links
          , UNNEST(variable_list) variable
    )
    SELECT
        x.dataset_id
      , x.version_id
      , x.variable
      , x.variable_id
      , v.version_number as version
      , v.is_latest
    FROM
        cmip5_version AS v
    INNER JOIN x ON (v.version_id = x.version_id);

/* The most recent version attached to a dataset */
CREATE OR REPLACE VIEW cmip5_latest_version AS
    SELECT DISTINCT ON (dataset_id, variable_id)
        dataset_id
      , version_id
      , variable_id
    FROM cmip5_attributes_links
    NATURAL JOIN cmip5_version
    ORDER BY
        dataset_id
      , variable_id
      , is_latest DESC
      , version_id DESC;
