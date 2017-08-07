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
GRANT SELECT ON cf_attributes TO PUBLIC;

/* CF standard variable name */
CREATE TABLE IF NOT EXISTS cf_variable (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    canonical_unit TEXT,
    grib TEXT,
    amip TEXT,
    description TEXT
    );
GRANT SELECT ON cf_variable TO PUBLIC;

/* Alias of a CF standard variable name */
CREATE TABLE IF NOT EXISTS cf_variable_alias (
    id SERIAL PRIMARY KEY,
    variable_id INTEGER REFERENCES cf_variable ON DELETE CASCADE,
    name TEXT
    );
CREATE INDEX IF NOT EXISTS cf_variable_alias_name_idx ON cf_variable_alias(name);
GRANT SELECT ON cf_variable_alias TO PUBLIC;

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
GRANT SELECT ON cf_variable_link TO PUBLIC;

