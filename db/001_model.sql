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
      , 'r' || md_json->'attributes'->>'realization' ||
        'i' || md_json->'attributes'->>'initialization_method' ||
        'p' || md_json->'attributes'->>'physics_version' as ensemble_member
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

CREATE MATERIALIZED VIEW cmip5_dataset TABLESPACE ceph AS
    SELECT
        ROW_NUMBER() OVER() AS id
      , s.experiment_id
      , s.institute_id
      , s.model_id
      , s.modeling_realm
      , s.frequency
    FROM (SELECT DISTINCT
        experiment_id
      , institute_id
      , model_id
      , modeling_realm
      , frequency
    FROM cmip5_attributes) as s;
CREATE UNIQUE INDEX ON cmip5_dataset (id);

CREATE MATERIALIZED VIEW cmip5_version TABLESPACE ceph AS
    SELECT
        ROW_NUMBER() OVER() AS id
      , dataset_id
      , version_number
    FROM cmip5_version_distinct;

CREATE UNIQUE INDEX ON cmip5_version (id);

CREATE TABLE cmip5_version_extra TABLESPACE ceph (
    id SERIAL PRIMARY KEY
  , experiment_id TEXT
  , institute_id TEXT
  , model_id TEXT
  , modeling_realm TEXT
  , frequency TEXT
  , version_number TEXT
  , is_latest BOOLEAN
    );

CREATE UNIQUE INDEX ON cmip5_version_extra (
    experiment_id
  , institute_id
  , model_id
  , modeling_realm
  , frequency
  , version_number)


CREATE TABLE warnings (
    id SERIAL PRIMARY KEY,
    warning TEXT,
    added_by TEXT,
    added_on DATE
    );
